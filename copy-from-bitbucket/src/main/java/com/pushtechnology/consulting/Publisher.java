package com.pushtechnology.consulting;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.math.NumberUtils;

import com.pushtechnology.consulting.Benchmarker.CreatorState;
import com.pushtechnology.diffusion.client.Diffusion;
import com.pushtechnology.diffusion.client.callbacks.ErrorReason;
import com.pushtechnology.diffusion.client.callbacks.Registration;
import com.pushtechnology.diffusion.client.content.Content;
import com.pushtechnology.diffusion.client.features.RegisteredHandler;
import com.pushtechnology.diffusion.client.features.Topics;
import com.pushtechnology.diffusion.client.features.Topics.CompletionCallback;
import com.pushtechnology.diffusion.client.features.Topics.UnsubscribeReason;
import com.pushtechnology.diffusion.client.features.control.topics.TopicAddFailReason;
import com.pushtechnology.diffusion.client.features.control.topics.TopicControl;
import com.pushtechnology.diffusion.client.features.control.topics.TopicControl.AddCallback;
import com.pushtechnology.diffusion.client.features.control.topics.TopicControl.MissingTopicHandler;
import com.pushtechnology.diffusion.client.features.control.topics.TopicControl.MissingTopicNotification;
import com.pushtechnology.diffusion.client.features.control.topics.TopicUpdateControl;
import com.pushtechnology.diffusion.client.features.control.topics.TopicUpdateControl.UpdateSource;
import com.pushtechnology.diffusion.client.features.control.topics.TopicUpdateControl.Updater;
import com.pushtechnology.diffusion.client.features.control.topics.TopicUpdateControl.Updater.UpdateCallback;
import com.pushtechnology.diffusion.client.session.Session;
import com.pushtechnology.diffusion.client.session.Session.ErrorHandler;
import com.pushtechnology.diffusion.client.session.Session.Listener;
import com.pushtechnology.diffusion.client.session.Session.SessionError;
import com.pushtechnology.diffusion.client.session.Session.State;
import com.pushtechnology.diffusion.client.session.SessionFactory;
import com.pushtechnology.diffusion.client.topics.details.TopicDetails;
import com.pushtechnology.diffusion.client.topics.details.TopicType;
import com.pushtechnology.diffusion.client.types.UpdateContext;

public class Publisher {

	SessionFactory sessionFactory;
	Session session;
	String connectionString;

	TopicControl topicControl;
	TopicUpdateControl topicUpdateControl;

	Updater updater;
	public Map<String, ScheduledFuture<?>> topicUpdatersByTopic = new HashMap<>();

	public boolean onStandby = true;
	List<String> topicStandbyList = new ArrayList<>();

	private CreatorState state;

	public Publisher(String connectionString, String username, String password, final List<String> topics) {
		Out.t("Publisher constructor");

		this.connectionString = connectionString;
		this.sessionFactory = Diffusion.sessions().connectionTimeout(10000);
		if (!username.isEmpty()) {
			this.sessionFactory = this.sessionFactory.principal(username);
			if (!password.isEmpty()) {
				this.sessionFactory = this.sessionFactory.password(password);
			}
		}
		this.sessionFactory = this.sessionFactory.errorHandler(new ErrorHandler() {

			@Override
			public void onError(Session session, SessionError err) {
				Out.e("SessionCreator#sessionFactory.onError : '%s'", err.getMessage());
			}
		}).listener(new Listener() {

			@Override
			public void onSessionStateChanged(Session session, State oldState, State newState) {
				Out.t("Publisher#sessionFactory.onSessionStateChanged");
				Out.d("Session state changed from '%s' to '%s'", oldState, newState);
				switch (newState) {
				case CONNECTED_ACTIVE:
					Out.d("Session state is active registeringTopicControlAndSource...");
					registerTopicControlAndSource();
					Out.d("Subscibing to topics...");
					subscribeToTopics(topics);
					break;
				default:
					break;
				}
				Out.t("Done Publisher#sessionFactory.onSessionStateChanged");
			}
		});

		state = CreatorState.INITIALISED;

		Out.t("Done Publisher constructor");
	}

	void stopAllFeeds() {
		Out.t("Publisher#stopAllFeeds");
		Out.d("Stopping all feeds...");
		for (ScheduledFuture<?> future : topicUpdatersByTopic.values()) {
			future.cancel(false);
		}
		Out.t("Done Publisher#stopAllFeeds");
	}

	void registerTopicControlAndSource() {
		Out.t("Publisher#registerTopicControlAndSource");

		Out.d("Setting up topic control...");
		this.topicControl = this.session.feature(TopicControl.class);
		this.topicControl.addMissingTopicHandler(Benchmarker.ROOT_TOPIC, new MissingTopicHandler() {

			@Override
			public void onClose(String topicPath) {
				Out.t("MissingTopicHandler#OnClose for topic '%s'", topicPath);
			}

			@Override
			public void onActive(String topicPath, RegisteredHandler registeredHandler) {
				Out.i("MissingTopicHandler active for topic '%s'", topicPath);
			}

			@Override
			public void onMissingTopic(MissingTopicNotification notification) {
				Out.t("Publisher#missingTopicHandler.OnMissingTopic");
				String topicPath = notification.getTopicPath();
				Out.i("OnMissingTopic called for '%s'", topicPath);
				createTopic(topicPath, TopicType.SINGLE_VALUE);
				notification.proceed();
				Out.t("Done Publisher#missingTopicHandler.OnMissingTopic");

			}
		});

		Out.d("Setting up topic update control (source)...");
		this.topicUpdateControl = this.session.feature(TopicUpdateControl.class);
		this.topicUpdateControl.registerUpdateSource(Benchmarker.ROOT_TOPIC, new UpdateSource() {

			@Override
			public void onClose(String topicPath) {
				Out.t("Publisher#TopicSource.onClosed");
				Publisher.this.updater = null;
				shutdown();
				Out.t("Done Publisher#TopicSource.onClosed");

			}

			@Override
			public void onError(String topicPath, ErrorReason reason) {
				Out.e("Publisher#TopicSource.onError for topic '%s' cause: '%s'", topicPath, reason);
			}

			@Override
			public void onRegistered(String topicPath, Registration registration) {

			}

			@Override
			public void onActive(String topicPath, Updater updater) {
				Out.t("Publisher#TopicSource.onActive");
				onStandby = false;
				Publisher.this.updater = updater;
				Out.d("Creating root topic '%s'", Benchmarker.ROOT_TOPIC);
				createTopic(Benchmarker.ROOT_TOPIC, TopicType.STATELESS);
				for (String topic : topicStandbyList) {
					startFeed(topic);
				}
				Out.t("Done Publisher#TopicSource.onActive");
			}

			@Override
			public void onStandby(String topicPath) {
				Out.t("Publisher#TopicSource.onStandby");
				onStandby = true;
				stopAllFeeds();
				Out.t("Done Publisher#TopicSource.onStandby");

			}
		});

		Out.t("Done Publisher#registerTopicControlAndSource");
	}

	void subscribeToTopics(List<String> topics) {
		Out.d("Subscibing to topics: '%s'", ArrayUtils.toString(topics));
		for (String topic : topics) {
			String sel = ">" + topic;
			Out.d("Subscribing to topic '%s'", sel);
			Topics topicFeature = session.feature(Topics.class);
			topicFeature.addTopicStream(sel, new Topics.TopicStream() {

				@Override
				public void onClose() {
				}

				@Override
				public void onError(ErrorReason reason) {
					Out.e("Publisher#TopicStream.onError : '%s'", reason);
				}

				@Override
				public void onSubscription(String topicPath, TopicDetails details) {
					startFeed(topicPath);
				}

				@Override
				public void onTopicUpdate(String topicPath, Content content, UpdateContext context) {
				}

				@Override
				public void onUnsubscription(String topicPath, UnsubscribeReason reason) {
				}
			});

			topicFeature.subscribe(sel, new CompletionCallback() {

				@Override
				public void onDiscard() {
					Out.t("Publisher#topics.onDiscard");
				}

				@Override
				public void onComplete() {
					Out.t("Publisher#topics.onComplete");
				}
			});
		}
	}

	void createTopic(String topicPath, TopicType topicType) {
		Out.t("Publisher#createTopic");
		Out.d("Creating topic '%s' of type '%s'", topicPath, topicType);
		this.topicControl.addTopic(topicPath, topicType, new AddCallback() {

			@Override
			public void onDiscard() {
				Out.e("Publisher#TopicControlAddCallbackImpl.onDiscard() :: Notification that a call context was closed prematurely, typically due to a timeout or the session being closed.");
			}

			@Override
			public void onTopicAdded(String topicPath) {
				Out.i("Topic '%s' added.", topicPath);
				startFeed(topicPath);
			}

			@Override
			public void onTopicAddFailed(String topicPath, TopicAddFailReason reason) {
				Out.t("Publisher#TopicControlAddCallback.onTopicAddFailed");
				Out.d("topicAddFailed path: '%s', reason: '%s'", topicPath, reason);
				switch (reason) {
				case EXISTS:
					startFeed(topicPath);
					break;
				default:
					Out.e("Adding topic: '%s' failed for reason: '%s'", topicPath, reason);
					break;
				}
				Out.t("Done Publisher#TopicControlAddCallback.onTopicAddFailed");
			}
		});
		Out.t("Done Publisher#createTopic");
	}

	void startFeed(final String topicPath) {
		Out.t("Publisher#startFeed for '%s'", topicPath);
		if (onStandby) {
			Out.d("Publisher#startFeed : OnStandby, adding to list and waiting...");
			topicStandbyList.add(topicPath);
			return;
		}
		Out.d("Trying to start feed for: '%s'", topicPath);
		String[] paths = topicPath.split("/");
		if (paths.length == 3) {
			Out.d("Found topicPath '%s' elements: '%s', '%s', '%s'", topicPath, paths[0], paths[1], paths[2]);
			final int messageSizeInBytes = NumberUtils.toInt(paths[1], -1);
			final int messagesPerSecond = NumberUtils.toInt(paths[2], -1);
			if (messageSizeInBytes > 0 && messagesPerSecond > 0) {
				Out.d("Using messageSizeInBytes: '%d' and messagesPerSecond: '%d'", messageSizeInBytes, messagesPerSecond);
				ScheduledFuture<?> tmpFuture;
				if (topicUpdatersByTopic.containsKey(topicPath)) {
					Out.t("Service already found, trying to pull by topicPath '%s'", topicPath);
					tmpFuture = topicUpdatersByTopic.get(topicPath);
					if (!tmpFuture.isDone() && !tmpFuture.isCancelled()) {
						Out.d("Service is already running for topic '%s'", topicPath);
						return;
					}
				} else {
					Out.d("Service not found, creating for '%s'", topicPath);
				}
				Long scheduleIntervalInMillis = new Long(1000 / messagesPerSecond);
				Out.i("Updating topic path '%s', every '%d' ms", topicPath, scheduleIntervalInMillis);
				tmpFuture = Benchmarker.globalThreadPool.scheduleAtFixedRate(new Runnable() {

					@Override
					public void run() {
						Out.d("Update for topic path: '%s'", topicPath);
						Publisher.this.updater.update(topicPath, Diffusion.content().newContent(createSizedByteArray(messageSizeInBytes)),
								new UpdateCallback() {

									@Override
									public void onError(ErrorReason error) {
										Out.e("Error : '%s'", error);
									}

									@Override
									public void onSuccess() {
										Out.d("Topic updated");
									}
								});
					}
				}, 0L, scheduleIntervalInMillis, TimeUnit.MILLISECONDS);
				Out.d("Started updater for '%s'", topicPath);
				topicUpdatersByTopic.put(topicPath, tmpFuture);
			} else {
				Out.e("Could not parse topicPath: '%s', found messageSizeInBytes: '%d' and messagesPerSecond: '%d'", topicPath, messageSizeInBytes,
						messagesPerSecond);
			}
		} else {
			Out.e("Could not parse topicPath '%s', length was : %d, expected 3!", topicPath, paths.length);
		}
		Out.t("Done Publisher#startFeed for '%s'", topicPath);
	}

	byte[] createSizedByteArray(int messageSizeInBytes) {
		byte[] bytes = new byte[messageSizeInBytes];
		ThreadLocalRandom.current().nextBytes(bytes);
		return bytes;
	}

	void stopFeed(String topicPath) {
		Out.t("Publisher#stopFeed for '%s'", topicPath);
		Out.d("Trying to stop feed for: '%s'", topicPath);
		if (topicUpdatersByTopic.containsKey(topicPath)) {
			Out.i("Stopping topic feed for '%s'", topicPath);
			topicUpdatersByTopic.get(topicPath).cancel(false);
		}
		Out.t("Done Publisher#stopFeed for '%s'", topicPath);
	}

	public void start() {
		Out.t("Publisher#start");
		switch (state) {
		case INITIALISED:
			doStart();
			state = CreatorState.STARTED;
			break;
		default:
			break;
		}
		Out.t("Done Publisher#start");
	}

	private void doStart() {
		Out.t("Publisher#doStart");
		this.session = this.sessionFactory.open(this.connectionString);
		Out.t("Done Publisher#doStart");
	}

	public void shutdown() {
		Out.t("Publisher#shutdown");
		switch (state) {
		case STARTED:
			stopAllFeeds();
			state = CreatorState.SHUTDOWN;
			break;
		default:
			break;
		}
		Out.t("Done Publisher#shutdown");
	}

}