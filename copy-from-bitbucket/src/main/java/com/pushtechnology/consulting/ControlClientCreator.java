package com.pushtechnology.consulting;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import com.pushtechnology.consulting.Benchmarker.CreatorState;
import com.pushtechnology.diffusion.client.Diffusion;
import com.pushtechnology.diffusion.client.callbacks.ErrorReason;
import com.pushtechnology.diffusion.client.callbacks.Registration;
import com.pushtechnology.diffusion.client.content.Content;
import com.pushtechnology.diffusion.client.features.Topics;
import com.pushtechnology.diffusion.client.features.Topics.CompletionCallback;
import com.pushtechnology.diffusion.client.features.Topics.TopicStream;
import com.pushtechnology.diffusion.client.features.Topics.UnsubscribeReason;
import com.pushtechnology.diffusion.client.features.control.topics.TopicAddFailReason;
import com.pushtechnology.diffusion.client.features.control.topics.TopicControl;
import com.pushtechnology.diffusion.client.features.control.topics.TopicControl.AddCallback;
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
import com.pushtechnology.diffusion.client.session.SessionId;
import com.pushtechnology.diffusion.client.topics.details.TopicDetails;
import com.pushtechnology.diffusion.client.topics.details.TopicType;
import com.pushtechnology.diffusion.client.types.UpdateContext;

public class ControlClientCreator {

	private static final String CC_BASE_TOPIC = Benchmarker.ROOT_TOPIC + "/ControlClients";
	private SessionFactory sessionFactory;
	private String connectionString;
	List<String> updateTopics = new ArrayList<>();

	private ScheduledFuture<?> addControlClients;
	private boolean doAddControlClientsTest;
	public Map<String, ScheduledFuture<?>> topicUpdatersByTopicPath = new HashMap<>();

	private final Object controlClientListLock = new Object();

	public volatile List<Session> controlClients = new ArrayList<>();
	public volatile List<Session> connectingControlClients = new ArrayList<>();
	public volatile List<Session> closedByServerControlClients = new ArrayList<>();
	public volatile List<Session> closedControlClients = new ArrayList<>();
	public volatile int connectionFailures = 0;

	public CreatorState state;

	private UpdateCallback updateCallback = new UpdateCallback() {

		@Override
		public void onError(ErrorReason error) {
			Out.e("Error : '%s'", error);
		}

		@Override
		public void onSuccess() {
			Out.d("Topic updated");
		}
	};

	abstract class CCUpdateSource implements UpdateSource {

		@Override
		public void onRegistered(String topicPath, Registration registration) {
			Out.d("Registered UpdateSource on topicPath '%s'", topicPath);
		}

		@Override
		public void onClose(String topicPath) {
			Out.d("Closed UpdateSource on topicPath '%s'", topicPath);
		}

		@Override
		public void onError(String topicPath, ErrorReason errorReason) {
			if (ErrorReason.SESSION_CLOSED.equals(errorReason)) {
				// ignore => saves a bunch of output for now.
			} else {
				Out.e("Error on UpdateSource for topicPath '%s' :: '%s'", topicPath, errorReason);
			}
		}

		@Override
		public void onStandby(String topicPath) {
			Out.d("UpdateSource on standby for topicPath '%s'", topicPath);
		}
	}

	private AddCallback addCallback = new AddCallback() {

		@Override
		public void onDiscard() {
		}

		@Override
		public void onTopicAdded(String topicPath) {
			Out.d("Added topic: '%s'", topicPath);
		}

		@Override
		public void onTopicAddFailed(String topicPath, TopicAddFailReason reason) {
			Out.e("Failed to add topic: '%s' because '%s'", topicPath, reason);
		}
	};

	public ControlClientCreator(final String connectionString, String username, String password, List<String> topics) {
		Out.t("ControlClientCreator constructor...");

		this.connectionString = connectionString;
		Out.i("Creating controlClients publishing to topics: '%s'", StringUtils.join(topics, ", "));
		this.updateTopics.addAll(topics);

		this.sessionFactory = Diffusion.sessions().connectionTimeout(60000);
		if (!username.isEmpty()) {
			this.sessionFactory = this.sessionFactory.principal(username);
			if (!password.isEmpty()) {
				this.sessionFactory = this.sessionFactory.password(password);
			}
		}
		this.sessionFactory = this.sessionFactory.errorHandler(new ErrorHandler() {

			@Override
			public void onError(Session session, SessionError err) {
				Out.e("ControlClientCreator#sessionFactory.onError : '%s'", err.getMessage());
			}
		}).listener(new Listener() {

			@Override
			public void onSessionStateChanged(final Session session, State oldState, State newState) {
				Out.t("ControlClientCreator#sessionFactory.onSessionStateChanged");
				Out.t("ControlClient state changed from '%s' to '%s'", oldState, newState);
				switch (newState) {
				case CONNECTED_ACTIVE:
					final String baseControlClientTopic = getFullTopicPathForSessionId(session.getSessionId());
					final TopicControl topicControlFeature = session.feature(TopicControl.class);
					topicControlFeature.addTopic(baseControlClientTopic, TopicType.STATELESS, addCallback);

					session.feature(TopicUpdateControl.class).registerUpdateSource(baseControlClientTopic, new CCUpdateSource() {

						@Override
						public void onActive(String topicPath, final Updater updater) {
							Out.d("UpdateSource active for topicPath '%s'", topicPath);
							for (String top : updateTopics) {
								topicControlFeature.addTopic(baseControlClientTopic + "/" + top, TopicType.SINGLE_VALUE, new AddCallback() {

									@Override
									public void onDiscard() {
									}

									@Override
									public void onTopicAdded(final String topicPath) {
										Out.d("Added topic '%s', starting feed...", topicPath);
										Topics topics = session.feature(Topics.class);
										topics.addTopicStream(">" + topicPath, new TopicStream() {

											@Override
											public void onClose() {
											}

											@Override
											public void onUnsubscription(String topic, UnsubscribeReason reason) {
											}

											@Override
											public void onSubscription(String topic, TopicDetails details) {
												Out.d("Subscribed to topic '%s'", topic);
											}

											@Override
											public void onError(ErrorReason reason) {
												if (!ErrorReason.SESSION_CLOSED.equals(reason)) {
													Out.e("TopicStream::OnError '%s'", reason);
												}
											}

											@Override
											public void onTopicUpdate(String topicPath, Content content, UpdateContext context) {
												Out.d("Update for topic '%s'", topicPath);
											}
										});
										topics.subscribe(">" + topicPath, new CompletionCallback() {

											@Override
											public void onDiscard() {
												Out.t("ControlClient#topics.onDiscard");
											}

											@Override
											public void onComplete() {
												Out.t("ControlClient#topics.onComplete to topicSelector '%s'", topicPath);
											}
										});
										startFeed(topicPath, updater);
									}

									@Override
									public void onTopicAddFailed(String topicPath, TopicAddFailReason reason) {
										switch (reason) {
										case EXISTS:
										case EXISTS_MISMATCH:
											startFeed(topicPath, updater);
											break;
										default:
											Out.e("Failed to add topic: '%s' because '%s'", topicPath, reason);
											break;
										}
									}
								});
							}
						}

					});

					break;
				case CLOSED_BY_SERVER:
					synchronized (controlClientListLock) {
						closedByServerControlClients.add(session);
						controlClients.remove(session);
					}
					break;
				default:
					break;
				}
				Out.t("Done ControlClientCreator#sessionFactory.onSessionStateChanged");
			}
		});

		state = CreatorState.INITIALISED;

		Out.t("Done ControlClientCreator constructor...");
	}

	void startFeed(final String topicPath, final Updater updater) {
		Out.t("ControlClientCreator#startFeed for '%s'", topicPath);
		Out.d("Trying to start ControlClient feed for: '%s'", topicPath);
		String[] paths = topicPath.split("/");
		if (paths.length > 4) {
			Out.d("Found topicPath '%s' elements: '%s', '%s', '%s', '%s', '%s'", topicPath, paths[0], paths[1], "<sessionId>", paths[paths.length - 2],
					paths[paths.length - 1]);
			final int messageSizeInBytes = NumberUtils.toInt(paths[paths.length - 2], -1);
			final int messagesPerSecond = NumberUtils.toInt(paths[paths.length - 1], -1);
			if (messageSizeInBytes > 0 && messagesPerSecond > 0) {
				Out.d("Using messageSizeInBytes: '%d' and messagesPerSecond: '%d'", messageSizeInBytes, messagesPerSecond);
				Long scheduleIntervalInMillis = new Long(1000 / messagesPerSecond);
				Out.d("Updating topic path '%s', every '%d' ms", topicPath, scheduleIntervalInMillis);
				ScheduledFuture<?> tmpFuture = Benchmarker.globalThreadPool.scheduleAtFixedRate(new Runnable() {

					@Override
					public void run() {
						Out.d("Update for topic path: '%s'", topicPath);
						updater.update(topicPath, Diffusion.content().newContent(createSizedByteArray(messageSizeInBytes)), updateCallback);
					}
				}, 0L, scheduleIntervalInMillis, TimeUnit.MILLISECONDS);

				topicUpdatersByTopicPath.put(topicPath, tmpFuture);
				Out.d("Started controlClient updater for '%s'", topicPath);
			} else {
				Out.e("Could not parse topicPath: '%s', found messageSizeInBytes: '%d' and messagesPerSecond: '%d'", topicPath, messageSizeInBytes,
						messagesPerSecond);
			}
		} else {
			Out.e("Could not parse topicPath '%s', length was : %d, expected more than 4!", topicPath, paths.length);
		}
		Out.t("Done ControlClientCreator#startFeed for '%s'", topicPath);
	}

	String getFullTopicPathForSessionId(SessionId sessionId) {
		StringBuilder builder = new StringBuilder();

		String sessionIdString = StringUtils.trimToEmpty((sessionId == null) ? StringUtils.EMPTY : sessionId.toString());
		String tmpStringToSplit = sessionIdString;

		String[] sessIdSplit = sessionIdString.split("-");
		if (sessIdSplit.length == 2) {
			builder.append("/");
			builder.append(sessIdSplit[0]);
			tmpStringToSplit = sessIdSplit[1];
		}

		for (char c : tmpStringToSplit.toCharArray()) {
			builder.append("/");
			builder.append(c);
		}

		return CC_BASE_TOPIC + builder.toString();
	}

	byte[] createSizedByteArray(int messageSizeInBytes) {
		byte[] bytes = new byte[messageSizeInBytes];
		ThreadLocalRandom.current().nextBytes(bytes);
		return bytes;
	}

	public void start() {
		Out.t("ControlClientCreator#start");
		switch (state) {
		case INITIALISED:
			doAddControlClientsTest = true;
			doStart();
			// doStartTest();
			state = CreatorState.STARTED;
			break;
		default:
			break;
		}
		Out.t("Done ControlClientCreator#start");
	}

	@SuppressWarnings("unused")
	private void doStartTest() {

		final BlockingQueue<Runnable> connectQ = new ArrayBlockingQueue<Runnable>(500);
		final ThreadPoolExecutor connectThread = new ThreadPoolExecutor(20, 20, 30, TimeUnit.SECONDS, connectQ);
		Benchmarker.globalThreadPool.execute(new Runnable() {

			@Override
			public void run() {
				while (doAddControlClientsTest) {
					if (connectQ.remainingCapacity() == 0) {
						LockSupport.parkNanos(10000000L);
					} else {
						connectThread.execute(new Runnable() {

							@Override
							public void run() {
								Out.t("Adding session");
								Session session = sessionFactory.open(connectionString);
								synchronized (controlClientListLock) {
									// connectingSessions.add(session);
									controlClients.add(session);
								}
								Out.t("Done Adding ControlClient");
							}
						});
					}
				}

			}
		});
	}

	private void doStart() {
		Out.t("ControlClientCreator#doStart");

		addControlClients = Benchmarker.globalThreadPool.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				Out.t("Adding ControlClient");
				try {
					Session session = sessionFactory.open(connectionString);

					// session.start();
					synchronized (controlClientListLock) {
						connectingControlClients.add(session);
						// sessions.add(session);
					}
					Out.t("Done Adding ControlClient");
				} catch (Throwable t) {
					synchronized (controlClientListLock) {
						connectionFailures++;
					}
					Out.e("Exception caught trying to connect: '%s'", t.getMessage());
					t.printStackTrace();
				}
				// Out.t("Adding 500 ControlClients");
				// List<Session> tmpControlClients = new ArrayList<>();
				// for (int i = 0; i < 500; i++) {
				// Session session =
				// sessionFactory.open(connectionString);
				// session.start();
				// tmpControlClients.add(session);
				// }
				// synchronized (controlClientListLock) {
				// // connectingControlClients.add(session);
				// sessions.addAll(tmpControlClients);
				// }
				// Out.t("Done Adding 500 ControlClients");
			}
		}, 0L, 1L, TimeUnit.MICROSECONDS);

		// checkControlClient =
		// Benchmarker.globalThreadPool.scheduleAtFixedRate(new
		Benchmarker.globalThreadPool.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				Out.t("Checking which ControlClients have connected...");
				synchronized (controlClientListLock) {
					for (Iterator<Session> iter = controlClients.iterator(); iter.hasNext();) {
						Session sess = iter.next();
						if (!Session.State.CONNECTED_ACTIVE.equals(sess.getState())) {
							Out.t("Moving closed ControlClient from ControlClients to closedControlClients");
							closedControlClients.add(sess);
							iter.remove();
						}
					}
					for (Iterator<Session> iter = connectingControlClients.iterator(); iter.hasNext();) {
						Session sess = iter.next();
						if (sess.getState().isConnected()) {
							Out.t("Moving connected ControlClient from connectingControlClients to ControlClients");
							controlClients.add(sess);
							iter.remove();
						}
					}
				}
				Out.t("Done Checking which ControlClient have connected...");
			}
		}, 1500L, 500L, TimeUnit.MILLISECONDS);

		Out.t("Done ControlClientCreator#doStart");
	}

	public void stop() {
		Out.t("ControlClientCreator#stop");
		switch (state) {
		case STARTED:
			if (addControlClients != null) {
				addControlClients.cancel(false);
			}

			doAddControlClientsTest = false;
			state = CreatorState.STOPPED;
			break;
		default:
			break;
		}
		Out.t("Done ControlClientCreator#stop");
	}

	public void shutdown() {
		Out.t("ControlClientCreator#shutdown");
		switch (state) {
		case STARTED:
			stop();
		case STOPPED:
			// this.checkControlClient.cancel(false);
			Out.i("ControlClient Creator stopping...");
			synchronized (controlClientListLock) {
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				for (ScheduledFuture<?> tmpFuture : topicUpdatersByTopicPath.values()) {
					tmpFuture.cancel(true);
				}
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				for (Session s : this.controlClients) {
					s.close();
				}
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			state = CreatorState.SHUTDOWN;
			break;
		default:
			break;
		}
		Out.t("Done ControlClientCreator#shutdown");
	}
}