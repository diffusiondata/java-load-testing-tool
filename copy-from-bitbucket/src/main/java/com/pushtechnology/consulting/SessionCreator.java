package com.pushtechnology.consulting;

import static com.pushtechnology.diffusion.client.session.SessionAttributes.DEFAULT_MAXIMUM_MESSAGE_SIZE;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;

import com.pushtechnology.consulting.Benchmarker.CreatorState;
import com.pushtechnology.diffusion.client.Diffusion;
import com.pushtechnology.diffusion.client.callbacks.ErrorReason;
import com.pushtechnology.diffusion.client.content.Content;
import com.pushtechnology.diffusion.client.features.Topics;
import com.pushtechnology.diffusion.client.features.Topics.CompletionCallback;
import com.pushtechnology.diffusion.client.features.Topics.UnsubscribeReason;
import com.pushtechnology.diffusion.client.session.Session;
import com.pushtechnology.diffusion.client.session.Session.ErrorHandler;
import com.pushtechnology.diffusion.client.session.Session.Listener;
import com.pushtechnology.diffusion.client.session.Session.SessionError;
import com.pushtechnology.diffusion.client.session.Session.State;
import com.pushtechnology.diffusion.client.session.SessionFactory;
import com.pushtechnology.diffusion.client.session.reconnect.ReconnectionStrategy;
import com.pushtechnology.diffusion.client.topics.TopicSelector;
import com.pushtechnology.diffusion.client.topics.details.TopicDetails;
import com.pushtechnology.diffusion.client.types.UpdateContext;

public class SessionCreator {

	private SessionFactory sessionFactory;
	private String connectionString;
	List<TopicSelector> topicSelectors = new ArrayList<>();

	private List<ScheduledFuture<?>> addSessions = new ArrayList<>();

	private final Object sessionSetLock = new Object();

	public final Set<Session> sessions = new HashSet<>();
	public final AtomicInteger connectedSessions = new AtomicInteger(0);
	public final AtomicInteger recoveringSessions = new AtomicInteger();
	public final AtomicInteger closedSessions = new AtomicInteger(0);
	public final AtomicInteger connectionFailures = new AtomicInteger(0);
	public final AtomicInteger messageCount = new AtomicInteger(0);
	public final AtomicInteger messageByteCount = new AtomicInteger(0);

	public CreatorState state;

	public SessionCreator(String connectionString, List<String> topics) {
		Out.t("SessionCreator constructor...");

		this.connectionString = connectionString;
		Out.i("Creating sessions listening to topics: '%s'", StringUtils.join(topics, ", "));
		for (String top : topics) {
			this.topicSelectors.add(Diffusion.topicSelectors().parse(">" + top));
		}

		this.sessionFactory = Diffusion.sessions().inputBufferSize(Integer.getInteger("bench.input.buffer.size", DEFAULT_MAXIMUM_MESSAGE_SIZE))
				.outputBufferSize(Integer.getInteger("bench.output.buffer.size", DEFAULT_MAXIMUM_MESSAGE_SIZE)).connectionTimeout(60 * 1000).reconnectionTimeout(120 * 1000)
				.reconnectionStrategy(new ReconnectionStrategy() {

					@Override
					public void performReconnection(final ReconnectionAttempt reconnection) {
						Benchmarker.globalThreadPool.schedule(new Runnable() {

							@Override
							public void run() {
								reconnection.start();
							}
						}, getRandomReconnect(10), TimeUnit.SECONDS);
					}
				}).errorHandler(new ErrorHandler() {

					@Override
					public void onError(Session session, SessionError err) {
						Out.e("SessionCreator#sessionFactory.onError : '%s'", err.getMessage());
					}
				}).listener(new Listener() {

					@Override
					public void onSessionStateChanged(Session session, State oldState, State newState) {
						Out.t("SessionCreator#sessionFactory.onSessionStateChanged");
						Out.t("Session state changed from '%s' to '%s'", oldState, newState);

						if (!oldState.isConnected() && newState.isConnected()) {
							connectedSessions.incrementAndGet();
						}

						if (oldState.isConnected() && !newState.isConnected()) {
							connectedSessions.decrementAndGet();
						}

						if (!oldState.isRecovering() && newState.isRecovering()) {
							recoveringSessions.incrementAndGet();
						}

						if (oldState.isRecovering() && !newState.isRecovering()) {
							recoveringSessions.decrementAndGet();
						}

						if (oldState == State.CONNECTING) {
							// No not increment closed sessions when the client failed to connect
							return;
						}

						if (newState.isClosed()) {
							closedSessions.incrementAndGet();

							synchronized (sessionSetLock) {
								// Assumes sessions are only closed when shutting down
								// The state listener is called during the close call
								// Do not modify the sessions object when iterating over it
								if (newState != State.CLOSED_BY_CLIENT) {
									sessions.remove(session);
								}
							}
						}

						Out.t("Done SessionCreator#sessionFactory.onSessionStateChanged");
					}
				});

		state = CreatorState.INITIALISED;

		Out.t("Done SessionCreator constructor...");
	}

	/**
	 * Returns a random number between 1 (inclusive) and <code>max</code>
	 * (inclusive).
	 * 
	 * @param max
	 * @return random {@link Integer}
	 */
	private int getRandomReconnect(int max) {
		return ThreadLocalRandom.current().nextInt(1, max + 1);
	}

	public void start(final Set<InetSocketAddress> multiIpClientAddresses, int maxNumberSessions) {
		Out.t("SessionCreator#start");
		switch (state) {
		case INITIALISED:
			doStart(multiIpClientAddresses, maxNumberSessions);
			state = CreatorState.STARTED;
			break;
		default:
			break;
		}
		Out.t("Done SessionCreator#start");
	}

	private void doStart(final Set<InetSocketAddress> multiIpClientAddresses, int maxNumberSessions) {
		final Set<InetSocketAddress> myClientAddresses = new HashSet<>();
		if (multiIpClientAddresses == null || multiIpClientAddresses.isEmpty()) {
			myClientAddresses.add(new InetSocketAddress(0));
		} else {
			myClientAddresses.addAll(multiIpClientAddresses);
		}
		Out.t("SessionCreator#doStart for '%d' sessions and '%d' clientAddress", maxNumberSessions, myClientAddresses.size());
		long delay = 0L;
		do {
			for (InetSocketAddress tmpAddress : myClientAddresses) {
				final InetSocketAddress myAddress = tmpAddress;
				addSessions.add(Benchmarker.globalThreadPool.schedule(new Runnable() {

					@Override
					public void run() {

						Out.t("Adding session");
						try {

							/* ASYNC session creation */
							sessionFactory.localSocketAddress(myAddress).open(connectionString, new SessionFactory.OpenCallback() {

								@Override
								public void onError(ErrorReason errorReason) {
									Out.e("Connection failed: '%s'", errorReason);
									connectionFailures.incrementAndGet();
								}

								@Override
								public void onOpened(Session session) {
									synchronized (sessionSetLock) {
										sessions.add(session);
									}

									for (TopicSelector sel : topicSelectors) {
										Topics topicFeature = session.feature(Topics.class);
										topicFeature.addTopicStream(sel, new Topics.TopicStream() {

											@Override
											public void onError(ErrorReason reason) {
												if (!ErrorReason.SESSION_CLOSED.equals(reason)) {
													Out.e("TopicStream::OnError '%s'", reason);
												}
											}

											@Override
											public void onClose() {
											}

											@Override
											public void onUnsubscription(String topic, UnsubscribeReason arg1) {
											}

											@Override
											public void onTopicUpdate(String topic, Content content, UpdateContext context) {
												Out.d("Update for topic '%s'", topic);
												messageCount.incrementAndGet();
												messageByteCount.addAndGet(content.length());
											}

											@Override
											public void onSubscription(String topic, TopicDetails details) {
												Out.d("Subscribed to topic '%s'", topic);
											}
										});

										topicFeature.subscribe(sel, new CompletionCallback() {

											@Override
											public void onDiscard() {
												Out.t("SessionCreator#topics.onDiscard");
											}

											@Override
											public void onComplete() {
												Out.t("SessionCreator#topics.onComplete");
											}
										});
									}
								}
							});

							Out.t("Done Adding session");
						} catch (Throwable t) {
							/* ASYNC session creation */
							// synchronized (sessionListLock) {
							// connectionFailures++;
							// }
							Out.e("Exception caught trying to connect: '%s'", t.getMessage());
							if (Out.doLog(Out.OutLevel.DEBUG)) {
								t.printStackTrace();
							}

						}
					}
				}, ++delay % 500, TimeUnit.MILLISECONDS));
			}
		} while (--maxNumberSessions > 0);

		Out.t("Done SessionCreator#doStart");
	}

	public void stop() {
		Out.t("SessionCreator#stop");
		switch (state) {
		case STARTED:
			for (ScheduledFuture<?> tmpFuture : addSessions) {
				if (tmpFuture != null) {
					tmpFuture.cancel(false);
				}
			}
			state = CreatorState.STOPPED;
			break;
		default:
			break;
		}
		Out.t("Done SessionCreator#stop");
	}

	public void shutdown() {
		Out.t("SessionCreator#shutdown");
		switch (state) {
		case STARTED:
			stop();
		case STOPPED:
			synchronized (sessionSetLock) {
				for (Session s : this.sessions) {
					s.close();
				}
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			state = CreatorState.SHUTDOWN;
			break;
		default:
			break;
		}
		Out.t("Done SessionCreator#shutdown");
	}
}
