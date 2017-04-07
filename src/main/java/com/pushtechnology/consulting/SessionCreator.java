/*******************************************************************************
 * Copyright (C) 2017 Push Technology Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package com.pushtechnology.consulting;

import static com.pushtechnology.consulting.Benchmarker.CreatorState.INITIALISED;
import static com.pushtechnology.consulting.Benchmarker.CreatorState.SHUTDOWN;
import static com.pushtechnology.consulting.Benchmarker.CreatorState.STARTED;
import static com.pushtechnology.consulting.Benchmarker.CreatorState.STOPPED;
import static com.pushtechnology.diffusion.client.session.Session.State.CONNECTING;
import static com.pushtechnology.diffusion.client.topics.details.TopicType.BINARY;
import static com.pushtechnology.diffusion.client.topics.details.TopicType.JSON;
import static com.pushtechnology.diffusion.client.topics.details.TopicType.SINGLE_VALUE;
import static java.lang.Integer.getInteger;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.join;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pushtechnology.consulting.Benchmarker.CreatorState;
import com.pushtechnology.diffusion.client.Diffusion;
import com.pushtechnology.diffusion.client.callbacks.ErrorReason;
import com.pushtechnology.diffusion.client.content.Content;
import com.pushtechnology.diffusion.client.features.Topics;
import com.pushtechnology.diffusion.client.features.Topics.CompletionCallback;
import com.pushtechnology.diffusion.client.session.Session;
import com.pushtechnology.diffusion.client.session.Session.ErrorHandler;
import com.pushtechnology.diffusion.client.session.Session.Listener;
import com.pushtechnology.diffusion.client.session.Session.SessionError;
import com.pushtechnology.diffusion.client.session.Session.State;
import com.pushtechnology.diffusion.client.session.SessionFactory;
import com.pushtechnology.diffusion.client.session.reconnect.ReconnectionStrategy;
import com.pushtechnology.diffusion.client.topics.TopicSelector;
import com.pushtechnology.diffusion.client.topics.details.TopicSpecification;
import com.pushtechnology.diffusion.client.topics.details.TopicType;
import com.pushtechnology.diffusion.client.types.UpdateContext;
import com.pushtechnology.diffusion.datatype.Bytes;
import com.pushtechnology.diffusion.datatype.json.JSON;

/**
 * Create topic subscribers.
 *
 * @author Push Technology Consulting.
 */
/* package */ final class SessionCreator {

    private static final Logger LOG = LoggerFactory.getLogger(SessionCreator.class);

    private final SessionFactory sessionFactory;
    private final List<String> connectionStrings;
    private final List<TopicSelector> topicSelectors = new ArrayList<>();
    private final List<ScheduledFuture<?>> addSessions = new ArrayList<>();
    private final Object sessionSetLock = new Object();
    private final Set<Session> sessions = new HashSet<>();

    private final AtomicInteger connectedSessions = new AtomicInteger(0);
    private final AtomicInteger recoveringSessions = new AtomicInteger();
    private final AtomicInteger closedSessions = new AtomicInteger(0);
    private final AtomicInteger endedSessions = new AtomicInteger(0);
    private final AtomicInteger startedSessions = new AtomicInteger(0);
    private final AtomicInteger connectionFailures = new AtomicInteger(0);
    private final AtomicInteger messageCount = new AtomicInteger(0);
    private final AtomicInteger messageByteCount = new AtomicInteger(0);

    // http://developer.reappt.io/docs/manual/html/developerguide/client/concepts/uci_reconnect.html
    private final int maxReconnectionIntervalSec = 10;
    // Set the maximum amount of time we'll try and reconnect for to 10 minutes.
    private final int maximumTimeoutDurationMs = 1000 * 60 * 10;

    private CreatorState state;
    private final TopicType topicType;

    SessionCreator(String connectionString, List<String> topicSelectors, TopicType topicType) {
        LOG.trace("SessionCreator constructor...");

        this.connectionStrings = asList(connectionString.split("[,]"));
        this.topicType = topicType;
        LOG.info("Creating sessions listening to topic selectors: '{}'", join(topicSelectors, ", "));
        for (String topicSelector : topicSelectors) {
            this.topicSelectors.add(Diffusion.topicSelectors().parse(topicSelector));
        }

        SessionFactory factory = Diffusion.sessions()
            .connectionTimeout(60 * 1000)
            .recoveryBufferSize(8000)
            .reconnectionStrategy(new ReconnectionStrategy() {

                @Override
                public void performReconnection(
                    final ReconnectionAttempt reconnection) {
                    Benchmarker.globalThreadPool.schedule(new Runnable() {
                        @Override
                        public void run() {
                            reconnection.start();
                        }
                    }, getRandomReconnect(maxReconnectionIntervalSec), SECONDS);
                }

            })
            .reconnectionTimeout(maximumTimeoutDurationMs)
            .errorHandler(new ErrorHandler() {

                @Override
                public void onError(Session session, SessionError err) {
                    LOG.error("SessionCreator#sessionFactory.onError : '{}'", err.getMessage());
                }

            }).listener(new Listener() {

                @Override
                public void onSessionStateChanged(Session session, State oldState, State newState) {
                    LOG.trace("SessionCreator#sessionFactory.onSessionStateChanged");
                    LOG.trace("Session state changed from '{}' to '{}'", oldState, newState);

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

                    if (oldState == CONNECTING) {
                        // No not increment closed sessions when the client
                        // failed to connect
                        return;
                    }

                    if (newState.isClosed()) {
                        closedSessions.incrementAndGet();

                        synchronized (sessionSetLock) {
                            // Assumes sessions are only closed when shutting
                            // down
                            // The state listener is called during the close
                            // call
                            // Do not modify the sessions object when iterating
                            // over it
                            if (newState != State.CLOSED_BY_CLIENT) {
                                sessions.remove(session);
                            }
                        }
                    }

                    LOG.trace("Done SessionCreator#sessionFactory.onSessionStateChanged");
                }
            });

        if (Integer.getInteger("bench.input.buffer.size", 0) > 0) {
            factory = factory.inputBufferSize(getInteger("bench.input.buffer.size"));
        }

        if (Integer.getInteger("bench.output.buffer.size", 0) > 0) {
            factory = factory.outputBufferSize(getInteger("bench.output.buffer.size"));
        }
        this.sessionFactory = factory;

        state = INITIALISED;
        LOG.trace("Done SessionCreator constructor...");
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

    public void start(int maxNumberSessions) {
        LOG.trace("SessionCreator#start");
        switch (state) {
        case INITIALISED:
            doStart(maxNumberSessions);
            state = STARTED;
            break;
        default:
            break;
        }
        LOG.trace("Done SessionCreator#start");
    }

    public void start(int sessionCreateRatePerSec, long sessionDurationMs) {
        LOG.trace("SessionCreator#start");
        switch (state) {
        case INITIALISED:
            doStart(sessionCreateRatePerSec, sessionDurationMs);
            state = STARTED;
            break;
        default:
            break;
        }
        LOG.trace("Done SessionCreator#start");
    }

    /**
     * Create a finite number of sessions.
     */
    private void doStart(int maxNumberSessions) {

        LOG.trace("SessionCreator#doStart for '{}' sessions ", maxNumberSessions);
        long delay = 0L;
        final CountDownLatch sessionsLatch = new CountDownLatch(maxNumberSessions);
        final CountDownLatch subscriptionLatch = new CountDownLatch(maxNumberSessions * topicSelectors.size());

        for (int i = 0; i < maxNumberSessions; i++) {
            addSessions.add(Benchmarker.globalThreadPool.schedule(new Runnable() {
                @Override
                public void run() {
                    LOG.trace("Adding session");
                    try {
                        /* ASYNC session creation */
                        sessionFactory.open(getConnectionString(), new OpenCallback(sessionsLatch, subscriptionLatch));
                        LOG.trace("Done submitting session open");
                    }
                    catch (Exception e) {
                        /* ASYNC session creation */
                        connectionFailures.incrementAndGet();
                        sessionsLatch.countDown();
                        LOG.error("Exception caught trying to connect", e);
                    }
                }

                private String getConnectionString() {
                    final ThreadLocalRandom rnd = ThreadLocalRandom.current();
                    return connectionStrings.get(rnd.nextInt(connectionStrings.size()));
                }

            }, ++delay % 500, MILLISECONDS));
        }

        try {
            final int remainingWait = 15 * 60;
            // Ensure all sessions connected
            sessionsLatch.await(remainingWait, SECONDS);

            // Countdown the subscriptions
            subscriptionLatch.await(remainingWait, SECONDS);
        }
        catch (InterruptedException e) {
            LOG.error("Exception caught waiting for sessions to open: '{}'", e);
        }
        // all connected/failed but not processed all the subscriptions yet.
        writeSteadyStateFlagFile();

        LOG.trace("Done SessionCreator#doStart");
    }

    /**
     * Session churn.
     *
     * @param multiIpClientAddresses
     * @param sessionCreateRatePerSec
     * @param sessionDurationMs
     */
    private void doStart(long sessionCreateRatePerSec, long sessionDurationSec) {

        LOG.trace("SessionCreator#doStart for '{}' sessions/second and '{}' sessionDurationMs", sessionCreateRatePerSec, sessionDurationSec);

        final long interval = 1000 / sessionCreateRatePerSec;
        long now = System.currentTimeMillis();

        do {
            try {
                startedSessions.incrementAndGet();
                Benchmarker.connectThreadPool.submit(new Runnable() {

                    @Override
                    public void run() {

                        LOG.trace("Adding session");
                        try {
                            /* ASYNC session creation */
                            sessionFactory.open(
                                getConnectionString(),
                                new OpenChurningSessionCallback(sessionDurationSec));
                            LOG.trace("Done submitting session open");
                        }
                        catch (Exception e) {
                            /* ASYNC session creation */
                            connectionFailures.incrementAndGet();
                            LOG.error("Exception caught trying to connect:", e);
                        }
                    }

                    private String getConnectionString() {
                        final ThreadLocalRandom rnd = ThreadLocalRandom.current();
                        return connectionStrings.get(rnd.nextInt(connectionStrings.size()));
                    }
                });
            }
            catch (Exception e) {
                LOG.error("Exception caught when submitting session open ", e.getMessage());
                connectionFailures.incrementAndGet();
            }
            if (connectedSessions.get() >= sessionCreateRatePerSec * sessionDurationSec) {
                writeSteadyStateFlagFile();
            }

            now = now + interval;
            LockSupport.parkUntil(now);
        }
        while (true);
    }

    private void subscribe(List<TopicSelector> selectors, Session session, CompletionCallback completionCallback) {
        for (TopicSelector sel : selectors) {
            final Topics topicFeature = session.feature(Topics.class);
            if (topicType == SINGLE_VALUE) {
                topicFeature.addTopicStream(sel, new SingleValueTopicStream());
            }
            else if (topicType == BINARY) {
                topicFeature.addStream(sel, Bytes.class, new BytesValueStream());
            }
            else if (topicType == JSON) {
                topicFeature.addStream(sel, JSON.class, new JsonStream());
            }

            topicFeature.subscribe(sel, completionCallback);
        }
    }

    private void updateCounters(String topic, int length) {
        LOG.debug("onTopicUpdate for topic '{}'", topic);
        messageCount.incrementAndGet();
        messageByteCount.addAndGet(length);
    }

    private void writeSteadyStateFlagFile() {
        final File file = new File("steady_state");
        try {
            file.createNewFile();
            LOG.info("Reached steady state (Wrote steady state file)");
        }
        catch (IOException e) {
            LOG.error("Exception caught in writeSteadyStateFlagFile: {}'", e);
        }
    }

    public void stop() {
        LOG.trace("SessionCreator#stop");
        switch (state) {
        case STARTED:
            for (ScheduledFuture<?> tmpFuture : addSessions) {
                if (tmpFuture != null) {
                    tmpFuture.cancel(false);
                }
            }
            state = STOPPED;
            break;
        default:
            break;
        }
        LOG.trace("Done SessionCreator#stop");
    }

    public void shutdown() {
        LOG.trace("SessionCreator#shutdown");
        switch (state) {
        case STARTED:
            stop();
            /* fall through */
        case STOPPED:
            synchronized (sessionSetLock) {
                for (Session s : this.sessions) {
                    s.close();
                }
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            state = SHUTDOWN;
            break;
        default:
            break;
        }
        LOG.trace("Done SessionCreator#shutdown");
    }

    /**
     * @return the connectedSessions
     */
    /* package */ AtomicInteger getConnectedSessions() {
        return connectedSessions;
    }

    /**
     * @return the startedSessions
     */
    /* package */ AtomicInteger getStartedSessions() {
        return startedSessions;
    }

    /**
     * @return the recoveringSessions
     */
    /* package */ AtomicInteger getRecoveringSessions() {
        return recoveringSessions;
    }

    /**
     * @return the closedSessions
     */
    /* package */ AtomicInteger getClosedSessions() {
        return closedSessions;
    }

    /**
     * @return the endedSessions
     */
    /* package */ AtomicInteger getEndedSessions() {
        return endedSessions;
    }

    /**
     * @return the connectionFailures
     */
    /* package */ AtomicInteger getConnectionFailures() {
        return connectionFailures;
    }

    /**
     * @return the messageCount
     */
    /* package */ AtomicInteger getMessageCount() {
        return messageCount;
    }

    /**
     * @return the messageByteCount
     */
    /* package */ AtomicInteger getMessageByteCount() {
        return messageByteCount;
    }

    private final class OpenChurningSessionCallback implements SessionFactory.OpenCallback {

        private ScheduledExecutorService closeExecutor = Executors.newScheduledThreadPool(10);
        private final AtomicInteger selectorsCount = new AtomicInteger(topicSelectors.size());
        private final long sessionDurationSec;

        OpenChurningSessionCallback(long sessionDurationSec) {
            this.sessionDurationSec = sessionDurationSec;
        }

        @Override
        public void onError(ErrorReason errorReason) {
            LOG.error("Connection failed: '{}'", errorReason);
            connectionFailures.incrementAndGet();
        }

        @Override
        public void onOpened(Session session) {
            subscribe(topicSelectors, session, new CompletionCallback.Default() {
                @Override
                public void onComplete() {
                    LOG.trace("SessionCreator#topics.onComplete");
                    if (selectorsCount.decrementAndGet() <= 0) {
                        setupDisconnectPhase(session, sessionDurationSec);
                    }
                }
            });

            synchronized (sessionSetLock) {
                sessions.add(session);
            }
            LOG.trace("Session open complete");
        }

        private void setupDisconnectPhase(Session session, long sessionDuration) {
            if (sessionDuration > 0) {
                closeExecutor.schedule(new SessionCloseTask(session), sessionDuration, SECONDS);
            }
        }
    }

    private final class OpenCallback implements SessionFactory.OpenCallback {
        private final CountDownLatch sessionsLatch;
        private final CountDownLatch subscriptionLatch;

        private OpenCallback(CountDownLatch sessionsLatch, CountDownLatch subscriptionLatch) {
            this.sessionsLatch = sessionsLatch;
            this.subscriptionLatch = subscriptionLatch;
        }

        @Override
        public void onError(ErrorReason errorReason) {
            LOG.error("Connection failed: '{}'", errorReason);
            connectionFailures.incrementAndGet();
            sessionsLatch.countDown();
        }

        @Override
        public void onOpened(Session session) {
            subscribe(topicSelectors, session, new CompletionCallback() {
                @Override
                public void onDiscard() {
                    LOG.trace("SessionCreator#topics.onDiscard");
                    subscriptionLatch.countDown();
                }

                @Override
                public void onComplete() {
                    LOG.trace("SessionCreator#topics.onComplete");
                    subscriptionLatch.countDown();
                }
            });

            synchronized (sessionSetLock) {
                sessions.add(session);
            }
            LOG.trace("Session open complete");
            sessionsLatch.countDown();
        }
    }

    private class SessionCloseTask implements Runnable {
        private Session session;

        SessionCloseTask(Session session) {
            this.session = session;
        }

        @Override
        public void run() {
            this.session.close();
            endedSessions.incrementAndGet();
            synchronized (sessionSetLock) {
                sessions.remove(session);
            }
        }
    }

    private class SingleValueTopicStream extends Topics.TopicStream.Default {
        @Override
        public void onError(ErrorReason reason) {
            if (!ErrorReason.SESSION_CLOSED.equals(reason)) {
                LOG.error("TopicStream::OnError '{}'", reason);
            }
        }

        @Override
        public void onTopicUpdate(String topic, Content content, UpdateContext context) {
            updateCounters(topic, content.length());
        }
    }

    /**
     * The value stream.
     */
    private final class JsonStream extends Topics.ValueStream.Default<JSON> {
        @Override
        public void onValue(String topic, TopicSpecification arg1, JSON oldValue, JSON newValue) {
            updateCounters(topic, newValue.length());
        }
    }

    /**
     * The value stream.
     */
    private final class BytesValueStream extends Topics.ValueStream.Default<Bytes> {
        @Override
        public void onValue(String topicPath, TopicSpecification specification, Bytes oldValue, Bytes newValue) {
            updateCounters(topicPath, newValue.toByteArray().length);
        }
    }
}
