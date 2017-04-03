package com.pushtechnology.consulting;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

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
import com.pushtechnology.diffusion.client.topics.details.TopicSpecification;
import com.pushtechnology.diffusion.client.topics.details.TopicType;
import com.pushtechnology.diffusion.client.types.UpdateContext;
import com.pushtechnology.diffusion.datatype.Bytes;
import com.pushtechnology.diffusion.datatype.json.JSON;

public class SessionCreator {

    private SessionFactory sessionFactory;
    private String connectionString[];
    List<TopicSelector> topicSelectors = new ArrayList<>();

    private List<ScheduledFuture<?>> addSessions = new ArrayList<>();

    private final Object sessionSetLock = new Object();

    public final Set<Session> sessions = new HashSet<>();
    public final AtomicInteger connectedSessions = new AtomicInteger(0);
    public final AtomicInteger recoveringSessions = new AtomicInteger();
    public final AtomicInteger closedSessions = new AtomicInteger(0);
    public final AtomicInteger endedSessions = new AtomicInteger(0);
    public final AtomicInteger startedSessions = new AtomicInteger(0);
    public final AtomicInteger connectionFailures = new AtomicInteger(0);
    public final AtomicInteger messageCount = new AtomicInteger(0);
    public final AtomicInteger messageByteCount = new AtomicInteger(0);

    // http://developer.reappt.io/docs/manual/html/developerguide/client/concepts/uci_reconnect.html
    private final int maxReconnectionIntervalSec = 10;
    // Set the maximum amount of time we'll try and reconnect for to 10 minutes.
    private final int maximumTimeoutDurationMs = 1000 * 60 * 10;

    public CreatorState state;
    private final TopicType topicType;

    public SessionCreator(String connectionString,List<String> topicSelectors,
        TopicType topicType) {
        Out.t("SessionCreator constructor...");

        this.connectionString = connectionString.split("[,]");
        this.topicType = topicType;
        Out.i("Creating sessions listening to topic selectors: '%s'",
            StringUtils.join(topicSelectors, ", "));
        for (String topicSelector : topicSelectors) {
            this.topicSelectors
                .add(Diffusion.topicSelectors().parse(topicSelector));
        }

        if (Integer.getInteger("bench.input.buffer.size", 0) > 0) {
            this.sessionFactory = Diffusion.sessions()
                .inputBufferSize(Integer.getInteger("bench.input.buffer.size"));
        }
        if (Integer.getInteger("bench.output.buffer.size", 0) > 0) {
            this.sessionFactory = Diffusion.sessions()
                .outputBufferSize(
                    Integer.getInteger("bench.output.buffer.size"));
        }

        this.sessionFactory = Diffusion.sessions()
            .connectionTimeout(60 * 1000)
            .reconnectionTimeout(120 * 1000)
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
                    }, getRandomReconnect(maxReconnectionIntervalSec),
                        TimeUnit.SECONDS);
                }
            })
            .reconnectionTimeout(maximumTimeoutDurationMs)
            .errorHandler(new ErrorHandler() {

                @Override
                public void onError(Session session,SessionError err) {
                    Out.e("SessionCreator#sessionFactory.onError : '%s'",
                        err.getMessage());
                }
            }).listener(new Listener() {

                @Override
                public void onSessionStateChanged(Session session,
                    State oldState,State newState) {
                    Out.t(
                        "SessionCreator#sessionFactory.onSessionStateChanged");
                    Out.t("Session state changed from '%s' to '%s'", oldState,
                        newState);

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

                    Out.t(
                        "Done SessionCreator#sessionFactory.onSessionStateChanged");
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

    public void start(final Set<InetSocketAddress> multiIpClientAddresses,
        int maxNumberSessions) {
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

    public void start(final Set<InetSocketAddress> multiIpClientAddresses,
        int sessionCreateRatePerSec,long sessionDurationMs) {
        Out.t("SessionCreator#start");
        switch (state) {
        case INITIALISED:
            doStart(multiIpClientAddresses, sessionCreateRatePerSec,
                sessionDurationMs);
            state = CreatorState.STARTED;
            break;
        default:
            break;
        }
        Out.t("Done SessionCreator#start");
    }

    private void doStart(final Set<InetSocketAddress> multiIpClientAddresses,
        int maxNumberSessions) {
        final Set<InetSocketAddress> myClientAddresses = new HashSet<>();
        if (multiIpClientAddresses == null ||
            multiIpClientAddresses.isEmpty()) {
            myClientAddresses.add(new InetSocketAddress(0));
        }
        else {
            myClientAddresses.addAll(multiIpClientAddresses);
        }
        Out.t("SessionCreator#doStart for '%d' sessions and '%d' clientAddress",
            maxNumberSessions, myClientAddresses.size());
        long delay = 0L;
        CountDownLatch sessionsLatch = new CountDownLatch(maxNumberSessions);
        final List<CountDownLatch> subscribeCdlList = new ArrayList();
        do {
            for (InetSocketAddress tmpAddress : myClientAddresses) {
                final InetSocketAddress myAddress = tmpAddress;
                addSessions
                    .add(Benchmarker.globalThreadPool.schedule(new Runnable() {

                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        @Override
                        public void run() {

                            Out.t("Adding session");
                            try {

                                /* ASYNC session creation */
                                sessionFactory.localSocketAddress(myAddress)
                                    .open(getConnectionString(),
                                        new SessionFactory.OpenCallback() {

                                            @Override
                                            public void onError(
                                                ErrorReason errorReason) {
                                                Out.e("Connection failed: '%s'",
                                                    errorReason);
                                                connectionFailures
                                                    .incrementAndGet();
                                                sessionsLatch.countDown();
                                            }

                                            @Override
                                            public void
                                                onOpened(Session session) {

                                                CountDownLatch cdl =
                                                    new CountDownLatch(
                                                        topicSelectors.size());
                                                subscribe(topicSelectors,
                                                    session,
                                                    new CompletionCallback() {

                                                        @Override
                                                        public void
                                                            onDiscard() {
                                                            Out.t(
                                                                "SessionCreator#topics.onDiscard");
                                                            cdl.countDown();
                                                        }

                                                        @Override
                                                        public void
                                                            onComplete() {
                                                            Out.t(
                                                                "SessionCreator#topics.onComplete");
                                                            cdl.countDown();

                                                        }
                                                    });

                                                synchronized (sessionSetLock) {
                                                    sessions.add(session);
                                                }
                                                Out.t("Session open complete");
                                                sessionsLatch.countDown();
                                                subscribeCdlList.add(cdl);
                                            }
                                        });

                                Out.t("Done submitting session open");
                            }
                            catch (Throwable t) {
                                /* ASYNC session creation */
                                // synchronized (sessionListLock) {
                                // connectionFailures++;
                                // }
                                connectionFailures.incrementAndGet();
                                sessionsLatch.countDown();
                                Out.e(
                                    "Exception caught trying to connect: '%s'",
                                    t.getMessage());
                                if (Out.doLog(Out.OutLevel.DEBUG)) {
                                    t.printStackTrace();
                                }

                            }
                        }

                        private String getConnectionString() {
                            if (connectionString.length == 1) {
                                return connectionString[0];
                            }
                            return connectionString[rnd
                                .nextInt(connectionString.length)];
                        }
                    }, ++delay % 500, TimeUnit.MILLISECONDS));
            }
        }
        while (--maxNumberSessions > 0);

        int remainingWait = 900;
        try {
            // ensure all sessions submitted
            sessionsLatch.await(remainingWait, TimeUnit.SECONDS);
            // now can countdown the subscriptions
            for (CountDownLatch cdl : subscribeCdlList) {
                long start = System.currentTimeMillis();
                cdl.await(remainingWait, TimeUnit.SECONDS);
                long elapsed = (System.currentTimeMillis() - start) / 1000;
                if (remainingWait > elapsed) {
                    remainingWait -= elapsed;
                }
                else {
                    remainingWait = 0;
                }
            }
        }
        catch (InterruptedException e) {
            Out.e("Exception caught waiting for sessions to open: '%s'",
                e.getMessage());
            if (Out.doLog(Out.OutLevel.DEBUG)) {
                e.printStackTrace();
            }
        }
        // all connected/failed but not processed all the subscriptions yet.
        writeSteadyStateFlagFile();

        Out.t("Done SessionCreator#doStart");
    }

    /**
     * Session churn
     *
     * @param multiIpClientAddresses
     * @param sessionCreateRatePerSec
     * @param sessionDurationMs
     */
    private void doStart(Set<InetSocketAddress> multiIpClientAddresses,
        long sessionCreateRatePerSec,long sessionDurationSec) {

        final Set<InetSocketAddress> myClientAddresses = new HashSet<>();
        if (multiIpClientAddresses == null ||
            multiIpClientAddresses.isEmpty()) {
            myClientAddresses.add(new InetSocketAddress(0));
        }
        else {
            myClientAddresses.addAll(multiIpClientAddresses);
        }
        Out.t(
            "SessionCreator#doStart for '%d' sessions/second and '%d' sessionDurationMs",
            sessionCreateRatePerSec, sessionDurationSec);

        long interval = 1000 / sessionCreateRatePerSec;
        long now = System.currentTimeMillis();
        boolean writtenSteadyStateFlagFile = false;

        do {
            for (InetSocketAddress tmpAddress : myClientAddresses) {
                final InetSocketAddress myAddress = tmpAddress;
                try {
                    startedSessions.incrementAndGet();
                    Benchmarker.connectThreadPool.submit(new Runnable() {

                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        @Override
                        public void run() {

                            Out.t("Adding session");
                            try {

                                /* ASYNC session creation */
                                sessionFactory.localSocketAddress(myAddress)
                                    .open(getConnectionString(),
                                        new SessionFactory.OpenCallback() {

                                            AtomicInteger selectorsCount =
                                                new AtomicInteger(
                                                    topicSelectors.size());

                                            @Override
                                            public void onError(
                                                ErrorReason errorReason) {
                                                Out.e("Connection failed: '%s'",
                                                    errorReason);
                                                connectionFailures
                                                    .incrementAndGet();
                                            }

                                            @Override
                                            public void
                                                onOpened(Session session) {

                                                subscribe(topicSelectors,
                                                    session,
                                                    new CompletionCallback() {

                                                        @Override
                                                        public void
                                                            onDiscard() {
                                                            Out.t(
                                                                "SessionCreator#topics.onDiscard");
                                                        }

                                                        @Override
                                                        public void
                                                            onComplete() {
                                                            Out.t(
                                                                "SessionCreator#topics.onComplete");
                                                            if (selectorsCount
                                                                .decrementAndGet() <= 0) {
                                                                setupDisconnectPhase(
                                                                    session,
                                                                    sessionDurationSec);
                                                            }
                                                        }
                                                    });

                                                synchronized (sessionSetLock) {
                                                    sessions.add(session);
                                                }
                                                Out.t("Session open complete");
                                            }
                                        });

                                Out.t("Done submitting session open");
                            }
                            catch (Throwable t) {
                                /* ASYNC session creation */
                                // synchronized (sessionListLock) {
                                // connectionFailures++;
                                // }
                                connectionFailures.incrementAndGet();
                                Out.e(
                                    "Exception caught trying to connect: '%s'",
                                    t.getMessage());
                                if (Out.doLog(Out.OutLevel.DEBUG)) {
                                    t.printStackTrace();
                                }

                            }
                        }

                        private String getConnectionString() {
                            if (connectionString.length == 1) {
                                return connectionString[0];
                            }
                            return connectionString[rnd
                                .nextInt(connectionString.length)];
                        }
                    });
                }
                catch (Exception e) {
                    Out.e("Exception caught when submitting session open ",
                        e.getMessage());
                    connectionFailures.incrementAndGet();
                }
            }

            if (connectedSessions.get() >= sessionCreateRatePerSec *
                sessionDurationSec &&
                !writtenSteadyStateFlagFile) {
                writeSteadyStateFlagFile();
                writtenSteadyStateFlagFile = true;
            }

            now = now + interval;
            LockSupport.parkUntil(now);
        }
        while (true);
    }

    private void subscribe(List<TopicSelector> topicSelectors,Session session,
        CompletionCallback completionCallback) {

        for (TopicSelector sel : topicSelectors) {
            Topics topicFeature = session.feature(Topics.class);
            if (topicType == TopicType.SINGLE_VALUE) {
                topicFeature.addTopicStream(sel, new SingleValueTopicStream());
            }
            else if (topicType == TopicType.BINARY) {
                topicFeature.addStream(sel, Bytes.class,
                    new BytesValueStream());
            }
            else if (topicType == TopicType.JSON) {
                topicFeature.addStream(sel, JSON.class, new JsonStream());
            }

            topicFeature.subscribe(sel, completionCallback);
        }
    }

    private class SingleValueTopicStream implements Topics.TopicStream {

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
        public void onUnsubscription(String topic,UnsubscribeReason arg1) {
        }

        @Override
        public void onTopicUpdate(String topic,Content content,
            UpdateContext context) {
            updateCounters(topic, content.length());
        }

        @Override
        public void onSubscription(String topic,TopicDetails details) {
            Out.d("Subscribed to topic '%s'", topic);
        }
    }

    /**
     * The value stream.
     */
    private final class JsonStream implements Topics.ValueStream<JSON> {

        @Override
        public void onSubscription(String arg0,TopicSpecification arg1) {
            // TODO Auto-generated method stub

        }

        @Override
        public void onUnsubscription(String arg0,TopicSpecification arg1,
            UnsubscribeReason arg2) {
        }

        @Override
        public void onClose() {
        }

        @Override
        public void onError(ErrorReason arg0) {
        }

        @Override
        public void onValue(String topic,TopicSpecification arg1,JSON oldValue,
            JSON newValue) {
            updateCounters(topic, newValue.length());
        }

    }

    /**
     * The value stream.
     */
    private final class BytesValueStream implements Topics.ValueStream<Bytes> {

        @Override
        public void onValue(String topicPath,TopicSpecification specification,
            Bytes oldValue,Bytes newValue) {

            updateCounters(topicPath, newValue.toByteArray().length);
        }

        @Override
        public void onSubscription(String arg0,TopicSpecification arg1) {
        }

        @Override
        public void onUnsubscription(String arg0,TopicSpecification arg1,
            UnsubscribeReason arg2) {
        }

        @Override
        public void onClose() {
        }

        @Override
        public void onError(ErrorReason arg0) {
        }
    }

    private void updateCounters(String topic,int length) {
        Out.d("onTopicUpdate for topic '%s'", topic);
        messageCount.incrementAndGet();
        messageByteCount.addAndGet(length);
    }

    private static volatile ScheduledExecutorService closeExecutor =
        Executors.newScheduledThreadPool(10);

    protected void setupDisconnectPhase(Session session,long sessionDuration) {
        if (sessionDuration > 0) {
            closeExecutor.schedule(new SessionCloseTask(session),
                sessionDuration, TimeUnit.SECONDS);
        }
    }

    class SessionCloseTask implements Runnable {

        private Session session;

        public SessionCloseTask(Session session) {
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

    private void writeSteadyStateFlagFile() {
        File file = new File("steady_state");
        try {
            file.createNewFile();
            Out.i("Reached steady state (Wrote steady state file)");
        }
        catch (IOException e) {
            Out.e("Exception caught in writeSteadyStateFlagFile: '%s'",
                e.getMessage());
            if (Out.doLog(Out.OutLevel.DEBUG)) {
                e.printStackTrace();
            }
        }
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
                }
                catch (InterruptedException e) {
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
