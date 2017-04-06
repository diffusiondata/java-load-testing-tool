package com.pushtechnology.consulting;

import static com.pushtechnology.consulting.Benchmarker.CreatorState.INITIALISED;
import static com.pushtechnology.consulting.Benchmarker.CreatorState.SHUTDOWN;
import static com.pushtechnology.consulting.Benchmarker.CreatorState.STARTED;
import static com.pushtechnology.diffusion.client.topics.details.TopicType.BINARY;
import static com.pushtechnology.diffusion.client.topics.details.TopicType.JSON;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pushtechnology.consulting.Benchmarker.CreatorState;
import com.pushtechnology.diffusion.client.Diffusion;
import com.pushtechnology.diffusion.client.callbacks.ErrorReason;
import com.pushtechnology.diffusion.client.features.RegisteredHandler;
import com.pushtechnology.diffusion.client.features.Topics;
import com.pushtechnology.diffusion.client.features.Topics.CompletionCallback;
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
import com.pushtechnology.diffusion.datatype.binary.Binary;
import com.pushtechnology.diffusion.datatype.binary.BinaryDataType;
import com.pushtechnology.diffusion.datatype.json.JSON;
import com.pushtechnology.diffusion.datatype.json.JSONDataType;

/*package*/ final class Publisher {
    private static final Logger LOG = LoggerFactory.getLogger(Publisher.class);

    private final String connectionString;
    private final String rootTopic;
    private final TopicType topicType;
    private SessionFactory sessionFactory;

    private Session session;
    private TopicControl topicControl;
    private TopicUpdateControl topicUpdateControl;
    private Updater updater;
    private Map<String,ScheduledFuture<?>> topicUpdatersByTopic = new HashMap<>();
    private boolean onStandby = true;
    private List<String> topicStandbyList = new ArrayList<>();
    private CreatorState state;
    private RandomString randomString;

    public Publisher(String connectionString,String username,String password,
        final List<String> topics,TopicType topicType) {
        LOG.trace("Publisher constructor");

        this.connectionString = connectionString;
        this.rootTopic = topics.get(0).split("/")[0];
        this.topicType = topicType;

        this.sessionFactory = Diffusion.sessions().noReconnection().connectionTimeout(10000);
        if (!username.isEmpty()) {
            this.sessionFactory = this.sessionFactory.principal(username);
            if (!password.isEmpty()) {
                this.sessionFactory = this.sessionFactory.password(password);
            }
        }
        this.sessionFactory = this.sessionFactory.errorHandler(new ErrorHandler() {

                @Override
                public void onError(Session session, SessionError err) {
                    LOG.error("SessionCreator#sessionFactory.onError : '{}'", err.getMessage());
                }

            }).listener(new Listener() {

                @Override
                public void onSessionStateChanged(Session theSession, State oldState, State newState) {
                    LOG.trace("Publisher#sessionFactory.onSessionStateChanged");
                    LOG.debug("Session state changed from '{}' to '{}'", oldState, newState);
                    switch (newState) {
                    case CONNECTED_ACTIVE:
                        LOG.debug("Session state is active registeringTopicControlAndSource...");
                        session = theSession;
                        registerTopicControlAndSource();
                        LOG.debug("Subscribing to topics...");
                        subscribeToTopics(topics);
                        break;
                    default:
                        break;
                    }
                    LOG.trace("Done Publisher#sessionFactory.onSessionStateChanged");
                }
            });

        state = INITIALISED;

        LOG.trace("Done Publisher constructor");
    }

    void stopAllFeeds() {
        LOG.trace("Publisher#stopAllFeeds");
        LOG.debug("Stopping all feeds...");
        for (ScheduledFuture<?> future : topicUpdatersByTopic.values()) {
            future.cancel(false);
        }
        LOG.trace("Done Publisher#stopAllFeeds");
    }

    void registerTopicControlAndSource() {
        LOG.trace("Publisher#registerTopicControlAndSource");

        LOG.debug("Setting up topic control...");
        this.topicControl = this.session.feature(TopicControl.class);
        this.topicControl.addMissingTopicHandler(rootTopic,
            new MissingTopicHandler() {

                @Override
                public void onClose(String topicPath) {
                    LOG.trace("MissingTopicHandler#OnClose for topic '{}'", topicPath);
                }

                @Override
                public void onActive(String topicPath,
                    RegisteredHandler registeredHandler) {
                    LOG.info("MissingTopicHandler active for topic '{}'", topicPath);
                }

                @Override
                public void
                    onMissingTopic(MissingTopicNotification notification) {
                    LOG.trace("Publisher#missingTopicHandler.OnMissingTopic");
                    String topicPath = notification.getTopicPath();
                    LOG.info("OnMissingTopic called for '{}'", topicPath);
                    createTopic(topicPath, topicType);
                    notification.proceed();
                    LOG.trace("Done Publisher#missingTopicHandler.OnMissingTopic");
                }
            });

        LOG.debug("Setting up topic update control (source)...");
        this.topicUpdateControl = this.session.feature(TopicUpdateControl.class);
        this.topicUpdateControl.registerUpdateSource(rootTopic, new UpdateSource.Default() {

                @Override
                public void onClose(String topicPath) {
                    LOG.trace("Publisher#TopicSource.onClosed");
                    Publisher.this.updater = null;
                    shutdown();
                    LOG.trace("Done Publisher#TopicSource.onClosed");
                }

                @Override
                public void onActive(String topicPath, Updater updater) {
                    LOG.trace("Publisher#TopicSource.onActive");
                    onStandby = false;
                    Publisher.this.updater = updater;

                    for (String topic : topicStandbyList) {
                        startFeed(topic);
                    }
                    LOG.trace("Done Publisher#TopicSource.onActive");
                }

                @Override
                public void onStandby(String topicPath) {
                    LOG.trace("Publisher#TopicSource.onStandby");
                    onStandby = true;
                    stopAllFeeds();
                    LOG.trace("Done Publisher#TopicSource.onStandby");
                }
            });

        LOG.trace("Done Publisher#registerTopicControlAndSource");
    }

    void subscribeToTopics(List<String> topics) {
        LOG.debug("Subscribing to topics: '{}'", ArrayUtils.toString(topics));
        for (String topic : topics) {
            final String sel = ">" + topic;
            LOG.debug("Subscribing to topic '{}'", sel);
            final Topics topicFeature = session.feature(Topics.class);
            topicFeature.addTopicStream(sel, new Topics.TopicStream.Default() {

                @Override
                public void onSubscription(String topicPath, TopicDetails details) {
                    startFeed(topicPath);
                }

            });

            topicFeature.subscribe(sel, new CompletionCallback.Default(){});
        }
    }

    void createTopic(String topicPath, TopicType topicType) {
        LOG.trace("Publisher#createTopic");
        LOG.debug("Creating topic '{}' of type '{}'", topicPath, topicType);
        this.topicControl.addTopic(topicPath, topicType, new AddCallback() {

            @Override
            public void onDiscard() {
                LOG.error("Publisher#TopicControlAddCallbackImpl.onDiscard() :: Notification that a call context was closed prematurely, typically due to a timeout or the session being closed.");
            }

            @Override
            public void onTopicAdded(String topicPath) {
                LOG.info("Topic '{}' added.", topicPath);
                startFeed(topicPath);
            }

            @Override
            public void onTopicAddFailed(String topicPath,
                TopicAddFailReason reason) {
                LOG.trace("Publisher#TopicControlAddCallback.onTopicAddFailed");
                LOG.debug("topicAddFailed path: '{}', reason: '{}'", topicPath,
                    reason);
                switch (reason) {
                case EXISTS:
                    startFeed(topicPath);
                    break;
                default:
                    LOG.error("Adding topic: '{}' failed for reason: '{}'",
                        topicPath, reason);
                    break;
                }
                LOG.trace(
                    "Done Publisher#TopicControlAddCallback.onTopicAddFailed");
            }
        });
        LOG.trace("Done Publisher#createTopic");
    }

    /**
     * FIXME: more detail on this needed.
     * 1 - path 2 - path 3 - messageSizeInBytes 4 - messagesPerSecond
     *
     * @param topicPath
     */
    void startFeed(final String topicPath) {
        LOG.trace("Publisher#startFeed for '{}'", topicPath);
        if (onStandby) {
            LOG.debug("Publisher#startFeed : OnStandby, adding to list and waiting...");
            topicStandbyList.add(topicPath);
            return;
        }
        LOG.debug("Trying to start feed for: '{}'", topicPath);
        final String[] paths = topicPath.split("/");
        final int expectedLength = 4;
        if (paths.length == 4) {
            LOG.debug("Found topicPath '{}' elements: '{}', '{}', '{}'", topicPath, paths[0] + "/" + paths[1], paths[2], paths[3]);
            final int messageSizeInBytes = NumberUtils.toInt(paths[2], -1);
            randomString = new RandomString(messageSizeInBytes);
            final int messagesPerSecond = NumberUtils.toInt(paths[3], -1);
            if (messageSizeInBytes > 0 && messagesPerSecond > 0 && messagesPerSecond <= 1000) {
                LOG.debug("Using messageSizeInBytes: '{}' and messagesPerSecond: '{}'", messageSizeInBytes, messagesPerSecond);
                ScheduledFuture<?> tmpFuture;
                if (topicUpdatersByTopic.containsKey(topicPath)) {
                    LOG.trace("Service already found, trying to pull by topicPath '{}'", topicPath);
                    tmpFuture = topicUpdatersByTopic.get(topicPath);
                    if (!tmpFuture.isDone() && !tmpFuture.isCancelled()) {
                        LOG.debug("Service is already running for topic '{}'", topicPath);
                        return;
                    }
                }
                else {
                    LOG.debug("Service not found, creating for '{}'", topicPath);
                }
                final Long scheduleIntervalInMillis = new Long(1000 / messagesPerSecond);
                LOG.info("Updating topic path {}', every '{}' ms", topicPath, scheduleIntervalInMillis);
                tmpFuture = Benchmarker.globalThreadPool.scheduleAtFixedRate(new Runnable() {

                        @Override
                        public void run() {
                            LOG.debug("updater.update() for topic path: '{}' {}", topicPath, messageSizeInBytes);
                            update(topicPath, new UpdateCallback() {

                                @Override
                                public void onError(ErrorReason error) {
                                    LOG.error("Error : '{}'", error);
                                }

                                @Override
                                public void onSuccess() {
                                    LOG.debug("Topic updated");
                                }
                            }, false, messageSizeInBytes);
                        }
                    }, 0L, scheduleIntervalInMillis, MILLISECONDS);
                LOG.debug("Started updater for '{}'", topicPath);
                topicUpdatersByTopic.put(topicPath, tmpFuture);
            }
            else {
                LOG.error("Could not parse topicPath: '{}', found messageSizeInBytes: '{}' and messagesPerSecond: {}'", topicPath, messageSizeInBytes, messagesPerSecond);
            }
        }
        else {
            LOG.error("Could not parse topicPath '{}', length was : {}, expected {}", expectedLength, topicPath, paths.length);
        }
        LOG.trace("Done Publisher#startFeed for '{}'", topicPath);
    }

    private void update(final String selector,UpdateCallback cb,
        boolean hasInitialContent,int messageSizeInBytes) {
        try {
            if (topicType == BINARY) {
                final byte[] bytes = ByteBuffer.allocate(messageSizeInBytes).putLong(System.nanoTime()).array();
                final BinaryDataType binaryDataType = Diffusion.dataTypes().binary();
                final Binary b = binaryDataType.readValue(bytes);
                Publisher.this.updater
                    .valueUpdater(Binary.class)
                    .update(selector, b, cb);
            }
            else if (topicType == JSON) {
                final String jsonString =
                    "{ \"Time\" : \"" + System.nanoTime() +
                    "\", \"Data\" : \"" + randomString.nextString() + "\"}";
                final JSONDataType jsonDataType = Diffusion.dataTypes().json();
                final JSON json = jsonDataType.fromJsonString(jsonString);
                Publisher.this.updater
                    .valueUpdater(JSON.class)
                    .update(selector, json, cb);
            }
            else {
                Publisher.this.updater.update(selector, Diffusion.content().newContent(createSizedByteArray(messageSizeInBytes)), cb);
            }

        }
        catch (Exception e) {
            e.printStackTrace();
            LOG.error("Error in publisher loop " + e.getMessage(), e);
        }
    }

    byte[] createSizedByteArray(int messageSizeInBytes) {
        byte[] bytes = new byte[messageSizeInBytes];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }

    void stopFeed(String topicPath) {
        LOG.trace("Publisher#stopFeed for '{}'", topicPath);
        LOG.debug("Trying to stop feed for: '{}'", topicPath);
        if (topicUpdatersByTopic.containsKey(topicPath)) {
            LOG.info("Stopping topic feed for '{}'", topicPath);
            topicUpdatersByTopic.get(topicPath).cancel(false);
        }
        LOG.trace("Done Publisher#stopFeed for '{}'", topicPath);
    }

    public void start() {
        LOG.trace("Publisher#start");
        switch (state) {
        case INITIALISED:
            doStart();
            state = STARTED;
            break;
        default:
            break;
        }
        LOG.trace("Done Publisher#start");
    }

    private void doStart() {
        LOG.trace("Publisher#doStart");
        this.session = this.sessionFactory.open(this.connectionString);
        LOG.trace("Done Publisher#doStart");
    }

    public void shutdown() {
        LOG.trace("Publisher#shutdown");
        switch (state) {
        case STARTED:
            stopAllFeeds();
            state = SHUTDOWN;
            break;
        default:
            break;
        }
        LOG.trace("Done Publisher#shutdown");
    }

    /**
     * @return the topicUpdatersByTopic
     */
    Map<String,ScheduledFuture<?>> getTopicUpdatersByTopic() {
        return topicUpdatersByTopic;
    }

    /**
     * @return the onStandby
     */
    boolean isOnStandby() {
        return onStandby;
    }

    //FIXME: javadocs, or a btter name
    class RandomString {

        private char[] symbols;
        private final Random random = new Random();
        private final char[] buf;

        public RandomString(int length) {

            StringBuilder tmp = new StringBuilder();
            for (char ch = '0';ch <= '9';++ch)
                tmp.append(ch);
            for (char ch = 'a';ch <= 'z';++ch)
                tmp.append(ch);
            symbols = tmp.toString().toCharArray();

            if (length < 1)
                throw new IllegalArgumentException("length < 1: " + length);
            buf = new char[length];
        }

        public String nextString() {
            for (int idx = 0;idx < buf.length;++idx) {
                buf[idx] = symbols[random.nextInt(symbols.length)];
            }
            return new String(buf);
        }
    }


}
