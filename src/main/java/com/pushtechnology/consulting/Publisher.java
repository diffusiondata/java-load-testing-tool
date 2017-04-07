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
import static com.pushtechnology.diffusion.client.topics.details.TopicType.BINARY;
import static com.pushtechnology.diffusion.client.topics.details.TopicType.JSON;
import static java.lang.Integer.parseInt;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledFuture;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pushtechnology.consulting.Benchmarker.CreatorState;
import com.pushtechnology.diffusion.client.Diffusion;
import com.pushtechnology.diffusion.client.features.control.topics.TopicAddFailReason;
import com.pushtechnology.diffusion.client.features.control.topics.TopicControl;
import com.pushtechnology.diffusion.client.features.control.topics.TopicControl.AddCallback;
import com.pushtechnology.diffusion.client.features.control.topics.TopicUpdateControl;
import com.pushtechnology.diffusion.client.features.control.topics.TopicUpdateControl.Updater;
import com.pushtechnology.diffusion.client.features.control.topics.TopicUpdateControl.Updater.UpdateCallback;
import com.pushtechnology.diffusion.client.session.Session;
import com.pushtechnology.diffusion.client.session.Session.ErrorHandler;
import com.pushtechnology.diffusion.client.session.Session.Listener;
import com.pushtechnology.diffusion.client.session.Session.State;
import com.pushtechnology.diffusion.client.session.SessionFactory;
import com.pushtechnology.diffusion.client.topics.details.TopicType;
import com.pushtechnology.diffusion.datatype.binary.Binary;
import com.pushtechnology.diffusion.datatype.binary.BinaryDataType;
import com.pushtechnology.diffusion.datatype.json.JSON;
import com.pushtechnology.diffusion.datatype.json.JSONDataType;

/**
 * Create and regularly update a set of topics.
 *
 * @author Push Technology Consulting.
 */
/*package*/ final class Publisher {
    private static final Logger LOG = LoggerFactory.getLogger(Publisher.class);

    private final String connectionString;
    private final TopicType topicType;
    private SessionFactory sessionFactory;

    private Session session;
    private Updater updater;
    private Map<String, ScheduledFuture<?>> updaterFuturessByTopic = new HashMap<>();
    private List<ValidatedTopicPath> topicPaths;
    private CreatorState state;
    private RandomStringSource randomStrings = new RandomStringSource();

    Publisher(String connectionString, String username, String password, final List<String> topics, TopicType topicType) {
        LOG.trace("Publisher constructor");

        this.connectionString = connectionString;
        this.topicType = topicType;
        this.topicPaths = ValidatedTopicPath.parse(topics);

        this.sessionFactory = Diffusion.sessions().noReconnection().connectionTimeout(10000);
        if (!username.isEmpty()) {
            this.sessionFactory = this.sessionFactory.principal(username);
        }
        if (!password.isEmpty()) {
            this.sessionFactory = this.sessionFactory.password(password);
        }
        this.sessionFactory = this.sessionFactory
            .errorHandler(new ErrorHandler.Default())
            .listener(new Listener() {
                @Override
                public void onSessionStateChanged(Session theSession, State oldState, State newState) {
                    LOG.trace("Publisher#sessionFactory.onSessionStateChanged");
                    LOG.debug("Session state changed from '{}' to '{}'", oldState, newState);
                    switch (newState) {
                    case CONNECTED_ACTIVE:
                        LOG.debug("Session state is active registeringTopicControlAndSource...");
                        session = theSession;
                        updater = session.feature(TopicUpdateControl.class).updater();
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

    private void stopAllFeeds() {
        LOG.trace("Publisher#stopAllFeeds");
        LOG.debug("Stopping all feeds...");
        for (ScheduledFuture<?> future : updaterFuturessByTopic.values()) {
            future.cancel(false);
        }
        LOG.trace("Done Publisher#stopAllFeeds");
    }

    private void createTopic(ValidatedTopicPath validatedTopicPath) {
        LOG.trace("Publisher#createTopic");
        LOG.debug("Creating topic '{}' of type '{}'", validatedTopicPath, topicType);

        this.session.feature(TopicControl.class).addTopic(validatedTopicPath.getPath(), topicType, new AddCallback.Default() {

            @Override
            public void onTopicAdded(String topicPath) {
                LOG.info("Topic '{}' added.", topicPath);
                startFeed(validatedTopicPath);
            }

            @Override
            public void onTopicAddFailed(String topicPath, TopicAddFailReason reason) {
                LOG.trace("Publisher#TopicControlAddCallback.onTopicAddFailed");
                LOG.debug("topicAddFailed path: '{}', reason: '{}'", topicPath, reason);
                switch (reason) {
                case EXISTS:
                    startFeed(validatedTopicPath);
                    break;
                default:
                    LOG.error("Adding topic: '{}' failed for reason: '{}'", topicPath, reason);
                    break;
                }
                LOG.trace("Done Publisher#TopicControlAddCallback.onTopicAddFailed");
            }
        });
        LOG.trace("Done Publisher#createTopic");
    }

    private void startFeed(final ValidatedTopicPath topicPath) {
        LOG.trace("Publisher#startFeed for '{}'", topicPath);

        LOG.debug("Using messageSizeInBytes: '{}' and messagesPerSecond: '{}'", topicPath.getMessageSize(), topicPath.getFrequency());
        if (updaterFuturessByTopic.containsKey(topicPath.getPath())) {
            LOG.debug("Service is already running for topic '{}'", topicPath);
            return;
        }
        else {
            LOG.debug("Service not found, creating for '{}'", topicPath);
        }
        final long scheduleIntervalInMillis = 1000 / topicPath.getFrequency();
        LOG.info("Updating topic path {}', every '{}' ms", topicPath, scheduleIntervalInMillis);
        final ScheduledFuture<?> future = Benchmarker.globalThreadPool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                LOG.debug("updater.update() for topic path: '{}'", topicPath);
                update(topicPath.getPath(), new UpdateCallback.Default(), false, topicPath.getMessageSize());
            }
        }, 0L, scheduleIntervalInMillis, MILLISECONDS);
        LOG.debug("Started updater for '{}'", topicPath);
        updaterFuturessByTopic.put(topicPath.getPath(), future);

        LOG.trace("Done Publisher#startFeed for '{}'", topicPath);
    }

    @SuppressWarnings("unchecked")
    private void update(final String selector, UpdateCallback cb, boolean hasInitialContent, int messageSizeInBytes) {
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
                final JSONObject obj = new JSONObject();
                obj.put("time", System.nanoTime());
                obj.put("data", randomStrings.nextString(messageSizeInBytes));

                final JSONDataType jsonDataType = Diffusion.dataTypes().json();
                final JSON json = jsonDataType.fromJsonString(obj.toJSONString());
                Publisher.this.updater
                    .valueUpdater(JSON.class)
                    .update(selector, json, cb);
            }
            else {
                Publisher.this.updater.update(selector, Diffusion.content().newContent(randomStrings.nextString(messageSizeInBytes)), cb);
            }

        }
        catch (Exception e) {
            e.printStackTrace();
            LOG.error("Error in publisher loop", e);
        }
    }

    public void start() {
        LOG.trace("Publisher#start");
        switch (state) {
        case INITIALISED:
            this.session = this.sessionFactory.open(this.connectionString);
            for (ValidatedTopicPath topicPath : topicPaths) {
                createTopic(topicPath);
            }
            state = STARTED;
            break;
        default:
            break;
        }
        LOG.trace("Done Publisher#start");
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
     * @return the updaterFuturessByTopic
     */
    /*package*/ Map<String, ScheduledFuture<?>> getUpdaterFuturessByTopic() {
        return updaterFuturessByTopic;
    }

    /**
     * Source of random strings.
     */
    static final class RandomStringSource {

        private static final char[] ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz".toCharArray();
        private final Random random = new Random();

        public String nextString(int length) {
            final StringBuilder result = new StringBuilder(length);
            for (int i = 0; i < length; i++) {
                result.append(ALPHABET[random.nextInt(ALPHABET.length)]);
            }
            return result.toString();
        }
    }

    /**
     * Non mutable topic path adhering to the pattern string/string/size-in-bytes/messages-per-second
     * <p>
     * {@code size-in-bytes} and {@code messages-per-second} are decimal integers.
     * {@code size-in-bytes} must be positive.
     * {@code messages-per-second} must be positive and less than 1,000.
     */
    private static final class ValidatedTopicPath {
        private final String path;
        private final int messageSize;
        private final int frequency;

        /*package*/ ValidatedTopicPath(String path, int messageSize, int frequency) {
            this.path = path;
            this.messageSize = messageSize;
            this.frequency = frequency;
        }

        /*package*/ static List<ValidatedTopicPath> parse(List<String> topics) {
            final List<ValidatedTopicPath> result = new ArrayList<>(topics.size());
            for (String topic : topics) {
                result.add(parse(topic));
            }
            return result;
        }

        /*package*/ static ValidatedTopicPath parse(String topicPath) {
            final String[] paths = topicPath.split("/");
            if (paths.length != 4) {
                throw new IllegalArgumentException("Too few elements, need 4: " + topicPath);
            }

            try {
                final int messageSizeInBytes = parseInt(paths[2]);
                if (messageSizeInBytes < 0) {
                    throw new IllegalArgumentException("messageSizeInBytes must be positive: " + topicPath);
                }

                final int messagesPerSecond = parseInt(paths[3]);
                if (messagesPerSecond < 0 || messagesPerSecond > 1000) {
                    throw new IllegalArgumentException("messagesPerSecond must be in the range 1..1000: " + topicPath);
                }

                return new ValidatedTopicPath(topicPath, messageSizeInBytes, messagesPerSecond);
            }
            catch (NumberFormatException ex) {
                throw new IllegalArgumentException("Expecting numeric values in topic path elements 2 and 3: " + topicPath, ex);
            }
        }

        /**
         * @return the path
         */
        String getPath() {
            return path;
        }

        /**
         * @return the messageSize
         */
        int getMessageSize() {
            return messageSize;
        }

        /**
         * @return the number of updates a second
         */
        int getFrequency() {
            return frequency;
        }

        @Override
        public String toString() {
            return path;
        }
    }

}
