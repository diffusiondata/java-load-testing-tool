package com.pushtechnology.consulting;

import static com.pushtechnology.diffusion.client.topics.details.TopicType.SINGLE_VALUE;
import static java.lang.Integer.parseInt;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.join;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pushtechnology.diffusion.api.APIException;
import com.pushtechnology.diffusion.api.config.ConfigManager;
import com.pushtechnology.diffusion.api.config.ThreadPoolConfig;
import com.pushtechnology.diffusion.api.config.ThreadsConfig;
import com.pushtechnology.diffusion.client.topics.details.TopicType;

public final class Benchmarker {

    private static final Logger LOG = LoggerFactory.getLogger(Benchmarker.class);

    private static final int CLIENT_INBOUND_QUEUE_QUEUE_SIZE = 5000000;
    private static final int CLIENT_INBOUND_QUEUE_CORE_SIZE = 16;
    private static final int CLIENT_INBOUND_QUEUE_MAX_SIZE = 16;
    private static final String CLIENT_INBOUND_THREAD_POOL_NAME = "JavaBenchmarkInboundThreadPool";
    private static final CountDownLatch RUNNING_LATCH = new CountDownLatch(1);

    public enum CreatorState {
        SHUTDOWN, INITIALISED, STOPPED, STARTED,
    }

    private static ScheduledFuture<?> publisherMonitor;
    private static SessionCreator sessionCreator;
    private static ScheduledFuture<?> sessionsCounter;

    public static ScheduledExecutorService globalThreadPool = Executors.newScheduledThreadPool(10);
    public static ScheduledExecutorService connectThreadPool;

    public static void main(String[] args) throws InterruptedException {
        LOG.info("Starting Java Benchmark Suite v{}", Benchmarker.class.getPackage().getImplementationVersion());

        final Arguments arguments = Arguments.parse(args);

        try {
            LOG.debug("Trying to set client InboundThreadPool queue size to '{}'", CLIENT_INBOUND_QUEUE_QUEUE_SIZE);
            final ThreadsConfig threadsConfig = ConfigManager.getConfig().getThreads();
            final ThreadPoolConfig inboundPool = threadsConfig.addPool(CLIENT_INBOUND_THREAD_POOL_NAME);
            inboundPool.setQueueSize(CLIENT_INBOUND_QUEUE_QUEUE_SIZE);
            inboundPool.setCoreSize(CLIENT_INBOUND_QUEUE_CORE_SIZE);
            inboundPool.setMaximumSize(CLIENT_INBOUND_QUEUE_MAX_SIZE);
            threadsConfig.setInboundPool(CLIENT_INBOUND_THREAD_POOL_NAME);
            LOG.debug("Successfully set client InboundThreadPool queue size to '{}'", CLIENT_INBOUND_QUEUE_QUEUE_SIZE);
        }
        catch (APIException ex) {
            LOG.error("Failed to set client inbound pool size to '{}'", CLIENT_INBOUND_QUEUE_QUEUE_SIZE);
            ex.printStackTrace();
        }

        connectThreadPool = Executors.newScheduledThreadPool(arguments.connectThreadPoolSize);
        final Publisher publisher;

        if (arguments.doPublish) {
            LOG.info("Creating Publisher with connection string: '{}'", arguments.publisherConnectionString);
            publisher = new Publisher(arguments.publisherConnectionString, arguments.publisherUsername, arguments.publisherPassword, arguments.topics,
                arguments.topicType);
            publisher.start();

            publisherMonitor = globalThreadPool.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    LOG.trace("publisherMonitor fired");
                    LOG.info("Publisher " + ((publisher.isOnStandby()) ? "is" : "is not") + " on standby");
                    LOG.info("There are {} publishers running for topics: '{}'",
                        publisher.getTopicUpdatersByTopic().size(),
                        ArrayUtils.toString(publisher.getTopicUpdatersByTopic().keySet()));
                    for (ScheduledFuture<?> svc : publisher.getTopicUpdatersByTopic().values()) {
                        if (svc.isCancelled()) {
                            LOG.debug("Service is cancelled...");
                        }
                        if (svc.isDone()) {
                            LOG.debug("Service is done...");
                        }
                    }
                    LOG.trace("Done publisherMonitor fired");
                }
            }, 2L, 5L, SECONDS);
        } else {
            publisher = null;
        }

        /* Create subscribing sessions. Finite or churning */
        if (arguments.doCreateSessions) {
            if (arguments.maxNumSessions > 0) {
                LOG.info("Creating {} Sessions with connection string: '{}'", arguments.maxNumSessions, arguments.sessionConnectionString);
            }
            else {
                LOG.info("Creating Sessions with connection string: '{}s'", arguments.sessionConnectionString);
                LOG.info("Creating Sessions at {}/second, disconnecting after {} seconds", arguments.sessionRate, arguments.sessionDuration);
            }
            sessionCreator = new SessionCreator(arguments.sessionConnectionString, arguments.myTopics, arguments.topicType);

            LOG.info("Sessions: [Connected] [Started] [Recovering] [Closed] [Ended] [Failed]  | Messages: [Number] [Bytes]");
            sessionsCounter = globalThreadPool.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    LOG.trace("sessionsCounter fired");
                    LOG.info("Sessions: {} {} {} {} {} {} | Messages: {} {}",
                        sessionCreator.getConnectedSessions().get(),
                        sessionCreator.getStartedSessions().get(),
                        sessionCreator.getRecoveringSessions().get(),
                        sessionCreator.getClosedSessions().get(),
                        sessionCreator.getEndedSessions().get(),
                        sessionCreator.getConnectionFailures().get(),
                        sessionCreator.getMessageCount().getAndSet(0),
                        sessionCreator.getMessageByteCount().getAndSet(0));
                    LOG.trace("Done sessionsCounter fired");
                }
            }, 0L, 5L, SECONDS);

            if (arguments.maxNumSessions > 0) {
                sessionCreator.start(arguments.maxNumSessions);
            }
            else {
                sessionCreator.start(arguments.sessionRate, arguments.sessionDuration);
            }
        }

        RUNNING_LATCH.await();

        if (arguments.doPublish) {
            if (publisher != null) {
                publisher.shutdown();
            }
            publisherMonitor.cancel(false);
        }

        if (arguments.doCreateSessions) {
            sessionCreator.shutdown();
            sessionsCounter.cancel(false);
        }

        if (!globalThreadPool.isTerminated()) {
            try {
                globalThreadPool.awaitTermination(1L, SECONDS);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static final class Arguments {
        /** Create and regularly update topics */
        /* package */ boolean doPublish;
        /* package */ String publisherConnectionString;
        /* package */ String publisherUsername = EMPTY;
        /* package */ String publisherPassword = EMPTY;

        /** Connect sessions, and subscribe. Optionally churn those sessions */
        /* package */ boolean doCreateSessions;
        /* package */ String sessionConnectionString;
        /* package */ int maxNumSessions;

        /** Sessions created per second. */
        /* package */ int sessionRate;
        /** Session duration in ms. */
        /* package */ int sessionDuration;

        /* package */ List<String> paramTopics = new ArrayList<>();
        /** Subscribed topics */
        /* package */ List<String> myTopics = new ArrayList<>();
        /* package */ List<String> topics = new ArrayList<>();
        /* package */ TopicType topicType = SINGLE_VALUE;
        /* package */ int connectThreadPoolSize = 10;

        /**
         * Parse & validate command line arguments.
         */
        static Arguments parse(String[] args) {
            LOG.trace("Parsing args...");
            final Arguments result = new Arguments();

            if (args == null || args.length == 0) {
                usage(1, "Parameters are required, please ensure that these have been provided");
                return result;
            }

            final String errStr = "'%s' takes %d parameters, please check the usage and try again";
            for (int i = 0;i < args.length;i++) {
                final String arg = args[i];
                switch (arg) {

                case "-publish":
                    if (hasNext(args, i, 1)) {
                        result.doPublish = true;
                        result.publisherConnectionString = args[++i];
                        if (hasNext(args, i, 1) && !args[i + 1].startsWith("-")) {
                            result.publisherUsername = args[++i];
                            if (hasNext(args, i, 1) && !args[i + 1].startsWith("-")) {
                                result.publisherPassword = args[++i];
                            }
                        }
                        LOG.debug("Creating Publisher with connection string: '{}', username: '{}' and password: '{}'",
                            result.publisherConnectionString, result.publisherUsername, result.publisherPassword);
                    }
                    else {
                        usage(1, errStr, "-publish", 1);
                    }
                    break;

                case "-sessions":
                    if (hasNext(args, i, 1)) {
                        result.doCreateSessions = true;
                        result.sessionConnectionString = args[++i];
                        result.maxNumSessions = parseInt(args[++i]);
                        LOG.debug("Creating {} Sessions with connection string: '{}'",
                            result.maxNumSessions, result.sessionConnectionString);
                    }
                    else {
                        usage(1, errStr, "-sessions", 2);
                    }
                    break;

                case "-sessionsRate":
                    /* fall through */
                case "-sessionRate":
                    if (hasNext(args, i, 1)) {
                        result.doCreateSessions = true;
                        result.sessionConnectionString = args[++i];
                        result.sessionRate = parseInt(args[++i]);
                        result.sessionDuration = parseInt(args[++i]);
                        if (hasNext(args, i, 1)) {
                            result.connectThreadPoolSize = parseInt(args[++i]);
                            if (result.connectThreadPoolSize > Runtime.getRuntime().availableProcessors() * 5) {
                                usage(1, errStr, "-sessionsRate", 3);
                            }
                        }
                        result.maxNumSessions = 0;
                        LOG.debug("Creating Sessions at rate {} duration {} with connection string: '{}'",
                            result.sessionRate, result.sessionDuration, result.sessionConnectionString);
                    }
                    else {
                        usage(1, errStr, "-sessionsRate", 3);
                    }
                    break;

                case "-topics":
                    while (hasNext(args, i, 1)) {
                        if (args[i + 1].startsWith("-")) {
                            break;
                        }
                        else {
                            result.paramTopics.add(args[++i]);
                        }
                    }

                    LOG.debug("Using topics: '{}'", join(result.paramTopics, " || "));
                    if (result.paramTopics.size() == 0) {
                        usage(1, "'-topics' requires at least 1 parameter, please check the usage and try again");
                    }
                    break;

                case "-myTopics":
                    while (hasNext(args, i, 1)) {
                        if (args[i + 1].startsWith("-")) {
                            break;
                        }
                        else {
                            result.myTopics.add(args[++i]);
                        }
                    }

                    LOG.debug("Using user topics: '{}'", join(result.myTopics, " || "));
                    if (result.myTopics.size() == 0) {
                        usage(1, "'-myTopics' requires at least 1 parameter, please check the usage and try again");
                    }
                    break;

                case "-topicType":
                    if (hasNext(args, i, 1)) {
                        result.topicType = TopicType.valueOf(args[++i]);
                        LOG.debug("Topic type {}", result.topicType);
                    }
                    else {
                        usage(1, errStr, "-topicType needs 1 string argument");
                    }
                    break;

                default:
                    usage(1, "Found invalid argument: '%s', please check the usage and try again", arg);
                    break;
                }
            }

            result.topics.addAll(result.paramTopics);
            result.myTopics.addAll(result.topics);

            LOG.trace("Done parsing args...");
            return result;
        }

        private static boolean hasNext(String[] args,int index,int numNeeded) {
            return args.length > (index + numNeeded);
        }

        private static void usage(int exitCode,String err,Object... args) {
            // FIXME: undocumented options like multiiphosts, etc.
            LOG.info(err, args);
            System.out.println("");
            System.out.println("Usage: JavaBenchmarkSuite.jar [params]");
            System.out.println("     -publish [connectionString] <username> <password> => enables the publisher for");
            System.out.println("          the given full connection string (e.g. 'dpt://10.10.10.10:8080') with");
            System.out.println("          an optional username and password.");
            System.out.println("     -sessions [connectionString] [maxSessions] => enables creation of [maxSessions]");
            System.out.println("          Sessions (UnifiedAPI) for the given full connection string");
            System.out.println("          (e.g. 'dpt://10.10.10.10:8080' or 'ws://10.10.10.10:8080')");
            System.out.println("     -sessionsRate [connectionString] [sessionRate] [sessionDuration] <connectPoolSize> ");
            System.out.println("          => enables creation of sessions at rate of [sessionRate] per second each lasting [sessionDuration] seconds");
            System.out.println("          Sessions (UnifiedAPI) for the given full connection string");
            System.out.println("          (e.g. 'ws://10.10.10.10:8080' or 'wss://10.10.10.10:8080')");
            System.out.println("          The connectPoolSize cannot exceed 5 * number of cpus. If more connect concurrency is needed then add more cpus.");
            System.out.println("     -topics [topic] <topics...> => subscribes any created Clients or Sessions to the");
            System.out.println("          given topic(s) in the format of: '<numberOfBytesPerMessage>/<messagesPerSecond>'");
            System.out.println("          NOTE: This application will automatically add the namespace and topic selector formatting!!!");
            System.out.println("          <topics...> => additional topics to subscribe to (these are optional,");
            System.out.println("          and as many as desired can be provided)");
            System.out.println("     -myTopics [topic] <topics...> => subscribes to user defined topics, REQUIRES the");
            System.out.println("          full topic path of EACH topic (can provide as many as you would like)");
            System.out.println("     -topicType [type] => specify the topic type");
            System.out.println("");
            System.exit(exitCode);
        }
    }

}
