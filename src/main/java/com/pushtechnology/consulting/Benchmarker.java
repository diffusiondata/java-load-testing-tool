package com.pushtechnology.consulting;

import static com.pushtechnology.diffusion.client.topics.details.TopicType.SINGLE_VALUE;
import static java.lang.Integer.parseInt;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.join;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

    private static Publisher publisher;
    private static ScheduledFuture<?> publisherMonitor;
    private static SessionCreator sessionCreator;
    private static ScheduledFuture<?> sessionsCounter;

    /** Create and regularly update topics */
    private static boolean doPublish;
    private static String publisherConnectionString;
    private static String publisherUsername = EMPTY;
    private static String publisherPassword = EMPTY;

    /** Connect sessions, and subscribe. Optionally churn those sessions */
    private static boolean doCreateSessions;
    private static String sessionConnectionString;
    private static int maxNumSessions;

    /** Sessions created per second. */
    private static int sessionRate;
    /** Session duration in ms. */
    private static int sessionDuration;

    private static List<String> paramTopics = new ArrayList<>();
    /** Subscribed topics */
    private static List<String> myTopics = new ArrayList<>();
    private static List<String> topics = new ArrayList<>();
    private static TopicType topicType = SINGLE_VALUE;

    private static Set<InetSocketAddress> multiIpClientAddresses = new HashSet<>();
    private static int connectThreadPoolSize = 10;
    public static ScheduledExecutorService globalThreadPool = Executors.newScheduledThreadPool(10);
    public static ScheduledExecutorService connectThreadPool;

    public static void main(String[] args) throws InterruptedException {
        LOG.info("Starting Java Benchmark Suite v{}", Benchmarker.class.getPackage().getImplementationVersion());
        parseArgs(args);

        try {
            LOG.debug("Trying to set client InboundThreadPool queue size to '{}'", CLIENT_INBOUND_QUEUE_QUEUE_SIZE);
            final ThreadsConfig threadsConfig = ConfigManager.getConfig().getThreads();
            final ThreadPoolConfig inboundPool = threadsConfig.addPool(CLIENT_INBOUND_THREAD_POOL_NAME);
            inboundPool.setQueueSize(CLIENT_INBOUND_QUEUE_QUEUE_SIZE);
            inboundPool.setCoreSize(CLIENT_INBOUND_QUEUE_CORE_SIZE);
            inboundPool.setMaximumSize(CLIENT_INBOUND_QUEUE_MAX_SIZE);
            threadsConfig.setInboundPool(CLIENT_INBOUND_THREAD_POOL_NAME);
            LOG.debug( "Successfully set client InboundThreadPool queue size to '{}'", CLIENT_INBOUND_QUEUE_QUEUE_SIZE);
        }
        catch (APIException ex) {
            LOG.error("Failed to set client inbound pool size to '{}'", CLIENT_INBOUND_QUEUE_QUEUE_SIZE);
            ex.printStackTrace();
        }

        connectThreadPool = Executors.newScheduledThreadPool(connectThreadPoolSize);

        if (doPublish) {
            LOG.info("Creating Publisher with connection string: '{}'", publisherConnectionString);
            publisher = new Publisher(publisherConnectionString, publisherUsername, publisherPassword, topics, topicType);
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
        }

        /* Create subscribing sessions. Finite or churning */
        if (doCreateSessions) {
            if (maxNumSessions > 0) {
                LOG.info("Creating {} Sessions with connection string: '{}'", maxNumSessions, sessionConnectionString);
            }
            else {
                LOG.info("Creating Sessions with connection string: '{}s'", sessionConnectionString);
                LOG.info("Creating Sessions at {}/second, disconnecting after {} seconds", sessionRate, sessionDuration);
            }
            sessionCreator = new SessionCreator(sessionConnectionString, myTopics, topicType);

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

            if (maxNumSessions > 0) {
                sessionCreator.start(multiIpClientAddresses, maxNumSessions);
            }
            else {
                sessionCreator.start(multiIpClientAddresses, sessionRate, sessionDuration);
            }
        }

        RUNNING_LATCH.await();

        if (doPublish) {
            publisher.shutdown();
            publisherMonitor.cancel(false);
        }

        if (doCreateSessions) {
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

        System.exit(0);
    }

    static void parseArgs(String[] args) {
        LOG.trace("Parsing args...");
        if (args == null || args.length == 0) {
            usage(1, "Parameters are required, please ensure that these have been provided");
            return;
        }
        final String errStr = "'%s' takes %d parameters, please check the usage and try again";
        for (int i = 0; i < args.length; i++) {
            final String arg = args[i];
            switch (arg) {
            case "-multiIpHost":
                multiIpByHostname();
                break;
            case "-multiIpRange":
                if (hasNext(args, i, 1)) {
                    final String prefix = args[++i];
                    if (hasNext(args, i, 1) && !args[i + 1].startsWith("-")) {
                        final String startIp = args[++i];
                        if (hasNext(args, i, 1) && !args[i + 1].startsWith("-")) {
                            final String endIp = args[++i];
                            multiIpByRange(prefix, parseInt(startIp), parseInt(endIp));
                        }
                    }
                }
                else {
                    usage(1, errStr, "-multiIpClient", 3);
                }
                break;
            case "-publish":
                if (hasNext(args, i, 1)) {
                    doPublish = true;
                    publisherConnectionString = args[++i];
                    if (hasNext(args, i, 1) && !args[i + 1].startsWith("-")) {
                        publisherUsername = args[++i];
                        if (hasNext(args, i, 1) && !args[i + 1].startsWith("-")) {
                            publisherPassword = args[++i];
                        }
                    }
                    LOG.debug("Creating Publisher with connection string: '{}', username: '{}' and password: '{}'",
                        publisherConnectionString, publisherUsername, publisherPassword);
                }
                else {
                    usage(1, errStr, "-publish", 1);
                }
                break;
            case "-sessions":
                if (hasNext(args, i, 1)) {
                    doCreateSessions = true;
                    sessionConnectionString = args[++i];
                    maxNumSessions = parseInt(args[++i]);
                    LOG.debug("Creating {} Sessions with connection string: '{}'",
                        maxNumSessions, sessionConnectionString);
                }
                else {
                    usage(1, errStr, "-sessions", 2);
                }
                break;
            case "-sessionsRate":
                /* fall through */
            case "-sessionRate":
                if (hasNext(args, i, 1)) {
                    doCreateSessions = true;
                    sessionConnectionString = args[++i];
                    sessionRate = parseInt(args[++i]);
                    sessionDuration = parseInt(args[++i]);
                    if (hasNext(args, i, 1)) {
                        connectThreadPoolSize = parseInt(args[++i]);
                        if (connectThreadPoolSize > Runtime.getRuntime().availableProcessors() * 5) {
                            usage(1, errStr, "-sessionsRate", 3);
                        }
                    }
                    maxNumSessions = 0;
                    LOG.debug("Creating Sessions at rate {} duration {} with connection string: '{}'",
                        sessionRate, sessionDuration, sessionConnectionString);
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
                        paramTopics.add(args[++i]);
                    }
                }

                LOG.debug("Using topics: '{}'", join(paramTopics, " || "));
                if (paramTopics.size() == 0) {
                    usage(1, "'-topics' requires at least 1 parameter, please check the usage and try again");
                }
                break;
            case "-myTopics":
                while (hasNext(args, i, 1)) {
                    if (args[i + 1].startsWith("-")) {
                        break;
                    }
                    else {
                        myTopics.add(args[++i]);
                    }
                }

                LOG.debug("Using user topics: '{}'", join(myTopics, " || "));
                if (myTopics.size() == 0) {
                    usage(1, "'-myTopics' requires at least 1 parameter, please check the usage and try again");
                }
                break;
            case "-topicType":
                if (hasNext(args, i, 1)) {
                    topicType = TopicType.valueOf(args[++i]);
                    LOG.debug("Topic type {}", topicType);
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

        topics.addAll(paramTopics);
        myTopics.addAll(topics);

        LOG.trace("Done parsing args...");
    }

    private static void usage(int exitCode,String err,Object... args) {
        // FIXME: undocumented options like multiiphosts, etc.
        LOG.info(err, args);
        System.out.println("");
        System.out.println("Usage: JavaBenchmarkSuite.jar [params]");
//        System.out.println("     -logLevel [level] => sets default log level to given level (default: info)");
//        System.out.println("     -slf4j => uses slf4j logging ((default: false)");
//        System.out.println("          You must use the -Dorg.slf4j.simpleLogger.defaultLogLevel=<level> to set the level ");
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


    static boolean hasNext(String[] args, int index, int numNeeded) {
        return args.length > (index + numNeeded);
    }

    static void multiIpByHostname() {

        LOG.trace("Looking for InetAddesses on localhost");
        try {
            LOG.debug("Hostname is : '{}'", InetAddress.getLocalHost().getHostName());
            for (InetAddress tmpAddr : InetAddress.getAllByName(InetAddress.getLocalHost().getHostName())) {
                if (tmpAddr instanceof Inet4Address) {
                    LOG.trace("    Found IP: {}", tmpAddr.getHostAddress());
                    multiIpClientAddresses.add(new InetSocketAddress(tmpAddr.getHostAddress(), 0));
                }
            }
            for (InetSocketAddress tmp : multiIpClientAddresses) {
                if (tmp.isUnresolved()) {
                    LOG.error("Could not resolve: '{}'", tmp);
                }
                else {
                    LOG.debug("Resolved: '{}'", tmp);
                }
            }
        }
        catch (UnknownHostException e) {
            LOG.error("Error: '{}'", e.getLocalizedMessage());
            e.printStackTrace();
        }
    }

    static void multiIpByRange(String prefix, int start, int end) {
        LOG.debug("Using InetAddesses from {}.{} to {}.{}", prefix, start, prefix, end);
        for (int i = start; i <= end; i++) {
            LOG.trace("Adding IP '{}.{}'", prefix, i);
            multiIpClientAddresses.add(new InetSocketAddress(String.format("%s.%s", prefix, i), 0));
        }
        for (InetSocketAddress tmp : multiIpClientAddresses) {
            if (tmp.isUnresolved()) {
                LOG.error("Could not resolve: '{}'", tmp);
            }
            else {
                LOG.debug("Resolved: '{}'", tmp);
            }
        }
    }
}
