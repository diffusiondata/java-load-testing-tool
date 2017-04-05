package com.pushtechnology.consulting;

import static com.pushtechnology.consulting.Out.OutLevel.parse;
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

import com.pushtechnology.diffusion.api.APIException;
import com.pushtechnology.diffusion.api.config.ConfigManager;
import com.pushtechnology.diffusion.api.config.ThreadPoolConfig;
import com.pushtechnology.diffusion.api.config.ThreadsConfig;
import com.pushtechnology.diffusion.client.topics.details.TopicType;

public final class Benchmarker {

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
    private static ControlClientCreator controlClientCreator;
    private static ScheduledFuture<?> controlClientCounter;

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

    private static boolean doCreateControlClients;
    private static String controlClientsConnectionString;
    private static String controlClientsUsername = EMPTY;
    private static String controlClientsPassword = EMPTY;
    private static int maxNumControlClients;

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
        Out.i("Starting Java Benchmark Suite v%s", Out.version);
        parseArgs(args);

        try {
            Out.d("Trying to set client InboundThreadPool queue size to '%d'", CLIENT_INBOUND_QUEUE_QUEUE_SIZE);
            final ThreadsConfig threadsConfig = ConfigManager.getConfig().getThreads();
            final ThreadPoolConfig inboundPool = threadsConfig.addPool(CLIENT_INBOUND_THREAD_POOL_NAME);
            inboundPool.setQueueSize(CLIENT_INBOUND_QUEUE_QUEUE_SIZE);
            inboundPool.setCoreSize(CLIENT_INBOUND_QUEUE_CORE_SIZE);
            inboundPool.setMaximumSize(CLIENT_INBOUND_QUEUE_MAX_SIZE);
            threadsConfig.setInboundPool(CLIENT_INBOUND_THREAD_POOL_NAME);
            Out.d( "Successfully set client InboundThreadPool queue size to '%d'", CLIENT_INBOUND_QUEUE_QUEUE_SIZE);
        }
        catch (APIException ex) {
            Out.e("Failed to set client inbound pool size to '%d'",
                CLIENT_INBOUND_QUEUE_QUEUE_SIZE);
            ex.printStackTrace();
        }

        connectThreadPool = Executors.newScheduledThreadPool(connectThreadPoolSize);

        if (doPublish) {
            Out.i("Creating Publisher with connection string: '%s'", publisherConnectionString);
            publisher = new Publisher(publisherConnectionString, publisherUsername, publisherPassword, topics, topicType);
            publisher.start();

            publisherMonitor = globalThreadPool.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    Out.t("publisherMonitor fired");
                    Out.i("Publisher " + ((publisher.isOnStandby()) ? "is" : "is not") + " on standby");
                    Out.i("There are %d publishers running for topics: '%s'",
                        publisher.getTopicUpdatersByTopic().size(),
                        ArrayUtils.toString(publisher.getTopicUpdatersByTopic().keySet()));
                    for (ScheduledFuture<?> svc : publisher.getTopicUpdatersByTopic().values()) {
                        if (svc.isCancelled()) {
                            Out.d("Service is cancelled...");
                        }
                        if (svc.isDone()) {
                            Out.d("Service is done...");
                        }
                    }
                    Out.t("Done publisherMonitor fired");
                }
            }, 2L, 5L, SECONDS);
        }

        if (doCreateSessions) {
            if (maxNumSessions > 0) {
                Out.i("Creating %d Sessions with connection string: '%s'", maxNumSessions, sessionConnectionString);
            }
            else {
                Out.i("Creating Sessions with connection string: '%s'", sessionConnectionString);
                Out.i("Creating Sessions at %d/second, disconnecting after %d seconds", sessionRate, sessionDuration);
            }
            sessionCreator = new SessionCreator(sessionConnectionString, myTopics, topicType);

            Out.i("Sessions: [Connected] [Started] [Recovering] [Closed] [Ended] [Failed]  | Messages: [Number] [Bytes]");
            sessionsCounter = globalThreadPool.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    Out.t("sessionsCounter fired");
                    Out.i("Sessions: %d %d %d %d %d %d  | Messages: %d %d",
                        sessionCreator.getConnectedSessions().get(),
                        sessionCreator.getStartedSessions().get(),
                        sessionCreator.getRecoveringSessions().get(),
                        sessionCreator.getClosedSessions().get(),
                        sessionCreator.getEndedSessions().get(),
                        sessionCreator.getConnectionFailures().get(),
                        sessionCreator.getMessageCount().getAndSet(0),
                        sessionCreator.getMessageByteCount().getAndSet(0));
                    Out.t("Done sessionsCounter fired");
                }
            }, 0L, 5L, SECONDS);

            if (maxNumSessions > 0) {
                sessionCreator.start(multiIpClientAddresses, maxNumSessions);
            }
            else {
                sessionCreator.start(multiIpClientAddresses, sessionRate, sessionDuration);
            }
        }

        if (doCreateControlClients) {
            Out.i("Creating %d ControlClients with connection string: '%s'", maxNumControlClients, controlClientsConnectionString);
            controlClientCreator = new ControlClientCreator(controlClientsConnectionString, controlClientsUsername, controlClientsPassword, paramTopics);
            controlClientCreator.start();

            controlClientCounter = globalThreadPool.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        Out.t("controlClientCounter fired");
                        Out.i("====== ControlClient Status ======");
                        Out.i("       %d connected", controlClientCreator.getControlClients().size());
                        Out.i("       %d attempting to connect", controlClientCreator.getConnectingControlClients().size());
                        Out.i("       %d closed by server", controlClientCreator.getClosedByServerControlClients().size());
                        Out.i("       %d closed", controlClientCreator.getClosedControlClients().size());
                        Out.i("       %d failed", controlClientCreator.getConnectionFailures());
                        if (controlClientCreator.getControlClients().size() > maxNumControlClients) {
                            Out.i("Reached at least %d ControlClients, stopping adding ControlClients...", maxNumControlClients);
                            controlClientCreator.stop();
                        }
                        Out.t("Done controlClientCounter fired");
                    }
                }, 2L, 5L, SECONDS);
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

        if (doCreateControlClients) {
            controlClientCreator.shutdown();
            controlClientCounter.cancel(false);
        }

        if (!globalThreadPool.isTerminated()) {
            try {
                globalThreadPool.awaitTermination(1L, SECONDS);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // need to figure out the wait policy here...
        Out.exit(0);
    }

    static void parseArgs(String[] args) {
        Out.t("Parsing args...");
        if (args == null || args.length == 0) {
            Out.usage(1, "Parameters are required, please ensure that these have been provided");
            return;
        }
        final String errStr = "'%s' takes %d parameters, please check the usage and try again";
        for (int i = 0; i < args.length; i++) {
            final String arg = args[i];
            switch (arg) {
            case "-logLevel":
                if (hasNext(args, i, 1)) {
                    Out.setLogLevel(parse(args[++i]));
                }
                else {
                    Out.usage(1, errStr, "-logLevel", 1);
                }
                break;
            case "-slf4j":
                Out.useSlf4j();
                break;
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
                    Out.usage(1, errStr, "-multiIpClient", 3);
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
                    Out.d("Creating Publisher with connection string: '%s', username: '%s' and password: '%s'",
                        publisherConnectionString, publisherUsername, publisherPassword);
                }
                else {
                    Out.usage(1, errStr, "-publish", 1);
                }
                break;
            case "-sessions":
                if (hasNext(args, i, 1)) {
                    doCreateSessions = true;
                    sessionConnectionString = args[++i];
                    maxNumSessions = parseInt(args[++i]);
                    Out.d("Creating %d Sessions with connection string: '%s'",
                        maxNumSessions, sessionConnectionString);
                }
                else {
                    Out.usage(1, errStr, "-sessions", 2);
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
                            Out.usage(1, errStr, "-sessionsRate", 3);
                        }
                    }
                    maxNumSessions = 0;
                    Out.d("Creating Sessions at rate %s duration %s with connection string: '%s'",
                        sessionRate, sessionDuration, sessionConnectionString);
                }
                else {
                    Out.usage(1, errStr, "-sessionsRate", 3);
                }
                break;
            case "-controlClients":
                if (hasNext(args, i, 1)) {
                    doCreateControlClients = true;
                    controlClientsConnectionString = args[++i];
                    maxNumControlClients = parseInt(args[++i]);
                    if (hasNext(args, i, 1) && !args[i + 1].startsWith("-")) {
                        controlClientsUsername = args[++i];
                        if (hasNext(args, i, 1) && !args[i + 1].startsWith("-")) {
                            controlClientsPassword = args[++i];
                        }
                    }
                    Out.d(
                        "Creating %d ControlClients with connection string: '%s', username: '%s' and password: '%s'",
                        maxNumControlClients,
                        controlClientsConnectionString, controlClientsUsername,
                        controlClientsPassword);
                }
                else {
                    Out.usage(1, errStr, "-controlClients", 2);
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

                Out.d("Using topics: '%s'", join(paramTopics, " || "));
                if (paramTopics.size() == 0) {
                    Out.usage(1, "'-topics' requires at least 1 parameter, please check the usage and try again");
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

                Out.d("Using user topics: '%s'", join(myTopics, " || "));
                if (myTopics.size() == 0) {
                    Out.usage(1, "'-myTopics' requires at least 1 parameter, please check the usage and try again");
                }
                break;
            case "-topicType":
                if (hasNext(args, i, 1)) {
                    topicType = TopicType.valueOf(args[++i]);
                    Out.d("Topic type %s", topicType);
                }
                else {
                    Out.usage(1, errStr, "-topicType needs 1 string argument");
                }
                break;
            default:
                Out.usage(1, "Found invalid argument: '%s', please check the usage and try again", arg);
                break;
            }
        }

        topics.addAll(paramTopics);
        myTopics.addAll(topics);

        Out.t("Done parsing args...");
    }

    static boolean hasNext(String[] args, int index, int numNeeded) {
        return args.length > (index + numNeeded);
    }

    static void multiIpByHostname() {

        Out.t("Looking for InetAddesses on localhost");
        try {
            Out.d("Hostname is : '%s'", InetAddress.getLocalHost().getHostName());
            for (InetAddress tmpAddr : InetAddress.getAllByName(InetAddress.getLocalHost().getHostName())) {
                if (tmpAddr instanceof Inet4Address) {
                    Out.t("    Found IP: %s", tmpAddr.getHostAddress());
                    multiIpClientAddresses.add(new InetSocketAddress(tmpAddr.getHostAddress(), 0));
                }
            }
            for (InetSocketAddress tmp : multiIpClientAddresses) {
                if (tmp.isUnresolved()) {
                    Out.e("Could not resolve: '%s'", tmp);
                }
                else {
                    Out.d("Resolved: '%s'", tmp);
                }
            }
        }
        catch (UnknownHostException e) {
            Out.e("Error: '%s'", e.getLocalizedMessage());
            e.printStackTrace();
        }
    }

    static void multiIpByRange(String prefix, int start, int end) {
        Out.d("Using InetAddesses from %s.%s to %s.%s", prefix, start, prefix, end);
        for (int i = start; i <= end; i++) {
            Out.t("Adding IP '%s.%s'", prefix, i);
            multiIpClientAddresses.add(new InetSocketAddress(String.format("%s.%s", prefix, i), 0));
        }
        for (InetSocketAddress tmp : multiIpClientAddresses) {
            if (tmp.isUnresolved()) {
                Out.e("Could not resolve: '%s'", tmp);
            }
            else {
                Out.d("Resolved: '%s'", tmp);
            }
        }
    }
}
