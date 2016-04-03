package com.pushtechnology.consulting;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import com.pushtechnology.consulting.Out.OutLevel;
import com.pushtechnology.diffusion.api.APIException;
import com.pushtechnology.diffusion.api.config.ConfigManager;
import com.pushtechnology.diffusion.api.config.ThreadPoolConfig;
import com.pushtechnology.diffusion.api.config.ThreadsConfig;

public class Benchmarker {

	public enum CreatorState {
		SHUTDOWN, INITIALISED, STOPPED, STARTED,
	}

	public static ScheduledExecutorService globalThreadPool = Executors.newScheduledThreadPool(100);

	// params
	private static boolean doPublish;
	private static String publisherConnectionString;
	private static String publisherUsername = StringUtils.EMPTY;
	private static String publisherPassword = StringUtils.EMPTY;
	static Publisher publisher;
	static ScheduledFuture<?> publisherMonitor;

	private static boolean doCreateSessions;
	private static String sessionConnectionString;
	private static int maxNumSessions;
	static SessionCreator sessionCreator;
	static ScheduledFuture<?> sessionsCounter;

	private static boolean doCreateControlClients;
	private static String controlClientsConnectionString;
	private static String controlClientsUsername = StringUtils.EMPTY;
	private static String controlClientsPassword = StringUtils.EMPTY;
	private static int maxNumControlClients;
	static ControlClientCreator controlClientCreator;
	static ScheduledFuture<?> controlClientCounter;

	private static List<String> paramTopics = new ArrayList<>();
	private static List<String> myTopics = new ArrayList<>();
	private static List<String> topics = new ArrayList<>();

	private static Set<InetSocketAddress> multiIpClientAddresses = new HashSet<>();

	// static finals
	public static final String ROOT_TOPIC = "JavaBenchmarkSuite";
	private static final int CLIENT_INBOUND_QUEUE_QUEUE_SIZE = 5000000;
	private static final int CLIENT_INBOUND_QUEUE_CORE_SIZE = 16;
	private static final int CLIENT_INBOUND_QUEUE_MAX_SIZE = 16;
	private static final String CLIENT_INBOUND_THREAD_POOL_NAME = "JavaBenchmarkInboundThreadPool";

	public static void main(String[] args) {
		Out.i("Starting Java Benchmark Suite v%s", Out.version);
		parseArgs(args);

		try {
			Out.d("Trying to set client InboundThreadPool queue size to '%d'", CLIENT_INBOUND_QUEUE_QUEUE_SIZE);
			ThreadsConfig threadsConfig = ConfigManager.getConfig().getThreads();
			ThreadPoolConfig inboundPool = threadsConfig.addPool(CLIENT_INBOUND_THREAD_POOL_NAME);
			inboundPool.setQueueSize(CLIENT_INBOUND_QUEUE_QUEUE_SIZE);
			inboundPool.setCoreSize(CLIENT_INBOUND_QUEUE_CORE_SIZE);
			inboundPool.setMaximumSize(CLIENT_INBOUND_QUEUE_MAX_SIZE);
			inboundPool.setPriority(Thread.MAX_PRIORITY);
			threadsConfig.setInboundPool(CLIENT_INBOUND_THREAD_POOL_NAME);
			Out.d("Successfully set client InboundThreadPool queue size to '%d'", CLIENT_INBOUND_QUEUE_QUEUE_SIZE);
		} catch (APIException ex) {
			Out.e("Failed to set client inbound pool size to '%d'", CLIENT_INBOUND_QUEUE_QUEUE_SIZE);
			ex.printStackTrace();
		}

		if (doPublish) {
			Out.i("Creating Publisher with connection string: '%s'", publisherConnectionString);
			publisher = new Publisher(publisherConnectionString, publisherUsername, publisherPassword, topics);
			publisher.start();

			publisherMonitor = globalThreadPool.scheduleAtFixedRate(new Runnable() {

				@Override
				public void run() {
					Out.t("publisherMonitor fired");
					Out.i("Publisher " + ((publisher.onStandby) ? "is" : "is not") + " on standby");
					Out.i("There are %d publishers running for topics: '%s'", publisher.topicUpdatersByTopic.size(),
							ArrayUtils.toString(publisher.topicUpdatersByTopic.keySet()));
					for (ScheduledFuture<?> svc : publisher.topicUpdatersByTopic.values()) {
						if (svc.isCancelled()) {
							Out.d("Service is cancelled...");
						}
						if (svc.isDone()) {
							Out.d("Service is done...");
						}
					}
					Out.t("Done publisherMonitor fired");
				}
			}, 2L, 5L, TimeUnit.SECONDS);
		}

		if (doCreateSessions) {
			Out.i("Creating %d Sessions with connection string: '%s'", maxNumSessions, sessionConnectionString);
			sessionCreator = new SessionCreator(sessionConnectionString, myTopics);
			sessionCreator.start(multiIpClientAddresses, maxNumSessions);

			sessionsCounter = globalThreadPool.scheduleAtFixedRate(new Runnable() {

				@Override
				public void run() {
					Out.t("sessionsCounter fired");
					Out.i("====== Session Status ======");
					Out.i("       %d connected", sessionCreator.connectedSessions.get());
					Out.i("       %d reconnecting", sessionCreator.recoveringSessions.get());
					Out.i("       %d closed", sessionCreator.closedSessions.get());
					Out.i("       %d failed to start", sessionCreator.connectionFailures.get());
					Out.t("Done sessionsCounter fired");
				}
			}, 2L, 5L, TimeUnit.SECONDS);
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
					Out.i("       %d connected", controlClientCreator.controlClients.size());
					Out.i("       %d attempting to connect", controlClientCreator.connectingControlClients.size());
					Out.i("       %d closed by server", controlClientCreator.closedByServerControlClients.size());
					Out.i("       %d closed", controlClientCreator.closedControlClients.size());
					Out.i("       %d failed", controlClientCreator.connectionFailures);
					if (controlClientCreator.controlClients.size() > maxNumControlClients) {
						Out.i("Reached at least %d ControlClients, stopping adding ControlClients...", maxNumControlClients);
						controlClientCreator.stop();
					}
					Out.t("Done controlClientCounter fired");
				}
			}, 2L, 5L, TimeUnit.SECONDS);
		}

		Out.i("Press any key to exit...");
		try {
			(new BufferedReader(new InputStreamReader(System.in))).readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}

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
				globalThreadPool.awaitTermination(1L, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// need to figure out the wait policy here...
		Out.exit(0);
	}

	static void parseArgs(String[] args) {
		Out.t("Parsing args...");
		if (args == null) {
			Out.usage(1, "Parameters are required, please ensure that these have been provided!!!");
		}
		String errStr = "'%s' takes %d parameters, please check the usage and try again!";
		String tmpParam;
		for (int i = 0; i < args.length; i++) {
			tmpParam = args[i];
			switch (tmpParam) {
			case "-logLevel":
				if (hasNext(args, i, 1)) {
					Out.setLogLevel(OutLevel.parse(args[++i]));
				} else {
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
					String prefix = args[++i];
					if (hasNext(args, i, 1) && !args[i + 1].startsWith("-")) {
						String startIp = args[++i];
						if (hasNext(args, i, 1) && !args[i + 1].startsWith("-")) {
							String endIp = args[++i];
							multiIpByRange(prefix, Integer.parseInt(startIp), Integer.parseInt(endIp));
						}
					}
				} else {
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
					Out.d("Creating Publisher with connection string: '%s', username: '%s' and password: '%s'", publisherConnectionString, publisherUsername,
							publisherPassword);
				} else {
					Out.usage(1, errStr, "-publish", 1);
				}
				break;
			case "-sessions":
				if (hasNext(args, i, 1)) {
					doCreateSessions = true;
					sessionConnectionString = args[++i];
					maxNumSessions = Integer.parseInt(args[++i]);
					Out.d("Creating %d Sessions with connection string: '%s'", maxNumSessions, sessionConnectionString);
				} else {
					Out.usage(1, errStr, "-sessions", 2);
				}
				break;
			case "-controlClients":
				if (hasNext(args, i, 1)) {
					doCreateControlClients = true;
					controlClientsConnectionString = args[++i];
					maxNumControlClients = Integer.parseInt(args[++i]);
					if (hasNext(args, i, 1) && !args[i + 1].startsWith("-")) {
						controlClientsUsername = args[++i];
						if (hasNext(args, i, 1) && !args[i + 1].startsWith("-")) {
							controlClientsPassword = args[++i];
						}
					}
					Out.d("Creating %d ControlClients with connection string: '%s', username: '%s' and password: '%s'", maxNumControlClients,
							controlClientsConnectionString, controlClientsUsername, controlClientsPassword);
				} else {
					Out.usage(1, errStr, "-controlClients", 2);
				}
				break;
			case "-topics":
				while (hasNext(args, i, 1)) {
					if (args[i + 1].startsWith("-")) {
						break;
					} else {
						paramTopics.add(args[++i]);
					}
				}

				Out.d("Using topics: '%s'", StringUtils.join(paramTopics, " || "));
				if (paramTopics.size() == 0) {
					Out.usage(1, "'-topics' requires at least 1 parameter, please check the usage and try again!");
				}
				break;
			case "-myTopics":
				while (hasNext(args, i, 1)) {
					if (args[i + 1].startsWith("-")) {
						break;
					} else {
						myTopics.add(args[++i]);
					}
				}

				Out.d("Using user topics: '%s'", StringUtils.join(myTopics, " || "));
				if (myTopics.size() == 0) {
					Out.usage(1, "'-myTopics' requires at least 1 parameter, please check the usage and try again!");
				}
				break;
			default:
				Out.usage(1, "Found invalid argument: '%s', please check the usage and try again!", tmpParam);
				break;
			}
		}

		for (String tmpTopic : paramTopics) {
			topics.add(String.format("%s/%s", ROOT_TOPIC, tmpTopic));
		}

		myTopics.addAll(topics);

		Out.t("Done parsing args...");
	}

	static boolean hasNext(String[] args, int index, int numNeeded) {
		return (args.length > (index + numNeeded));
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
				} else {
					Out.d("Resolved: '%s'", tmp);
				}
			}
		} catch (UnknownHostException e) {
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
			} else {
				Out.d("Resolved: '%s'", tmp);
			}
		}
	}
}
