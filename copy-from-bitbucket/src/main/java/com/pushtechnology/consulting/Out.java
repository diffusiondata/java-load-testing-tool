package com.pushtechnology.consulting;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.impl.SimpleLoggerFactory;

public class Out {

	public static String version = Out.class.getPackage().getImplementationVersion();

	protected static enum OutLevel {
		TRACE(10, "[TRACE] "), DEBUG(20, "[DEBUG] "), INFO(30, "[INFO ] "), WARN(40, "[WARN ] "), ERROR(50, "[ERROR] "), ;

		int level;
		String levelString;

		OutLevel(int level, String levelString) {
			this.level = level;
			this.levelString = levelString;
		}

		int level() {
			return level;
		}

		String levelString() {
			return levelString;
		}

		public static OutLevel parse(String level) {

			OutLevel myLevel;
			try {
				myLevel = OutLevel.valueOf(level.toUpperCase());
			} catch (IllegalArgumentException ex) {
				e("No valid log level matches '%s', using INFO...", level);
				myLevel = OutLevel.INFO;
			}
			return myLevel;
		}

		public boolean doLog() {
			return defaultLevel.level() <= level();
		}
	}

	private static OutLevel defaultLevel = OutLevel.INFO;

	private static boolean slf4j = false;
	private static Logger defaultLog;
	private static Map<String, Logger> logMap = new HashMap<>();

	private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSS");

	public static void setLogLevel(OutLevel level) {
		defaultLevel = level;
	}

	public static boolean doLog(OutLevel level) {
		return level.doLog();
	}

	public static void useSlf4j() {
		SimpleLoggerFactory factory = new SimpleLoggerFactory();
		logMap.put(Benchmarker.class.getName(), factory.getLogger(Benchmarker.class.getName()));
		logMap.put(ControlClientCreator.class.getName(), factory.getLogger(ControlClientCreator.class.getName()));
		logMap.put(Publisher.class.getName(), factory.getLogger(Publisher.class.getName()));
		logMap.put(SessionCreator.class.getName(), factory.getLogger(SessionCreator.class.getName()));
		defaultLog = factory.getLogger(Out.class.getName());

		Out.slf4j = true;
	}

	public static void usage(int exitCode, String err, Object... args) {
		i(err, args);
		System.out.println("");
		System.out.println("Usage: JavaBenchmarkSuite.jar [params]");
		System.out.println("     -logLevel [level] => sets default log level to given level (default: info)");
		System.out.println("     -slf4j [level] => uses slf4j logging at the given level ((default: false)");
		System.out.println("          Respected log levels are (from most to least verbose): ");
		System.out.println("               trace | debug | info | warn | error ");
		System.out.println("          You must use the -Dorg.slf4j.simpleLogger.defaultLogLevel=<level> to set the level ");
		System.out.println("     -publish [connectionString] <username> <password> => enables the publisher for");
		System.out.println("          the given full connection string (e.g. 'dpt://10.10.10.10:8080') with");
		System.out.println("          an optional username and password.");
		System.out.println("     -sessions [connectionString] [maxSessions] => enables creation of [maxSessions]");
		System.out.println("          Sessions (UnifiedAPI) for the given full connection string");
		System.out.println("          (e.g. 'dpt://10.10.10.10:8080' or 'ws://10.10.10.10:8080')");
		System.out.println("     -controlClients [connectionString] [maxControlClients] <username> <password> =>");
		System.out.println("          enables creation of [maxControlClients] ControlClients (UnifiedAPI) for");
		System.out.println("          the given full connection string (e.g. 'dpt://10.10.10.10:8080')");
		System.out.println("          each of these will publish the topics provided");
		System.out.println("     -topics [topic] <topics...> => subscribes any created Clients or Sessions to the");
		System.out.println("          given topic(s) in the format of: '<numberOfBytesPerMessage>/<messagesPerSecond>'");
		System.out.println("          NOTE: This application will automatically add the namespace and topic selector formatting!!!");
		System.out.println("          <topics...> => additional topics to subscribe to (these are optional,");
		System.out.println("          and as many as desired can be provided)");
		System.out.println("     -myTopics [topic] <topics...> => subscribes to user defined topics, REQUIRES the");
		System.out.println("          full topic path of EACH topic (can provide as many as you would like)");
		System.out.println("");
		exit(exitCode);
	}

	public static void exit(int exitCode) {
		d("Exiting with status: '%d'", exitCode);
		System.out.println("Press any key to exit...");
		try {
			(new BufferedReader(new InputStreamReader(System.in))).readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.exit(exitCode);
	}

	/**
	 * Manages all formatted logging output
	 * 
	 * @param level
	 *            - {@link OutLevel} the log statement comes from
	 * @param str
	 *            - log string to format
	 * @param args
	 *            -varArg params for formatting
	 */
	static void log(OutLevel level, String str, Object... args) {
		if (slf4j) {
			StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
			String clazz = null;
			for (int i = 1; i < stackTrace.length; i++) {
				StackTraceElement stackTraceElement = stackTrace[i];
				if (!stackTraceElement.getClassName().equals(Out.class.getName())) {
					clazz = stackTraceElement.getClassName();
					break;
				}
			}
			Logger logger = (clazz == null) ? defaultLog : logMap.getOrDefault(clazz, defaultLog);
			String logStr = String.format(str, args);
			switch (level) {
			case TRACE:
				logger.trace(logStr);
				break;
			case DEBUG:
				logger.debug(logStr);
				break;
			case WARN:
				logger.warn(logStr);
				break;
			case ERROR:
				logger.error(logStr);
				break;
			case INFO:
			default:
				logger.info(logStr);
				break;
			}
		} else if (doLog(level)) {
			String logStr = String.format(timeStr() + level.levelString() + str, args);
			switch (level) {
			case ERROR:
				System.err.println(logStr);
				break;
			default:
				System.out.println(logStr);
				break;
			}
		}
	}

	/**
	 * {@link OutLevel#ERROR} or SLF4J <code>error</code> level logging
	 * 
	 * @param str
	 *            - log string to format
	 * @param args
	 *            -varArg params for formatting
	 * 
	 * @see #log(OutLevel, String, Object...)
	 */
	public static void e(String str, Object... args) {
		log(OutLevel.ERROR, str, args);
	}

	/**
	 * {@link OutLevel#WARN} or SLF4J <code>warn</code> level logging
	 * 
	 * @param str
	 *            - log string to format
	 * @param args
	 *            -varArg params for formatting
	 * 
	 * @see #log(OutLevel, String, Object...)
	 */
	public static void w(String str, Object... args) {
		log(OutLevel.WARN, str, args);
	}

	/**
	 * {@link OutLevel#INFO} or SLF4J <code>info</code> level logging
	 * 
	 * @param str
	 *            - log string to format
	 * @param args
	 *            -varArg params for formatting
	 * 
	 * @see #log(OutLevel, String, Object...)
	 */
	public static void i(String str, Object... args) {
		log(OutLevel.INFO, str, args);
	}

	/**
	 * {@link OutLevel#DEBUG} or SLF4J <code>debug</code> level logging
	 * 
	 * @param str
	 *            - log string to format
	 * @param args
	 *            -varArg params for formatting
	 * 
	 * @see #log(OutLevel, String, Object...)
	 */
	public static void d(String str, Object... args) {
		log(OutLevel.DEBUG, str, args);
	}

	/**
	 * {@link OutLevel#TRACE} or SLF4J <code>trace</code> level logging
	 * 
	 * @param str
	 *            - log string to format
	 * @param args
	 *            -varArg params for formatting
	 * 
	 * @see #log(OutLevel, String, Object...)
	 */
	public static void t(String str, Object... args) {
		log(OutLevel.TRACE, str, args);
	}

	/**
	 * Used to create a date/time string for logging
	 * 
	 * @return formatted datetime
	 * 
	 * @see #DATETIME_FORMATTER
	 */
	static String timeStr() {
		return DATETIME_FORMATTER.print(DateTime.now()) + " ::: ";
	}

}
