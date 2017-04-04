# java-load-testing-tool
Java Load Testing Tool for Diffusion

## Building

Requirements

* Oracle JDK v.8
* Maven v3.3


    mvn clean install

## As a publisher

    java -jar target/benchmark-0.10.0-script-SNAPSHOT.jar -publish ${server-url} ${user} ${password} -topics some/topic/${message_size}/${updates_per_second}

For example:

    java -jar target/benchmark-0.10.0-script-SNAPSHOT.jar -publish ws://localhost:8080 admin password -topics foo/bar/100/5 foo/bar/100/2

Will create topic `foo/bar/100/5` with 100 bytes of string data and update it five times a second. Topic `foo/bar/100/2` is created with 100 bytes and updated twice a second. 

## Execution

This tool is an executable Java jar file.

It can act as 
- a topic updater control client
- a simulator of multiple clients subscribing to topics

`clientloader.sh` generates subscribing clients connecting at a certain rate, subscribing to specific topics, then disconnecting after a number of seconds.

Edit the test and environment parameters at the top of the clientloader.sh script and run it as:

    ./clientloader.sh start
    ./clientloader.sh stop

On running, you should see the following output to confirm the test scenario:

    ========================
    Starting Client Loader..
    Connecting to:  ws://localhost:8080
    Subscribing to:  ?dr/json
    Ramping up to a steady state of 4500 sessions
    Ramp up rate: 15 clients per second
    Clients disconnect after: 300 seconds
    ========================
    Press ENTER to continue...(or Ctl-C to stop the test now)
 

The script is designed to ramp up new connections at a configured rate, then disconnect after a configuration time period.
The total number of (steady state) concurrent connections will be `SESSION_RATE x SESSION_DURATION`.
 
Output is written to a log file that will look like `clientloader20170116-110445.log`
 
Depending on the environment in which you are running you may also need to increase some OS settings like the maximum file descriptors that can be opened.

You can run multiple instances of the tool to simulate peak, or bursty loads.
 
Note that currently, the stop command will stop all instances of the tool running on a server. To increase and decrease the load, please run different instances on different servers.
This will allow, for example, a background load to be generated connecting at 20 connections/sec for 2 minutes, then add a burst of an additional 40 connections/sec for 2 minutes, then stop the additional load.
 
Monitor CPU and memory usage on the server(s) running the client loader to ensure they do not become the bottle neck.
For instance `top` on Linux is a tool to check the load average – if the load average spikes you may experience disconnections.

During the test run, we advise recording the behaviour of your Diffusion system by:
- Saving log files including garbage collection logs
- Running the Java Flight Recorder / Java Mission Control (part of the Java Development Kit) and save the recordings for analysis
- Running monitoring system(s) connected to Diffusion via JMX
- Running Linux or Windows Server monitoring tools
