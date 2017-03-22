# java-load-testing-tool
Java Load Testing Tool for Diffusion


This tool is an executable Java jar file.

It can act as 
- a topic updater control client
- a simulator of multiple clients subscribing to topics

The ***clientloader.sh*** script only generates subscribing clients, connecting at a certain rate, subscribing to specific topics, then disconnecting after a number of seconds.

You just need to edit the test and environment parameters and the top of the clientloader.sh script
and run it as:


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
 

The script is designed to ramp up new connections at a specified rate, then disconnect after a specified time period (both configurable).
The total number of (steady state) concurrent connections will be SESSION_RATE x SESSION_DURATION.
 
Output is written to a log file that will look like:

    clientloader20170116-110445.log
 
Depending on the OS environment on which you are running you may also need to increase some OS settings like the maximum file handles that can be opened.
Also, you can run multiple instances of the tool to simulate peak, or bursty loads.
 
Note that currently, the stop command will stop all instances of the tool running on a server. To increase and decrease the load, please run different instances on different servers.
This will allow, for example, a background load to be generated connecting at 20 connections/sec for 2 minutes, then add a burst of an additional 40 connections/sec for 2 minutes, then stop the additional load.
 
It’s worth keeping an eye on CPU and memory usage on the server(s) running the client loader to ensure they do not become the bottle neck.
For instance ***top*** on Linux is one of tools to check the load averages – if the load average goes to high you may experience disconnections.
During the test run, we advise recording the behaviour of your Diffusion system by:
- Saving log files including garbage collection logs
- Running the Java Flight Recorder / Java Mission Control (part of the Java Development Kit) and save the recordings for analysis
- Running monitoring system(s) connected to Diffusion via JMX
- Running Linux or Windows Server monitoring tools
