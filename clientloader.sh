#!/bin/bash        
#=================================
# Test Parameters
#=================================
CONNECT_STR="ws://localhost:8080"
SESSION_RATE=5
SESSION_DURATION=80
MY_TOPICS="?dr/json"
MY_TOPICS_TYPE=JSON
#=================================

#=================================
# Environment Parameters
#=================================
CONNECT_STR="ws://localhost:8080"
LOADTESTER_PATH=./
LOADTESTER_JAR=benchmark-0.10.0-script-SNAPSHOT.jar
LOGFILE=clientloader`date "+%Y%m%d-%H%M%S"`.log
CLASSPATH=$CLASSPATH:/Developer/Diffusion5.9.3/Diffusion5.9.3_01/clients/java/diffusion-client-5.9.3.jar:.
DIFFUSION_HOME=/Developer/Diffusion5.9.3/Diffusion5.9.3_01
#=================================


          if [ -z "$1" ]; then 
              echo "usage: $0 start|stop"
              exit
          elif [ "$1" = "start" ]; then
            echo "========================"
            echo "Starting Client Loader.."
            MAX_SESSIONS=$[SESSION_RATE * SESSION_DURATION]
            echo "Connecting to: " $CONNECT_STR
            echo "Subscribing to: " $MY_TOPICS
            echo "Ramping up to a steady state of" $MAX_SESSIONS "sessions"
            echo "Ramp up rate:" $SESSION_RATE "clients per second"
            echo "Clients disconnect after:" $SESSION_DURATION "seconds"
            echo "========================"
            echo "Press ENTER to continue...(or Ctl-C to stop the test now)"
            read CONT

             java -Xms512m -Xmx512m -Dbench.input.buffer.size=65535 \
             -Dbench.output.buffer.size=65535 -Dorg.slf4j.simpleLogger.defaultLogLevel=debug \
             -jar $LOADTESTER_PATH$LOADTESTER_JAR -sessionsRate $CONNECT_STR $SESSION_RATE $SESSION_DURATION -myTopics $MY_TOPICS -topicType JSON > $LOGFILE &
          elif [ "$1" = "stop" ]; then
             echo "Stopping Client Loader.."
             for pid in $(ps -ef | grep $LOADTESTER_PATH$LOADTESTER_JAR | grep -v grep | awk '{print $2}'); do kill -9 $pid; done
             exit
          else
              echo "usage: $0 start|stop"
          fi
tail -f $LOGFILE
