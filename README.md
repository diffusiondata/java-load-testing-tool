# java-load-testing-tool
Java Load Testing Tool for Diffusion

## Building

Requirements

* Oracle JDK v.8
* Maven v3.3
* Diffusion v5.9
* Internet access

```
mvn clean install
```

## As a publisher

    java -jar target/benchmark-1.0.0.jar -publish ${server-url} ${user} ${password} -topics some/topic/${message_size}/${updates_per_second}

For example:

    java -jar target/benchmark-1.0.0.jar -publish ws://localhost:8080 admin password -topics foo/bar/100/5 foo/bar/100/2

Will create topic `foo/bar/100/5` with 100 bytes of string data and update it five times a second. Topic `foo/bar/100/2` is created with 100 bytes and updated twice a second. Topic paths not matching this pattern will provoke an error `Could not parse topicPath`

## As a subscriber

Placing a finite number of subscribing sessions

    java -jar target/benchmark-0.10.0-script-SNAPSHOT.jar -sessions ${server-url} 2 -myTopics ${topics}

For example:

    java -jar target/benchmark-0.10.0-script-SNAPSHOT.jar -sessions ws://localhost:8080 2 -myTopics foo/bar/100/1 foo/bar/100/2
    
Frequently placing and closing (or 'churning') a given number of sessions a second:

    java -jar target/benchmark-0.10.0-script-SNAPSHOT.jar -myTopics foo/bar/100/1 /foo/bar/100/2 -sessionRate ws://localhost:8080 1 500 

[![Build Status](https://travis-ci.org/pushtechnology/java-load-testing-tool.svg?branch=master)](https://travis-ci.org/pushtechnology/java-load-testing-tool)