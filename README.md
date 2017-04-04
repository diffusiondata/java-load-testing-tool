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

