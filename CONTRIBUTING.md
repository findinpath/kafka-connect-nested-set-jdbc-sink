Contributing to JDBC Nested Set Sink Connector
==============================================


## Install the tools

The following software is required to work with the JDBC Nested Set Sink Connector codebase and build it locally:

* [Git 2.2.1](https://git-scm.com) or later
* [JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) or [OpenJDK 8](http://openjdk.java.net/projects/jdk8/)
* [Maven 3.6.0](https://maven.apache.org/index.html) or later
* [Docker Engine 19](http://docs.docker.com/engine/installation/) or later



## GitHub account

This project uses [GitHub](GitHub.com) for its primary code repository and for pull-requests, so if you don't already have a GitHub account you'll need to [join](https://github.com/join).


## Building locally


To build the source code locally, checkout and update the `master` branch:


    $ git checkout master
    $ git pull
    
    
Then use Maven to compile everything, run all unit and integration tests, build all artifacts, and install the JAR artifact into your local Maven repository:

    $ mvn clean install
    
If you want to skip the tests (e.g., if you don't have Docker installed), you can add `-DskipTests` to that command:

    $ mvn clean install -DskipTests

### Code Formatting

This project utilizes the IntelliJ IDEA Java default code style.


### Continuous Integration

The project currently builds its jobs in Travis CI environment:

https://travis-ci.com/github/findinpath/kafka-connect-nested-set-jdbc-sink