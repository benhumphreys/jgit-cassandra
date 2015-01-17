Cassandra Backend For JGit
==========================

**NOTE:** This is not production ready software. This is currently no more than
an experiment for the purposes of learning some Git internals and to understand
how to develop backends for JGit.

*To build as a libary to use in your own program:*

    git clone <repository>
    cd jgit-cassandra
    mvn package

The created JAR file can be found in the directory "target".

*To build and run the simple test server:*

    mvn assembly:assembly
    java -jar ./target/jgit-cassandra-0.0.1-SNAPSHOT-jar-with-dependencies.jar <NODE> [NODE....]

The hostname or IP address of one or more Cassandra nodes must be passed on the
command line.
