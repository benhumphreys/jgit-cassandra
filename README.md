Cassandra Backend For JGit
==========================

**NOTE:** This is not production ready software. This is currently no more than
an experiment for the purposes of learning some Git internals and to understand
how to develop backends for JGit.

To build and run the server:
    git clone <repository>
    cd jgit-cassandra
    mvn assembly:assembly
    java -jar ./target/jgit-cassandra-0.0.1-SNAPSHOT-jar-with-dependencies.jar
