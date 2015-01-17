/*
 * A Cassandra backend for JGit
 * Copyright 2014 Ben Humphreys
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.benhumphreys.jgitcassandra.store;

import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

/**
 * Encapsulates the connection to the Cassandra cluster, specifically a single
 * session object.
 */
public class StoreConnection {
    private final Cluster cluster;
    private final Session session;

    /**
     * Constructor
     *
     * @param nodes a list of one or more Cassandra nodes to connect to. Note
     *              that not all Cassandra nodes in the cluster need be
     *              supplied; one will suffice however if that node is
     *              unavailable the connection attempt will fail, even if the
     *              others are available.
     */
    public StoreConnection(List<String> nodes) {
        Cluster.Builder builder = Cluster.builder();
        for (String node : nodes) {
            builder.addContactPoint(node);
        }
        cluster = builder.build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s%n",
                metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Datacenter: %s; Host: %s; Rack: %s%n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
    }

    /**
     * Must be called to cleanup the connection
     */
    public void close() {
        session.close();
        cluster.close();
    }

    /**
     * Returns the session object associated with this connection.
     */
    public Session getSession() {
        return session;
    }
}
