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
package com.benhumphreys.jgitcassandra;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;

import org.eclipse.jgit.transport.Daemon;

import com.benhumphreys.jgitcassandra.store.StoreConnection;

/**
 * A simple server that provides "git" protocol access to the repositories.
 */
public class Server {

    public static void main(String[] args) {
        // Create the Cassandra Store Connection
        if (args.length < 1) {
            System.err.println("Must specify one or more Cassandra nodes");
            return;
        }
        StoreConnection conn = new StoreConnection(Arrays.asList(args));

        // Start the Git server
        Daemon server = new Daemon(new InetSocketAddress(9418));
        boolean uploadsEnabled = true;
        server.getService("git-receive-pack").setEnabled(uploadsEnabled);
        //server.setRepositoryResolver(new InMemoryRepositoryResolver()); // For testing
        server.setRepositoryResolver(new CassandraRepositoryResolver(conn));
        try {
            server.start();
        } catch (IOException e) {
            System.out.println("Failed to start server: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
