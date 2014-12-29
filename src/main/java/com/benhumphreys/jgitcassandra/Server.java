/*
 * A Cassandra backend for JGit
 * Copyright (C) 2014  Ben Humphreys
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */
package com.benhumphreys.jgitcassandra;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.eclipse.jgit.transport.Daemon;

/**
 * A simple server that provides "git" protocol access to the repositories.
 */
public class Server {

    public static void main(String[] args) {
        Daemon server = new Daemon(new InetSocketAddress(9418));
        boolean uploadsEnabled = true;
        server.getService("git-receive-pack").setEnabled(uploadsEnabled);
        //server.setRepositoryResolver(new InMemoryRepositoryResolver()); // For testing
        server.setRepositoryResolver(new CassandraRepositoryResolver());
        try {
            server.start();
        } catch (IOException e) {
            System.out.println("Failed to start server: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
