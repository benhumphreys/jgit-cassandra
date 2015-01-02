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
import java.util.HashMap;
import java.util.Map;

import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.transport.DaemonClient;
import org.eclipse.jgit.transport.ServiceMayNotContinueException;
import org.eclipse.jgit.transport.resolver.RepositoryResolver;
import org.eclipse.jgit.transport.resolver.ServiceNotAuthorizedException;
import org.eclipse.jgit.transport.resolver.ServiceNotEnabledException;

import com.benhumphreys.jgitcassandra.repo.CassandraRepository;
import com.benhumphreys.jgitcassandra.store.StoreConnection;

/**
 * Custom implementation of a RepositoryResolver for Cassandra based
 * repositories.
 */
final class CassandraRepositoryResolver implements
        RepositoryResolver<DaemonClient> {
    StoreConnection storeconn;
    
    /**
     * Maps repository names to repository instances
     */
    private final static Map<String, CassandraRepository> repositories =
            new HashMap<String, CassandraRepository>();
    
    public CassandraRepositoryResolver(StoreConnection conn) {
        storeconn = conn;
    }

    @Override
    public Repository open(DaemonClient client, String name)
            throws RepositoryNotFoundException,
            ServiceNotAuthorizedException, ServiceNotEnabledException,
            ServiceMayNotContinueException {
        CassandraRepository repo = repositories.get(name);
        if (repo == null) {
            try {
            repo = new CassandraRepository(
                    new DfsRepositoryDescription(sanitiseName(name)),
                    storeconn);
            } catch (IOException e) {
                throw new ServiceMayNotContinueException(e);
            }
            repositories.put(name, repo);
        }
        return repo;
    }
    
    static String sanitiseName(String name) {
        String s = name.toLowerCase();
        int idx = s.indexOf(".git");
        if (idx >= 0) {
            return s.substring(0, idx).trim();
        } else {
            return s.trim();
        }
                
    }
}