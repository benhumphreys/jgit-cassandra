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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private final StoreConnection storeconn;

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
            } catch (Exception e) {
                throw new ServiceMayNotContinueException(e);
            }
            repositories.put(name, repo);
        }
        return repo;
    }

    /**
     * Trims the ".git" from the end of the name and sanitises.
     * <p/>
     * Since the name forms the keyspace, and is input by the user, we need to
     * ensure the name is sanitised. Currently only support alpha-numeric, plus
     * hyphen and underscore characters in names.
     *
     * @param name
     * @return
     * @throws IllegalArgumentException
     */
    static String sanitiseName(String name) throws IllegalArgumentException {
        String str = name.toLowerCase().trim();
        int idx = str.indexOf(".git");
        str = (idx >= 0) ? str.substring(0, idx) : str;

        Pattern p = Pattern.compile("^[a-zA-Z0-9-_]+$");
        Matcher m = p.matcher(str);
        if (m.matches()) {
            return str;
        } else {
            throw new IllegalArgumentException("Invalid name: " + name);
        }
    }
}