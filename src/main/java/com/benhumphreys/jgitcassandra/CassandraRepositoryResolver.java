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

/**
 * Custom implementation of a RepositoryResolver for Cassandra based
 * repositories.
 */
final class CassandraRepositoryResolver implements
        RepositoryResolver<DaemonClient> {
    
    /**
     * Maps repository names to repository instances
     */
    private static Map<String, CassandraRepository> repositories =
            new HashMap<String, CassandraRepository>();

    @Override
    public Repository open(DaemonClient client, String name)
            throws RepositoryNotFoundException,
            ServiceNotAuthorizedException, ServiceNotEnabledException,
            ServiceMayNotContinueException {
        CassandraRepository repo = repositories.get(name);
        if (repo == null) {
            repo = new CassandraRepository(
                    new DfsRepositoryDescription(name));
            repositories.put(name, repo);
        }
        return repo;
    }
}