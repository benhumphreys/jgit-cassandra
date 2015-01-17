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

import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.transport.DaemonClient;
import org.eclipse.jgit.transport.ServiceMayNotContinueException;
import org.eclipse.jgit.transport.resolver.RepositoryResolver;
import org.eclipse.jgit.transport.resolver.ServiceNotAuthorizedException;
import org.eclipse.jgit.transport.resolver.ServiceNotEnabledException;

/**
 * Custom implementation of a RepositoryResolver for InMemory repositories.
 * This implementation resolves InMemoryRepositories, creating a new one if
 * the repository identified by by "name" does not exist.
 */
final class InMemoryRepositoryResolver implements
        RepositoryResolver<DaemonClient> {

    /**
     * Maps repository names to repository instances
     */
    private final static Map<String, InMemoryRepository> repositories =
            new HashMap<String, InMemoryRepository>();

    @Override
    public Repository open(DaemonClient client, String name)
            throws RepositoryNotFoundException,
            ServiceNotAuthorizedException, ServiceNotEnabledException,
            ServiceMayNotContinueException {
        InMemoryRepository repo = repositories.get(name);
        if (repo == null) {
            repo = new InMemoryRepository(
                    new DfsRepositoryDescription(name));
            repositories.put(name, repo);
        }
        return repo;
    }
}