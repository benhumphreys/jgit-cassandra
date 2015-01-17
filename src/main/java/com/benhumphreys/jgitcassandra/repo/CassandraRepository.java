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
package com.benhumphreys.jgitcassandra.repo;

import java.io.IOException;

import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsRepository;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryBuilder;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;

import com.benhumphreys.jgitcassandra.store.StoreConnection;

/**
 * A DfsRepository implemented with a Cassandra database store.
 */
public class CassandraRepository extends DfsRepository {
    private final DfsObjDatabase objdb;

    private final DfsRefDatabase refdb;

    /**
     * Creating a new repository object may result in creating a new repository
     * in the storage layer, or if the repository identified by "repoDesc" already
     * exists, it will be used instead.
     *
     * @param repoDesc  description of the repository that this object will
     *                  provide access to.
     */
    @SuppressWarnings("rawtypes")
    public CassandraRepository(DfsRepositoryDescription repoDesc, StoreConnection conn)
            throws IOException {
        super(new DfsRepositoryBuilder<DfsRepositoryBuilder, CassandraRepository>() {
            @Override
            public CassandraRepository build() throws IOException {
                throw new UnsupportedOperationException();
            }
        }.setRepositoryDescription(repoDesc));

        objdb = new CassandraObjDatabase(this, conn);
        refdb = new CassandraRefDatabase(this, conn);
    }

    @Override
    public DfsObjDatabase getObjectDatabase() {
        return objdb;
    }

    @Override
    public DfsRefDatabase getRefDatabase() {
        return refdb;
    }
}
