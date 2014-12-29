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
package com.benhumphreys.jgitcassandra.repo;

import java.io.IOException;

import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsRepository;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryBuilder;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;

/**
 *
 */
public class CassandraRepository extends DfsRepository {
    private final DfsObjDatabase objdb;

    private final DfsRefDatabase refdb;

    /**
     * @param repoDesc
     */
    @SuppressWarnings("rawtypes")
    public CassandraRepository(DfsRepositoryDescription repoDesc) {
        super(new DfsRepositoryBuilder<DfsRepositoryBuilder, CassandraRepository>() {
            @Override
            public CassandraRepository build() throws IOException {
                throw new UnsupportedOperationException();
            }
        }.setRepositoryDescription(repoDesc));

        objdb = new CassandraObjDatabase(this);
        refdb = new CassandraRefDatabase(this);
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
