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

import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsRepository;
import org.eclipse.jgit.lib.Ref;

public class CassandraRefDatabase extends DfsRefDatabase {

    public CassandraRefDatabase(DfsRepository repository) {
        super(repository);
        // TODO Auto-generated constructor stub
    }

    @Override
    protected boolean compareAndPut(Ref oldRef, Ref newRef) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    protected boolean compareAndRemove(Ref oldRef) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    protected RefCache scanAllRefs() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }
}
