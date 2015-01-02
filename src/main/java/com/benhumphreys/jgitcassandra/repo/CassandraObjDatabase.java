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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsOutputStream;
import org.eclipse.jgit.internal.storage.dfs.DfsPackDescription;
import org.eclipse.jgit.internal.storage.dfs.DfsReaderOptions;
import org.eclipse.jgit.internal.storage.dfs.DfsRepository;
import org.eclipse.jgit.internal.storage.dfs.ReadableChannel;
import org.eclipse.jgit.internal.storage.pack.PackExt;

import com.benhumphreys.jgitcassandra.store.StoreConnection;

class CassandraObjDatabase extends DfsObjDatabase {

    public CassandraObjDatabase(DfsRepository repo, StoreConnection conn) {
        super(repo, new DfsReaderOptions());
    }

    @Override
    protected void commitPackImpl(Collection<DfsPackDescription> desc,
            Collection<DfsPackDescription> replaces) throws IOException {
        // TODO Auto-generated method stub
        System.err.println("commitPackImpl not implemented");
    }

    @Override
    protected List<DfsPackDescription> listPacks() throws IOException {
        // TODO Auto-generated method stub
        System.err.println("listPacks not implemented");
        return null;
    }

    @Override
    protected DfsPackDescription newPack(PackSource source) throws IOException {
        // TODO Auto-generated method stub
        System.err.println("newPack not implemented");
        return null;
    }

    @Override
    protected ReadableChannel openFile(DfsPackDescription desc, PackExt ext)
            throws FileNotFoundException, IOException {
        // TODO Auto-generated method stub
        System.err.println("openFile not implemented");
        return null;
    }

    @Override
    protected void rollbackPack(Collection<DfsPackDescription> desc) {
        // TODO Auto-generated method stub
        System.err.println("rollbackPack not implemented");
    }

    @Override
    protected DfsOutputStream writeFile(DfsPackDescription desc, PackExt ext)
            throws IOException {
        // TODO Auto-generated method stub
        System.err.println("writeFile not implemented");
        return null;
    }
}
