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

import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsRepository;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.util.RefList;

import com.benhumphreys.jgitcassandra.Utils;
import com.benhumphreys.jgitcassandra.store.RefStore;
import com.benhumphreys.jgitcassandra.store.StoreConnection;

/**
 * A Cassandra backed implementation of the DfsRefDatabase
 */
class CassandraRefDatabase extends DfsRefDatabase {
    /**
     * RefStore object provides access to the Cassandra database
     */
    private final RefStore refs;

    /**
     * Constructor
     *
     * @param repository a reference to the repository this ref database
     *                   is associated with.
     */
    public CassandraRefDatabase(DfsRepository repository, StoreConnection conn)
            throws IOException {
        super(repository);
        refs = new RefStore(repository.getDescription().getRepositoryName(),
                conn.getSession());
    }

    /**
     * Compare a reference, and put if it matches.
     *
     * @param oldRef old value to compare to. If the reference is expected
     *               to not exist the old value has a storage of
     *               Ref.Storage.NEW and an ObjectId value of null.
     * @param newRef new reference to store.
     * @return true if the put was successful; false otherwise.
     * @throws IOException the reference cannot be put due to a system error.
     */
    @Override
    protected boolean compareAndPut(Ref oldRef, Ref newRef) throws IOException {
        String name = newRef.getName();
        if (oldRef == null || oldRef.getStorage() == Ref.Storage.NEW)
            return refs.putIfAbsent(name, newRef) == null;
        Ref cur = refs.get(name);
        if (cur != null && Utils.refsHaveEqualObjectId(cur, oldRef)) {
            return refs.replace(name, cur, newRef);
        } else {
            return false;
        }
    }

    /**
     * Compare a reference, and delete if it matches.
     *
     * @param oldRef the old reference information that was previously read.
     * @return true     if the remove was successful; false otherwise.
     * @throws IOException the reference could not be removed due to a system
     *                     error.
     */
    @Override
    protected boolean compareAndRemove(Ref oldRef) throws IOException {
        String name = oldRef.getName();
        Ref cur = refs.get(name);
        if (cur != null && Utils.refsHaveEqualObjectId(cur, oldRef)) {
            return refs.remove(name, cur);
        } else {
            return false;
        }
    }

    /**
     * Read all known references in the repository.
     *
     * @return all current references of the repository.
     * @throws IOException references cannot be accessed.
     */
    @Override
    protected RefCache scanAllRefs() throws IOException {
        RefList.Builder<Ref> ids = new RefList.Builder<Ref>();
        RefList.Builder<Ref> sym = new RefList.Builder<Ref>();
        for (Ref ref : refs.values()) {
            if (ref.isSymbolic()) {
                sym.add(ref);
            }
            ids.add(ref);
        }
        ids.sort();
        sym.sort();
        return new RefCache(ids.toRefList(), sym.toRefList());
    }
}
