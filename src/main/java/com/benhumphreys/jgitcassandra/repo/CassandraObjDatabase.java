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

public class CassandraObjDatabase extends DfsObjDatabase {

    public CassandraObjDatabase(DfsRepository repo) {
        super(repo, new DfsReaderOptions());
    }

    @Override
    protected void commitPackImpl(Collection<DfsPackDescription> desc,
            Collection<DfsPackDescription> replaces) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    protected List<DfsPackDescription> listPacks() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected DfsPackDescription newPack(PackSource source) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected ReadableChannel openFile(DfsPackDescription desc, PackExt ext)
            throws FileNotFoundException, IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected void rollbackPack(Collection<DfsPackDescription> desc) {
        // TODO Auto-generated method stub
    }

    @Override
    protected DfsOutputStream writeFile(DfsPackDescription desc, PackExt ext)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }
}
