/*
 * A Cassandra backend for JGit
 * Copyright 2015 Ben Humphreys
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.eclipse.jgit.internal.storage.dfs.DfsOutputStream;
import org.eclipse.jgit.internal.storage.dfs.DfsPackDescription;
import org.eclipse.jgit.internal.storage.pack.PackExt;

import com.benhumphreys.jgitcassandra.store.ObjStore;

/**
 * Output stream is used to write data into a file in the Cassandra store.
 */
public class CassandraOutputStream extends DfsOutputStream {

    private final ObjStore store;

    private final DfsPackDescription desc;

    private final PackExt ext;

    private final ByteArrayOutputStream dst = new ByteArrayOutputStream();

    private byte[] data;

    public CassandraOutputStream(ObjStore store, DfsPackDescription desc,
                                 PackExt ext) {
        this.store = store;
        this.desc = desc;
        this.ext = ext;
    }

    @Override
    public void write(byte[] buf, int off, int len) throws IOException {
        data = null;
        dst.write(buf, off, len);
    }

    @Override
    public int read(long position, ByteBuffer buf) {
        byte[] d = getData();
        int n = Math.min(buf.remaining(), d.length - (int) position);
        if (n == 0) {
            return -1;
        }
        buf.put(d, (int) position, n);
        return n;
    }

    @Override
    public void flush() throws IOException {
        getData();
        store.writeFile(desc, ext, ByteBuffer.wrap(data));
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private byte[] getData() {
        if (data == null) {
            data = dst.toByteArray();
        }
        return data;
    }
}
