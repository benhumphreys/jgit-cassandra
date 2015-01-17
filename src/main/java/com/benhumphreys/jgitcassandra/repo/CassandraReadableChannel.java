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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.eclipse.jgit.internal.storage.dfs.ReadableChannel;

public class CassandraReadableChannel implements ReadableChannel {
    private final byte[] data;

    private int position = 0;

    private boolean open = true;

    /**
     * @see org.eclipse.jgit.internal.storage.dfs.ReadableChannel
     */
    public CassandraReadableChannel(ByteBuffer bytes) {
        data = new byte[bytes.remaining()];
        bytes.get(data);
    }

    /**
     * @see org.eclipse.jgit.internal.storage.dfs.ReadableChannel
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        int n = Math.min(dst.remaining(), data.length - position);
        if (n == 0) {
            return -1;
        }
        dst.put(data, position, n);
        position += n;
        return n;
    }


    /**
     * @see org.eclipse.jgit.internal.storage.dfs.ReadableChannel
     */
    @Override
    public void close() throws IOException {
        open = false;
    }


    /**
     * @see org.eclipse.jgit.internal.storage.dfs.ReadableChannel
     */
    @Override
    public boolean isOpen() {
        return open;
    }


    /**
     * @see org.eclipse.jgit.internal.storage.dfs.ReadableChannel
     */
    @Override
    public int blockSize() {
        return 0;
    }


    /**
     * @see org.eclipse.jgit.internal.storage.dfs.ReadableChannel
     */
    @Override
    public long position() throws IOException {
        return position;
    }


    /**
     * @see org.eclipse.jgit.internal.storage.dfs.ReadableChannel
     */
    @Override
    public void position(long newPosition) throws IOException {
        position = (int)newPosition;
    }

    /**
     * @see org.eclipse.jgit.internal.storage.dfs.ReadableChannel
     */
    @Override
    public long size() throws IOException {
        return data.length;
    }
}
