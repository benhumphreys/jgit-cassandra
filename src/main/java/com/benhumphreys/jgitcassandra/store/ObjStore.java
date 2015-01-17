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
package com.benhumphreys.jgitcassandra.store;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.Bytes;
import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsPackDescription;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.pack.PackExt;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ObjStore {
    /**
     * Cassandra fetch size. This won't limit the size of a query result set, but
     * a large result set will be broken up into multiple fetches if the result set
     * size exceeds FETCH_SIZE
     */
    private static final int FETCH_SIZE = 100;

    /**
     * Pack description table name
     */
    private static final String DESC_TABLE_NAME = "pack_desc";

    /**
     * Pack data table name
     */
    private static final String DATA_TABLE_NAME = "pack_data";

    /**
     * The keyspace acts as a namespace
     */
    private final String keyspace;

    /**
     * A Cassandra session instance
     */
    private final Session session;

    /**
     * DfsRepositoryDescription is needed to instantiate new
     * DfsRepositoryDescription objects
     */
    private final DfsRepositoryDescription repoDesc;

    /**
     * Constructor
     *
     * @param keyspace the Cassandra keyspace
     * @param session  a Cassandra session instance
     * @throws NullPointerException if either of the parameters are null
     * @throws IOException          if an exception occurs when communicating to the
     *                              database
     */
    public ObjStore(String keyspace, Session session,
                    DfsRepositoryDescription repoDesc) throws IOException {
        if (keyspace == null || session == null) {
            throw new NullPointerException();
        }
        this.keyspace = keyspace;
        this.session = session;
        this.repoDesc = repoDesc;
        createSchemaIfNotExist();
    }

    /**
     * Inserts a Pack description into the store.
     * If a description for this "name" already exists it will be overwritten.
     *
     * @param desc  the pack description to insert
     * @throws IOException  if an exception occurs when communicating to the
     *                      database
     */
    public void insertDesc(Collection<DfsPackDescription> desc)
            throws IOException {

        try {
            for (DfsPackDescription pd : desc) {
                Statement stmt = QueryBuilder.insertInto(keyspace, DESC_TABLE_NAME)
                        .value("name", pd.toString())
                        .value("source", pd.getPackSource().ordinal())
                        .value("last_modified", pd.getLastModified())
                        .value("size_map", DescMapper.getFileSizeMap(pd))
                        .value("object_count", pd.getObjectCount())
                        .value("delta_count", pd.getDeltaCount())
                        .value("extensions", DescMapper.getExtBits(pd))
                        .value("index_version", pd.getIndexVersion());
                session.execute(stmt);
            }
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    /**
     * Removes the list of pack descriptions from the store.
     * If one of the descriptions is not present in the store it will be silently
     * ignored.
     *
     * A pack description is removed based on matching the "name" field; ie the
     * string returned by DfsPackDescrption.toString()
     *
     * @param desc
     * @throws IOException  if an exception occurs when communicating to the
     *                      database
     */
    public void removeDesc(Collection<DfsPackDescription> desc)
            throws IOException {
        for (DfsPackDescription pd : desc) {
            try {
                Statement stmt = QueryBuilder.delete()
                        .from(keyspace, DESC_TABLE_NAME)
                        .where(QueryBuilder.eq("name", pd.toString()));
                session.execute(stmt);
            } catch (RuntimeException e) {
                e.printStackTrace();
                throw new IOException(e);
            }
        }
    }

    /**
     * @return a list of all pack descriptions in the store.
     * @throws IOException  if an exception occurs when communicating to the
     *                      database
     */
    public List<DfsPackDescription> listPacks() throws IOException {
        Statement stmt = QueryBuilder
                .select()
                .all()
                .from(keyspace, DESC_TABLE_NAME);
        stmt.setFetchSize(FETCH_SIZE);
        ResultSet results = session.execute(stmt);
        List<DfsPackDescription> packs = new ArrayList<DfsPackDescription>();
        for (Row row : results) {
            packs.add(rowToPackDescription(row));
        }
        return packs;
    }

    /**
     * Returns a ByteBuffer with the contents of the file given by the pair
     * "desc" and "ext".
     *
     * @throws IOException  if an exception occurs when communicating to the
     *                      database
     */
    public ByteBuffer readFile(DfsPackDescription desc, PackExt ext)
            throws IOException {
        try {
            Statement stmt = QueryBuilder
                    .select()
                    .all()
                    .from(keyspace, DATA_TABLE_NAME)
                    .where(QueryBuilder.eq("name", desc.getFileName(ext)));
            ResultSet results = session.execute(stmt);
            Row r = results.one();
            if (!results.isExhausted()) {
                throw new IllegalStateException("Multiple rows for a single file: "
                        + desc.getFileName(ext));
            }
            return r.getBytes("data");
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    /**
     * Overwrites the file given by the pair "desc" and "ext" witht the data in
     * the "data" ByteArray.
     *
     * @throws IOException if an exception occurs when communicating to the
     *                     database
     */
    public void writeFile(DfsPackDescription desc, PackExt ext,
                          ByteBuffer data) throws IOException {
        try {
            Statement stmt = QueryBuilder.insertInto(keyspace, DATA_TABLE_NAME)
                    .value("name", desc.getFileName(ext))
                    .value("data", Bytes.toHexString(data));
            session.execute(stmt);
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    /**
     * Creates the Cassandra keyspace and pack tables if it does
     * not already exist.
     *
     * @throws IOException if an exception occurs when communicating to the
     *                     database
     */
    private void createSchemaIfNotExist() throws IOException {
        try {
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace
                    + " WITH replication = {'class':'SimpleStrategy',"
                    + " 'replication_factor':1};");

            session.execute("CREATE TABLE IF NOT EXISTS "
                    + keyspace + "." + DESC_TABLE_NAME
                    + " (name varchar PRIMARY KEY, source int, "
                    + "last_modified bigint, size_map map<text, bigint>, "
                    + "object_count bigint, delta_count bigint, "
                    + "extensions int, index_version int);");


            session.execute("CREATE TABLE IF NOT EXISTS "
                    + keyspace + "." + DATA_TABLE_NAME
                    + " (name varchar PRIMARY KEY, data blob);");
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    /**
     * Converts a row to a DfsPackDescription
     */
    private DfsPackDescription rowToPackDescription(Row row) {
        final String name = row.getString("name");
        final int source = row.getInt("source");
        final long lastModified = row.getLong("last_modified");
        final Map<String, Long> sizeMap = row.getMap("size_map", String.class, Long.class);
        final long objectCount = row.getLong("object_count");
        final long deltaCount = row.getLong("delta_count");
        final int extensions = row.getInt("extensions");
        final int indexVersion = row.getInt("index_version");

        DfsPackDescription desc = new DfsPackDescription(
                repoDesc,
                name);

        desc.setPackSource(DfsObjDatabase.PackSource.values()[source]);
        desc.setLastModified(lastModified);
        DescMapper.setFileSizeMap(desc, sizeMap);
        desc.setObjectCount(objectCount);
        desc.setDeltaCount(deltaCount);
        DescMapper.setExtsFromBits(desc, extensions);
        desc.setIndexVersion(indexVersion);

        return desc;
    }
}