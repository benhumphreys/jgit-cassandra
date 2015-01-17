/*
 * A Cassandra backend for JGit
 * Copyright 2014-2015 Ben Humphreys
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectIdRef;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.SymbolicRef;

import com.benhumphreys.jgitcassandra.Utils;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Provides access to the Ref store.
 * <p/>
 * This class provides map (i.e. key/value) semantics, mapping a "name" to
 * a Ref. The map exists within a namespace identified by the "keyspace".
 * Key/value pairs are distinct within a keyspace.
 */
public class RefStore {
    /**
     * Cassandra fetch size. This won't limit the size of a query result set, but
     * a large result set will be broken up into multiple fetches if the result set
     * size exceeds FETCH_SIZE
     */
    private static final int FETCH_SIZE = 100;

    /**
     * Refs table name
     */
    private static final String TABLE_NAME = "refs";

    /**
     * The keyspace acts as a namespace
     */
    private final String keyspace;

    /**
     * A Cassandra session instance
     */
    private final Session session;

    /**
     * Constructor
     *
     * @param keyspace the Cassandra keyspace
     * @param session  a Cassandra session instance
     * @throws NullPointerException if either of the parameters are null
     * @throws IOException          if an exception occurs when communicating to the
     *                              database
     */
    public RefStore(String keyspace, Session session) throws IOException {
        if (keyspace == null || session == null) {
            throw new NullPointerException();
        }
        this.keyspace = keyspace;
        this.session = session;
        createSchemaIfNotExist();
    }

    /**
     * Returns the Ref to which the specified name is mapped
     *
     * @param name the name whose associated value is to be returned
     * @return the Ref to which the specified name is mapped, or null if
     * the store contains no mapping for the name
     * @throws IOException if an exception occurs when communicating to the
     *                     database
     */
    public Ref get(String name) throws IOException {
        try {
            Statement stmt = QueryBuilder
                    .select()
                    .all()
                    .from(keyspace, TABLE_NAME)
                    .where(QueryBuilder.eq("name", name));
            ResultSet results = session.execute(stmt);
            Ref r = rowToRef(results.one());
            if (!results.isExhausted()) {
                throw new IllegalStateException("Multiple rows for a single ref: "
                        + name);
            }
            return r;
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    /**
     * @return a Collection view of all refs in the store
     * @throws IOException if an exception occurs when communicating to the
     *                     database
     */
    public Collection<Ref> values() throws IOException {
        try {
            List<Ref> refs = new ArrayList<Ref>();
            Statement stmt = QueryBuilder
                    .select()
                    .all()
                    .from(keyspace, TABLE_NAME);
            stmt.setFetchSize(FETCH_SIZE);
            ResultSet results = session.execute(stmt);
            for (Row row : results) {
                refs.add(rowToRef(row));
            }
            return refs;
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    /**
     * If the specified "name" is not already associated with a "Ref",
     * associate it with the given "Ref".
     *
     * @param name   the name (i.e. key) with which the specified value is
     *               to be associated
     * @param newRef the Ref to be associated with the specified name
     * @return the previous Ref associated with the specified name, or null if
     * there was no mapping for the name.
     * @throws IOException if an exception occurs when communicating to the
     *                     database
     */
    public Ref putIfAbsent(String name, Ref newRef) throws IOException {
        final Ref cur = get(name);
        if (cur == null) {
            putRef(name, newRef);
        }
        return cur;
    }

    /**
     * Replaces the entry for a name only if currently mapped to a given Ref.
     *
     * @param name   name  which the specified value is associated
     * @param cur    Ref expected to be currently associated with the
     *               specified key
     * @param newRef Ref to be associated with the specified key
     * @return true if the value was replaced
     * @throws IOException if an exception occurs when communicating to the
     *                     database
     */
    public boolean replace(String name, Ref cur, Ref newRef) throws IOException {
        final Ref curInStore = get(name);
        if (curInStore != null && Utils.refsHaveEqualObjectId(curInStore, cur)) {
            putRef(name, newRef);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Removes the entry for a name only if currently mapped to a given Ref
     *
     * @param name name with which the specified value is associated
     * @param cur  Ref expected to be associated with the specified key
     * @return true if the value was removed
     * @throws IOException if an exception occurs when communicating to the
     *                     database
     */
    public boolean remove(String name, Ref cur) throws IOException {
        final Ref curInStore = get(name);
        if (curInStore != null && Utils.refsHaveEqualObjectId(curInStore, cur)) {
            removeRef(name);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Creates the Cassandra keyspace and refs table if it does
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
                    + keyspace + "." + TABLE_NAME
                    + " (name varchar PRIMARY KEY, type int, value varchar, "
                    + "aux_value varchar);");
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    /**
     * Parses a Cassandra refs table row and converts it to a Ref
     *
     * @param row a single Cassandra row to parse
     * @return a ref, or null if the "row" parameter is null
     * @throws IOException           if an exception occurs when communicating to the
     *                               database
     * @throws IllegalStateException if the "type" field read back from the
     *                               database is not one of the four handled
     *                               types (@see RefType).
     */
    private Ref rowToRef(Row row) throws IOException {
        if (row == null) {
            return null;
        }

        final String name = row.getString("name");
        final String value = row.getString("value");
        final int refType = row.getInt("type");

        if (refType == RefType.PEELED_NONTAG.getValue()) {
            return new ObjectIdRef.PeeledNonTag(Ref.Storage.NETWORK, name,
                    ObjectId.fromString(value));
        } else if (refType == RefType.PEELED_TAG.getValue()) {
            final String auxValue = row.getString("aux_value");
            return new ObjectIdRef.PeeledTag(Ref.Storage.NETWORK, name,
                    ObjectId.fromString(value),
                    ObjectId.fromString(auxValue));
        } else if (refType == RefType.UNPEELED.getValue()) {
            return new ObjectIdRef.Unpeeled(Ref.Storage.NETWORK, name,
                    ObjectId.fromString(value));
        } else if (refType == RefType.SYMBOLIC.getValue()) {
            return new SymbolicRef(name, get(value));
        } else {
            throw new IllegalStateException("Unhandled ref type: " + refType);
        }
    }

    /**
     * Inserts a single ref into the database
     *
     * @throws IllegalStateException if the reference concrete type is not
     *                               one of the four handled classes
     *                               (@see RefType).
     */
    private void putRef(String name, Ref r) throws IOException {
        if (r instanceof SymbolicRef) {
            putRow(name, RefType.SYMBOLIC, r.getTarget().getName(), "");
        } else if (r instanceof ObjectIdRef.PeeledNonTag) {
            putRow(name, RefType.PEELED_NONTAG, r.getObjectId().name(), "");
        } else if (r instanceof ObjectIdRef.PeeledTag) {
            putRow(name, RefType.PEELED_TAG, r.getObjectId().name(),
                    r.getPeeledObjectId().toString());
        } else if (r instanceof ObjectIdRef.Unpeeled) {
            putRow(name, RefType.UNPEELED, r.getObjectId().name(), "");
        } else {
            throw new IllegalStateException("Unhandled ref type: " + r);
        }
    }

    /**
     * Inserts a row into the refs table. This works for both insertion of a
     * new row, and updating an existing row.
     *
     * @param name     the primary key
     * @param type     a type where the value is mapped to an integer through
     *                 the RefType enum
     * @param value    the value, either a commit id or in the case of a
     *                 symbolic reference, the target name
     * @param auxValue an additional value, either the peeled object id in the
     *                 case of a peeled tag ref, or an empty string for all
     *                 other types of commits
     * @throws IOException if an exception occurs when communicating to the
     *                     database
     */
    private void putRow(String name, RefType type, String value, String auxValue)
            throws IOException {
        try {
            Statement stmt = QueryBuilder.insertInto(keyspace, TABLE_NAME)
                    .value("name", name)
                    .value("type", type.getValue())
                    .value("value", value)
                    .value("aux_value", auxValue);

            session.execute(stmt);
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    /**
     * Removes the ref row given by "name".
     * <p/>
     * Given name is the primary key (and unique) only a single row will be
     * removed.
     *
     * @throws IOException if an exception occurs when communicating to the
     *                     database
     */
    private void removeRef(String name) throws IOException {
        try {
            Statement stmt = QueryBuilder.delete()
                    .from(keyspace, TABLE_NAME)
                    .where(QueryBuilder.eq("name", name));
            session.execute(stmt);
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }
}
