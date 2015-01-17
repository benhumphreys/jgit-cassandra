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
package com.benhumphreys.jgitcassandra.store;

/**
 * Maps reference types to ints.
 * Used for storage of the reference type (encoded as an int) in the refs
 * table.
 */
public enum RefType {
    SYMBOLIC(1),
    PEELED_NONTAG(2),
    PEELED_TAG(3),
    UNPEELED(4);

    private final int value;

    private RefType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
