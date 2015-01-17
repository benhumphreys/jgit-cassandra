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
package com.benhumphreys.jgitcassandra;

import org.eclipse.jgit.lib.Ref;

/**
 * Utility functions shared between multiple classes
 */
public class Utils {
    /**
     * Compares references by object id.
     *
     * @return true if the refs a & b have the same object id, also true if
     * the object ids for both refs are null, otherwise false
     */
    public static boolean refsHaveEqualObjectId(Ref a, Ref b) {
        if (a.getObjectId() == null && b.getObjectId() == null) {
            return true;
        }
        if (a.getObjectId() != null) {
            return a.getObjectId().equals(b.getObjectId());
        }
        return false;
    }
}
