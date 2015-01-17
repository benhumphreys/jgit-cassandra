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
package com.benhumphreys.jgitcassandra;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CassandraRepositoryResolverTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public final void testSanitiseName() {
        assertEquals("repo",
                CassandraRepositoryResolver.sanitiseName("repo.git"));
        assertEquals("repo",
                CassandraRepositoryResolver.sanitiseName("REPO.git"));
        assertEquals("repo",
                CassandraRepositoryResolver.sanitiseName("repo"));
        assertEquals("my_repo",
                CassandraRepositoryResolver.sanitiseName("my_repo.git"));
        assertEquals("my-repo",
                CassandraRepositoryResolver.sanitiseName("my-repo.git"));
        assertEquals("repo1",
                CassandraRepositoryResolver.sanitiseName("repo1.git"));
    }

    @Test(expected = IllegalArgumentException.class)
    public final void testSanitiseNameInvalid() {
        CassandraRepositoryResolver.sanitiseName("DROP TABLE refs.git");
    }
}
