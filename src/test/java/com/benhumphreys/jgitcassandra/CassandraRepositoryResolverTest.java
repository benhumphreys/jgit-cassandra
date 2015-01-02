package com.benhumphreys.jgitcassandra;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
    }

}
