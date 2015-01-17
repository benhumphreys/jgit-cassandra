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

import org.eclipse.jgit.internal.storage.dfs.DfsPackDescription;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.pack.PackExt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class DescMapperTest {
    private static final String DESC_NAME = "testdesc";
    private DfsPackDescription desc;

    @Before
    public void setUp() throws Exception {
        desc = new DfsPackDescription(new DfsRepositoryDescription(), DESC_NAME);
    }

    @After
    public void tearDown() throws Exception {
        desc = null;
    }

    @Test
    public void testGetFileSizeMap() throws Exception {
        // Prepare the desc object
        desc.setFileSize(PackExt.PACK, 1L);
        desc.setFileSize(PackExt.INDEX, 2L);

        // Test the mapper
        Map<String, Long> extSizes = DescMapper.getFileSizeMap(desc);
        assertEquals(Long.valueOf(1), extSizes.get(PackExt.PACK.getExtension()));
        assertEquals(Long.valueOf(2), extSizes.get(PackExt.INDEX.getExtension()));
        assertFalse(extSizes.containsKey(PackExt.BITMAP_INDEX.getExtension()));
    }

    @Test
    public void testSetFileSizeMap() throws Exception {
        Map<String, Long> extSizes = new HashMap<String, Long>();
        extSizes.put("pack", 1L);
        extSizes.put("idx", 2L);

        DescMapper.setFileSizeMap(desc, extSizes);
        assertEquals(1, desc.getFileSize(PackExt.PACK));
        assertEquals(2, desc.getFileSize(PackExt.INDEX));
        assertEquals(0, desc.getFileSize(PackExt.BITMAP_INDEX));
    }

    @Test
    public void testGetExtBits() throws Exception {
        // Prepare the desc object
        desc.addFileExt(PackExt.PACK);
        desc.addFileExt(PackExt.INDEX);

        int bits = DescMapper.getExtBits(desc);
        assertEquals(3, bits);
    }

    @Test
    public void testSetExtsFromBit() throws Exception {
        int bits = 3;
        DescMapper.setExtsFromBits(desc, bits);
        assertTrue(desc.hasFileExt(PackExt.PACK));
        assertTrue(desc.hasFileExt(PackExt.INDEX));
        assertFalse(desc.hasFileExt(PackExt.BITMAP_INDEX));
    }
}