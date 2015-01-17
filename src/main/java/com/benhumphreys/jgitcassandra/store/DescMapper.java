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

import org.eclipse.jgit.internal.storage.dfs.DfsPackDescription;
import org.eclipse.jgit.internal.storage.pack.PackExt;

import java.util.HashMap;
import java.util.Map;

/**
 * Provides utility functions for mapping between the DfsPackDescrption
 * object and the data storage layer.
 */
public class DescMapper {

    /**
     * Extracts a file size map from the pack description.
     *
     * @param desc
     * @return a map mapping extension strings to file sizes
     */
    public static Map<String, Long> getFileSizeMap(DfsPackDescription desc) {
        Map<String, Long>  sizeMap = new HashMap<String, Long>();
        for (PackExt ext : PackExt.values()) {
            long sz = desc.getFileSize(ext);
            if (sz > 0) {
                sizeMap.put(ext.getExtension(), sz);
            }
        }
        return sizeMap;
    }

    /**
     * Given a map of extension/size, add each to the pack description.
     *
     * @param desc      pack description to mutate
     * @param extsize   a map from extension string to file size
     */
    public static void setFileSizeMap(DfsPackDescription desc, Map<String, Long> extsize) {
        for (Map.Entry<String, Long> entry : extsize.entrySet()) {
            desc.setFileSize(lookupExt(entry.getKey()), entry.getValue());
        }
    }

    /**
     * Given a pack description, return a bit field representing the PackExt's
     * present.

     * The actual mapping from PackExt to bit position is provided by the
     * PackExt class.
     *
     * @param desc  the pack description to query for pack extensions
     * @return  an integer with bits set for each PackExt present in the pack
     *          description
     */
    public static int getExtBits(DfsPackDescription desc) {
        int bits = 0;
        for (PackExt ext : PackExt.values()) {
            if (desc.hasFileExt(ext)) {
                bits |= ext.getBit();
            }
        }
        return bits;
    }

    /**
     * Calls addFileExt on the pack description for each PackExt indicated by
     * the bit field "bits"
     *
     * The actual mapping from PackExt to bit position is provided by the
     * PackExt class.
     *
     * @param desc  the pack description to mutate
     * @param bits  the bit field to read from
     */
    public static void setExtsFromBits(DfsPackDescription desc, int bits) {
        for (PackExt ext : PackExt.values()) {
            if ((ext.getBit() & bits) != 0) {
                desc.addFileExt(ext);
            }
        }
    }

    /**
     * The PackExt class defines a number of static instances
     */
    private static PackExt lookupExt(String extStr) {
        for (PackExt ext : PackExt.values()) {
            if (ext.getExtension().equals(extStr)) {
                return ext;
            }
        }

        // If we get here, the extension does not exist so create it. It gets
        // added to the list of known extensions in PackExt, so next time the
        // lookup will be successful
        return PackExt.newPackExt(extStr);
    }
}
