/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.bookkeeper.bookie.EntryLogMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit test for {@link PersistentEntryLogMetadataMap}.
 */
public class PersistentEntryLogMetadataMapTest {

    private final ServerConfiguration configuration;

    public PersistentEntryLogMetadataMapTest() {
        this.configuration = new ServerConfiguration();
    }

    /**
     * Validates PersistentEntryLogMetadataMap functionalities.
     * 
     * @throws Exception
     */
    @Test
    public void simple() throws Exception {
        Path tmpDir = Files.createTempDirectory("bookie");
        String path = tmpDir.toAbsolutePath().toString();
        try {
            PersistentEntryLogMetadataMap entryMetadataMap = new PersistentEntryLogMetadataMap(path, configuration);

            List<EntryLogMetadata> metadatas = Lists.newArrayList();
            int totalMetadata = 1000;
            // insert entry-log-metadata records
            for (int i = 1; i <= totalMetadata; i++) {
                EntryLogMetadata entryLogMeta = createEntryLogMetadata(i, i);
                metadatas.add(entryLogMeta);
                entryMetadataMap.put(i, entryLogMeta);
            }
            for (int i = 1; i <= totalMetadata; i++) {
                assertTrue(entryMetadataMap.containsKey(i));
            }

            assertEquals(entryMetadataMap.size(), totalMetadata);

            entryMetadataMap.forEach((logId, metadata) -> {
                assertEquals(metadatas.get(logId.intValue() - 1).getTotalSize(), metadata.getTotalSize());
                for (int i = 0; i < logId.intValue(); i++) {
                    assertTrue(metadata.containsLedger(i));
                }
            });

            // remove entry-log entry
            for (int i = 1; i <= totalMetadata; i++) {
                entryMetadataMap.remove(i);
            }

            // entries should not be present into map
            for (int i = 1; i <= totalMetadata; i++) {
                assertFalse(entryMetadataMap.containsKey(i));
            }

            assertEquals(entryMetadataMap.size(), 0);

            entryMetadataMap.close();
        } finally {
            FileUtils.deleteDirectory(new File(tmpDir.toAbsolutePath().toString()));
        }
    }

    /**
     * Validates PersistentEntryLogMetadataMap persists metadata state in
     * rocksDB.
     * 
     * @throws Exception
     */
    @Test
    public void closeAndOpen() throws Exception {
        Path tmpDir = Files.createTempDirectory("bookie");
        String path = tmpDir.toAbsolutePath().toString();
        try {
            PersistentEntryLogMetadataMap entryMetadataMap = new PersistentEntryLogMetadataMap(path, configuration);

            List<EntryLogMetadata> metadatas = Lists.newArrayList();
            int totalMetadata = 1000;
            for (int i = 1; i <= totalMetadata; i++) {
                EntryLogMetadata entryLogMeta = createEntryLogMetadata(i, i);
                metadatas.add(entryLogMeta);
                entryMetadataMap.put(i, entryLogMeta);
            }
            for (int i = 1; i <= totalMetadata; i++) {
                assertTrue(entryMetadataMap.containsKey(i));
            }

            // close metadata-map
            entryMetadataMap.close();
            // Open it again
            entryMetadataMap = new PersistentEntryLogMetadataMap(path, configuration);

            entryMetadataMap.forEach((logId, metadata) -> {
                assertEquals(metadatas.get(logId.intValue() - 1).getTotalSize(), logId.longValue());
                for (int i = 0; i < logId.intValue(); i++) {
                    assertTrue(metadata.containsLedger(i));
                }
            });

            entryMetadataMap.close();
        } finally {
            FileUtils.deleteDirectory(new File(tmpDir.toAbsolutePath().toString()));
        }
    }

    private EntryLogMetadata createEntryLogMetadata(long logId, long totalLedgers) {
        EntryLogMetadata metadata = new EntryLogMetadata(logId);
        for (int i = 0; i < totalLedgers; i++) {
            metadata.addLedgerSize(i, 1);
        }
        return metadata;
    }
}
