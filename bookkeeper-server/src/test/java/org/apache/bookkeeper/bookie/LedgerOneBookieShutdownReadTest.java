/*
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
package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertEquals;

import java.util.Enumeration;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

/**
 * Tests of the main BookKeeper client.
 */
@Slf4j
public class LedgerOneBookieShutdownReadTest extends BookKeeperClusterTestCase {

    private static final int NUM_BOOKIES = 3;

    DigestType digestType;

    public LedgerOneBookieShutdownReadTest() {
        super(NUM_BOOKIES);
        this.digestType = DigestType.CRC32;
    }

    @Test
    public void testReadLegerWithUnAvailableBookie() throws Exception {
        LedgerHandle writelh = bkc.createLedger(3, 3, digestType, "testPasswd".getBytes());

        final int numEntries = 100;
        for (int i = 0; i < numEntries; i++) {
            writelh.addEntry(("entry-" + i).getBytes());
        }
        killBookie(1);

        int seq = 0;
        for (int e = 0; e < (numEntries / NUM_BOOKIES); e++) {
            int startEntry = NUM_BOOKIES * e;
            int endEntry = startEntry + NUM_BOOKIES - 1;
            Enumeration<LedgerEntry> entries = writelh.readEntries(startEntry, endEntry);
            for (int i = 0; i < NUM_BOOKIES; i++) {
                LedgerEntry entry = entries.nextElement();
                assertEquals("entry-" + (seq++), new String(entry.getEntry()));
            }
        }
    }

    
}
