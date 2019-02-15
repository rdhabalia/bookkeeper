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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.function.LongPredicate;

import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap;

/**
 * Records the total size, remaining size and the set of ledgers that comprise a
 * entry log.
 */
public class EntryLogMetadata {
    private final long entryLogId;
    private long totalSize;
    private long remainingSize;
    private final ConcurrentLongLongHashMap ledgersMap;

    public EntryLogMetadata(long logId) {
        this.entryLogId = logId;

        totalSize = remainingSize = 0;
        ledgersMap = new ConcurrentLongLongHashMap(256, 1);
    }

    public void addLedgerSize(long ledgerId, long size) {
        totalSize += size;
        remainingSize += size;
        ledgersMap.addAndGet(ledgerId, size);
    }

    public boolean containsLedger(long ledgerId) {
        return ledgersMap.containsKey(ledgerId);
    }

    public double getUsage() {
        if (totalSize == 0L) {
            return 0.0f;
        }
        return (double) remainingSize / totalSize;
    }

    public boolean isEmpty() {
        return ledgersMap.isEmpty();
    }

    public long getEntryLogId() {
        return entryLogId;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public long getRemainingSize() {
        return remainingSize;
    }

    ConcurrentLongLongHashMap getLedgersMap() {
        return ledgersMap;
    }

    public void removeLedgerIf(LongPredicate predicate) {
        ledgersMap.removeIf((ledgerId, size) -> {
            boolean shouldRemove = predicate.test(ledgerId);
            if (shouldRemove) {
                remainingSize -= size;
            }
            return shouldRemove;
        });
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ totalSize = ").append(totalSize).append(", remainingSize = ").append(remainingSize)
                .append(", ledgersMap = ").append(ledgersMap).append(" }");
        return sb.toString();
    }

    /**
     * Serializes {@link EntryLogMetadata} and writes to
     * {@link DataOutputStream}.
     * 
     * @param out
     * @throws IOException
     *             throws if it couldn't serialize metadata-fields
     * @throws IllegalStateException
     *             throws if it couldn't serialize ledger-map
     */
    public void serialize(DataOutputStream out) throws IOException, IllegalStateException {
        out.writeLong(entryLogId);
        out.writeLong(totalSize);
        out.writeLong(remainingSize);
        out.writeLong(ledgersMap.size());
        ledgersMap.forEach((ledgerId, size) -> {
            try {
                out.writeLong(ledgerId);
                out.writeLong(size);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to serialize entryLogMetadata", e);
            }
        });
        out.writeLong(remainingSize);
    }

    /**
     * Deserializes {@link EntryLogMetadata} from given {@link DataInputStream}.
     * 
     * @param in
     * @return
     * @throws IOException
     */
    public static EntryLogMetadata deserialize(DataInputStream in) throws IOException {
        EntryLogMetadata metadata = new EntryLogMetadata(in.readLong());
        metadata.totalSize = in.readLong();
        metadata.remainingSize = in.readLong();
        long ledgersMapSize = in.readLong();
        for (int i = 0; i < ledgersMapSize; i++) {
            long ledgerId = in.readLong();
            long entryId = in.readLong();
            metadata.ledgersMap.put(ledgerId, entryId);
        }
        return metadata;
    }
}
