package org.apache.bookkeeper.bookie;

import java.util.function.LongPredicate;

import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap;

/**
 * Records the total size, remaining size and the set of ledgers that comprise a entry log.
 */
public class EntryLogMetadata {
    private final long entryLogId;
    private long totalSize;
    private long remainingSize;
    private final ConcurrentLongLongHashMap ledgersMap;
    private long lastModifiedTimeStamp = -1;

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

    public long getLastModifiedTimeStamp() {
        return lastModifiedTimeStamp;
    }

    public void setLastModifiedTimeStamp(long lastModifiedTimeStamp) {
        this.lastModifiedTimeStamp = lastModifiedTimeStamp;
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

}
