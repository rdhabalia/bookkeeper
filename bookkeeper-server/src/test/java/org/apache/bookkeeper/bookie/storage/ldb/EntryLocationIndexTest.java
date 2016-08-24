package org.apache.bookkeeper.bookie.storage.ldb;

import static junit.framework.Assert.assertEquals;

import java.io.File;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Test;

public class EntryLocationIndexTest {

    private final ServerConfiguration serverConfiguration = new ServerConfiguration();

    @Test
    public void deleteLedgerTest() throws Exception {
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        tmpDir.deleteOnExit();

        EntryLocationIndex idx = new EntryLocationIndex(serverConfiguration, KeyValueStorageRocksDB.factory,
                tmpDir.getAbsolutePath(), NullStatsLogger.INSTANCE, 1 * 1024);

        // Add some dummy indexes
        idx.addLocation(40312, 0, 1);
        idx.addLocation(40313, 10, 2);
        idx.addLocation(40320, 0, 3);

        // Add more indexes in a different batch
        idx.addLocation(40313, 11, 5);
        idx.addLocation(40313, 12, 6);
        idx.addLocation(40320, 1, 7);
        idx.addLocation(40312, 3, 4);

        idx.delete(40313);

        assertEquals(1, idx.getLocation(40312, 0));
        assertEquals(4, idx.getLocation(40312, 3));
        assertEquals(3, idx.getLocation(40320, 0));
        assertEquals(7, idx.getLocation(40320, 1));

        assertEquals(2, idx.getLocation(40313, 10));
        assertEquals(5, idx.getLocation(40313, 11));
        assertEquals(6, idx.getLocation(40313, 12));

        idx.flush();

        // After flush the keys will be removed
        assertEquals(0, idx.getLocation(40313, 10));
        assertEquals(0, idx.getLocation(40313, 11));
        assertEquals(0, idx.getLocation(40313, 12));

        idx.close();
    }

    // this tests if a ledger is added after it has been deleted
    @Test
    public void addLedgerAfterDeleteTest() throws Exception {
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        tmpDir.deleteOnExit();

        EntryLocationIndex idx = new EntryLocationIndex(serverConfiguration, KeyValueStorageRocksDB.factory,
                tmpDir.getAbsolutePath(), NullStatsLogger.INSTANCE, 1 * 1024);

        // Add some dummy indexes
        idx.addLocation(40312, 0, 1);
        idx.addLocation(40313, 10, 2);
        idx.addLocation(40320, 0, 3);

        idx.delete(40313);

        // Add more indexes in a different batch
        idx.addLocation(40313, 11, 5);
        idx.addLocation(40313, 12, 6);
        idx.addLocation(40320, 1, 7);
        idx.addLocation(40312, 3, 4);

        idx.flush();

        assertEquals(0, idx.getLocation(40313, 11));
        assertEquals(0, idx.getLocation(40313, 12));

        idx.close();
    }
}
