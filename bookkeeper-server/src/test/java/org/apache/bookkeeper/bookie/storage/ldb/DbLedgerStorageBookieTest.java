package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertEquals;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

public class DbLedgerStorageBookieTest extends BookKeeperClusterTestCase {

    public DbLedgerStorageBookieTest() {
        super(1);
        baseConf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        baseConf.setFlushInterval(60000);
        baseConf.setGcWaitTime(60000);
    }

    @Test
    public void testRecoveryEmptyLedger() throws Exception {
        LedgerHandle lh1 = bkc.createLedger(1, 1, DigestType.MAC, new byte[0]);

        // Force ledger close & recovery
        LedgerHandle lh2 = bkc.openLedger(lh1.getId(), DigestType.MAC, new byte[0]);

        assertEquals(0, lh2.getLength());
        assertEquals(-1, lh2.getLastAddConfirmed());
    }
}
