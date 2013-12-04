package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class DbLedgerStorageBookieTest extends BookKeeperClusterTestCase {

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { false }/*, { true }*/ });
    }

    public DbLedgerStorageBookieTest(boolean rocksDBEnabled) {
        super(1);
        baseConf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        baseConf.setFlushInterval(60000);
        baseConf.setGcWaitTime(60000);
        baseConf.setProperty(DbLedgerStorage.ROCKSDB_ENABLED, rocksDBEnabled);
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
