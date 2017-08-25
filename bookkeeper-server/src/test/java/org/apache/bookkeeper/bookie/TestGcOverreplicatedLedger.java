package org.apache.bookkeeper.bookie;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.bookie.GarbageCollector.GarbageCleaner;
import org.apache.bookkeeper.bookie.GarbageCollectorThread.CompactableLedgerStorage;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.meta.FlatLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerManagerTestCase;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.util.SnapshotMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@RunWith(Parameterized.class)
public class TestGcOverreplicatedLedger extends LedgerManagerTestCase {

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        ledgerManager = ledgerManagerFactory.newLedgerManager();
        activeLedgers = new SnapshotMap<Long, Boolean>();
    }

    public TestGcOverreplicatedLedger(Class<? extends LedgerManagerFactory> lmFactoryCls) {
        super(lmFactoryCls, 3);
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { FlatLedgerManagerFactory.class } });
    }

    @Test(timeout = 60000)
    public void testGcOverreplicatedLedger() throws Exception {
        LedgerHandle lh = bkc.createLedger(2, 2, DigestType.MAC, "".getBytes());
        activeLedgers.put(lh.getId(), true);

        AtomicReference<LedgerMetadata> newLedgerMetadata = new AtomicReference<>(null);
        CountDownLatch latch = new CountDownLatch(1);
        ledgerManager.readLedgerMetadata(lh.getId(), new GenericCallback<LedgerMetadata>() {

            @Override
            public void operationComplete(int rc, LedgerMetadata result) {
                if (rc == BKException.Code.OK) {
                    newLedgerMetadata.set(result);
                }
                latch.countDown();
            }
        });
        latch.await();
        if (newLedgerMetadata.get() == null) {
            Assert.fail("No ledger metadata found");
        }
        BookieSocketAddress bookieNotInEnsemble = getBookieNotInEnsemble(newLedgerMetadata.get());

        lh.close();

        final CompactableLedgerStorage mockLedgerStorage = new MockLedgerStorage();
        final GarbageCollector garbageCollector = new ScanAndCompareGarbageCollector(ledgerManager, mockLedgerStorage,
                bookieNotInEnsemble, zkc, true, baseConf.getZkLedgersRootPath(), Sets.newHashSet());
        garbageCollector.gc(new GarbageCleaner() {

            @Override
            public void clean(long ledgerId) {
                try {
                    mockLedgerStorage.deleteLedger(ledgerId);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
            }
        });

        Assert.assertFalse(activeLedgers.containsKey(lh.getId()));
    }

    @Test(timeout = 60000)
    public void testNoGcOfLedger() throws Exception {
        LedgerHandle lh = bkc.createLedger(2, 2, DigestType.MAC, "".getBytes());
        activeLedgers.put(lh.getId(), true);

        AtomicReference<LedgerMetadata> newLedgerMetadata = new AtomicReference<>(null);
        CountDownLatch latch = new CountDownLatch(1);
        ledgerManager.readLedgerMetadata(lh.getId(), new GenericCallback<LedgerMetadata>() {

            @Override
            public void operationComplete(int rc, LedgerMetadata result) {
                if (rc == BKException.Code.OK) {
                    newLedgerMetadata.set(result);
                }
                latch.countDown();
            }
        });
        latch.await();
        if (newLedgerMetadata.get() == null) {
            Assert.fail("No ledger metadata found");
        }
        BookieSocketAddress address = null;
        SortedMap<Long, ArrayList<BookieSocketAddress>> ensembleMap = newLedgerMetadata.get().getEnsembles();
        for (ArrayList<BookieSocketAddress> ensemble : ensembleMap.values()) {
            address = ensemble.get(0);
        }

        lh.close();

        final CompactableLedgerStorage mockLedgerStorage = new MockLedgerStorage();
        final GarbageCollector garbageCollector = new ScanAndCompareGarbageCollector(ledgerManager, mockLedgerStorage,
                address, zkc, true, baseConf.getZkLedgersRootPath(), Sets.newHashSet());
        garbageCollector.gc(new GarbageCleaner() {

            @Override
            public void clean(long ledgerId) {
                try {
                    mockLedgerStorage.deleteLedger(ledgerId);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
            }
        });

        Assert.assertTrue(activeLedgers.containsKey(lh.getId()));
    }

    @Test(timeout = 60000)
    public void testNoGcIfLedgerBeingReplicated() throws Exception {
        LedgerHandle lh = bkc.createLedger(2, 2, DigestType.MAC, "".getBytes());
        activeLedgers.put(lh.getId(), true);

        AtomicReference<LedgerMetadata> newLedgerMetadata = new AtomicReference<>(null);
        CountDownLatch latch = new CountDownLatch(1);
        ledgerManager.readLedgerMetadata(lh.getId(), new GenericCallback<LedgerMetadata>() {

            @Override
            public void operationComplete(int rc, LedgerMetadata result) {
                if (rc == BKException.Code.OK) {
                    newLedgerMetadata.set(result);
                }
                latch.countDown();
            }
        });
        latch.await();
        if (newLedgerMetadata.get() == null) {
            Assert.fail("No ledger metadata found");
        }
        BookieSocketAddress bookieNotInEnsemble = getBookieNotInEnsemble(newLedgerMetadata.get());

        lh.close();

        ZkLedgerUnderreplicationManager.acquireUnderreplicatedLedgerLock(zkc, baseConf.getZkLedgersRootPath(),
                lh.getId());

        final CompactableLedgerStorage mockLedgerStorage = new MockLedgerStorage();
        final GarbageCollector garbageCollector = new ScanAndCompareGarbageCollector(ledgerManager, mockLedgerStorage,
                bookieNotInEnsemble, zkc, true, baseConf.getZkLedgersRootPath(), Sets.newHashSet());
        garbageCollector.gc(new GarbageCleaner() {

            @Override
            public void clean(long ledgerId) {
                try {
                    mockLedgerStorage.deleteLedger(ledgerId);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
            }
        });

        Assert.assertTrue(activeLedgers.containsKey(lh.getId()));
    }

    private BookieSocketAddress getBookieNotInEnsemble(LedgerMetadata ledgerMetadata) throws UnknownHostException {
        List<BookieSocketAddress> allAddresses = Lists.newArrayList();
        for (BookieServer bk : bs) {
            allAddresses.add(bk.getLocalAddress());
        }
        SortedMap<Long, ArrayList<BookieSocketAddress>> ensembles = ledgerMetadata.getEnsembles();
        for (ArrayList<BookieSocketAddress> fragmentEnsembles : ensembles.values()) {
            for (BookieSocketAddress ensemble : fragmentEnsembles) {
                allAddresses.remove(ensemble);
            }
        }
        Assert.assertEquals(allAddresses.size(), 1);
        return allAddresses.get(0);
    }
}
