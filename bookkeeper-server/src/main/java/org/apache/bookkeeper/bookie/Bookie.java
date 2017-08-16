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

package org.apache.bookkeeper.bookie;

import static com.google.common.base.Charsets.UTF_8;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.bookie.Journal.JournalScanner;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.jmx.BKMBeanInfo;
import org.apache.bookkeeper.jmx.BKMBeanRegistry;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNS;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_LEDGER_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_INDEX_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_BYTES;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.SERVER_STATUS;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.WRITE_BYTES;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.STORAGE_SCOPE;

/**
 * Implements a bookie.
 *
 */
public class Bookie extends BookieCriticalThread {

    private final static Logger LOG = LoggerFactory.getLogger(Bookie.class);

    final List<File> journalDirectories;
    final ServerConfiguration conf;

    final SyncThread syncThread;
    final LedgerStorage ledgerStorage;
    final List<Journal> journals;

    final HandleFactory handles;

    static final long METAENTRY_ID_LEDGER_KEY = -0x1000;
    static final long METAENTRY_ID_FENCE_KEY  = -0x2000;

    private final LedgerDirsManager ledgerDirsManager;
    private LedgerDirsManager indexDirsManager;

    // Running flag
    private volatile boolean running = false;
    // Flag identify whether it is in shutting down progress
    private volatile boolean shuttingdown = false;

    private int exitCode = ExitCode.OK;

    // jmx related beans
    BookieBean jmxBookieBean;
    BKMBeanInfo jmxLedgerStorageBean;

    private final ConcurrentLongHashMap<byte[]> masterKeyCache = new ConcurrentLongHashMap<>();

    final protected Registrar registrar;

    final private AtomicBoolean readOnly = new AtomicBoolean(false);

    // Expose Stats
    private final Counter writeBytes;
    private final Counter readBytes;

    public static class NoLedgerException extends IOException {
        private static final long serialVersionUID = 1L;
        private final long ledgerId;
        public NoLedgerException(long ledgerId) {
            super("Ledger " + ledgerId + " not found");
            this.ledgerId = ledgerId;
        }
        public long getLedgerId() {
            return ledgerId;
        }
    }
    public static class NoEntryException extends IOException {
        private static final long serialVersionUID = 1L;
        private final long ledgerId;
        private final long entryId;
        public NoEntryException(long ledgerId, long entryId) {
            this("Entry " + entryId + " not found in " + ledgerId, ledgerId, entryId);
        }

        public NoEntryException(String msg, long ledgerId, long entryId) {
            super(msg);
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }

        public long getLedger() {
            return ledgerId;
        }
        public long getEntry() {
            return entryId;
        }
    }

    // Write Callback do nothing
    static class NopWriteCallback implements WriteCallback {
        @Override
        public void writeComplete(int rc, long ledgerId, long entryId,
                                  BookieSocketAddress addr, Object ctx) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Finished writing entry {} @ ledger {} for {} : {}",
                          new Object[] { entryId, ledgerId, addr, rc });
            }
        }
    }

    final static Future<Boolean> SUCCESS_FUTURE = new Future<Boolean>() {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) { return false; }
        @Override
        public Boolean get() { return true; }
        @Override
        public Boolean get(long timeout, TimeUnit unit) { return true; }
        @Override
        public boolean isCancelled() { return false; }
        @Override
        public boolean isDone() {
            return true;
        }
    };

    static class CountDownLatchFuture<T> implements Future<T> {

        T value = null;
        volatile boolean done = false;
        CountDownLatch latch = new CountDownLatch(1);

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) { return false; }
        @Override
        public T get() throws InterruptedException {
            latch.await();
            return value;
        }
        @Override
        public T get(long timeout, TimeUnit unit)
            throws InterruptedException, TimeoutException {
            if (!latch.await(timeout, unit)) {
                throw new TimeoutException("Timed out waiting for latch");
            }
            return value;
        }

        @Override
        public boolean isCancelled() { return false; }

        @Override
        public boolean isDone() {
            return done;
        }

        void setDone(T value) {
            this.value = value;
            done = true;
            latch.countDown();
        }
    }

    static class FutureWriteCallback implements WriteCallback {

        CountDownLatchFuture<Boolean> result =
            new CountDownLatchFuture<Boolean>();

        @Override
        public void writeComplete(int rc, long ledgerId, long entryId,
                                  BookieSocketAddress addr, Object ctx) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Finished writing entry {} @ ledger {} for {} : {}",
                          new Object[] { entryId, ledgerId, addr, rc });
            }
            result.setDone(0 == rc);
        }

        public Future<Boolean> getResult() {
            return result;
        }
    }

    public static void checkDirectoryStructure(File dir) throws IOException {
        if (!dir.exists()) {
            File parent = dir.getParentFile();
            File preV3versionFile = new File(dir.getParent(),
                    BookKeeperConstants.VERSION_FILENAME);

            final AtomicBoolean oldDataExists = new AtomicBoolean(false);
            parent.list(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        if (name.endsWith(".txn") || name.endsWith(".idx") || name.endsWith(".log")) {
                            oldDataExists.set(true);
                        }
                        return true;
                    }
                });
            if (preV3versionFile.exists() || oldDataExists.get()) {
                String err = "Directory layout version is less than 3, upgrade needed";
                LOG.error(err);
                throw new IOException(err);
            }
            if (!dir.mkdirs()) {
                String err = "Unable to create directory " + dir;
                LOG.error(err);
                throw new IOException(err);
            }
        }
    }

    /**
     * Check that the environment for the bookie is correct.
     * This means that the configuration has stayed the same as the
     * first run and the filesystem structure is up to date.
     */
    private void checkEnvironment(ZooKeeper zk) throws BookieException, IOException {
        List<File> allLedgerDirs = new ArrayList<File>(ledgerDirsManager.getAllLedgerDirs().size()
                                                     + indexDirsManager.getAllLedgerDirs().size());
        allLedgerDirs.addAll(ledgerDirsManager.getAllLedgerDirs());
        if (indexDirsManager != ledgerDirsManager) {
            allLedgerDirs.addAll(indexDirsManager.getAllLedgerDirs());
        }
        if (zk == null) { // exists only for testing, just make sure directories are correct
            for (File journalDirectory : journalDirectories) {
                checkDirectoryStructure(journalDirectory);
            }
            for (File dir : allLedgerDirs) {
                checkDirectoryStructure(dir);
            }
            return;
        }
        try {
            boolean newEnv = false;
            List<File> missedCookieDirs = new ArrayList<File>();
            List<Cookie> journalCookies = Lists.newArrayList();

            // try to read cookie from journal directory
            for (File journalDirectory : journalDirectories) {
                try {
                    checkDirectoryStructure(journalDirectory);
                    Cookie journalCookie = Cookie.readFromDirectory(journalDirectory);
                    journalCookies.add(journalCookie);

                    if (journalCookie.isBookieHostCreatedFromIp()) {
                        conf.setUseHostNameAsBookieID(false);
                    } else {
                        conf.setUseHostNameAsBookieID(true);
                    }
                } catch (FileNotFoundException fnf) {
                    newEnv = true;
                    missedCookieDirs.add(journalDirectory);
                }
            }

            String instanceId = getInstanceId(zk);
            Cookie.Builder builder = Cookie.generateCookie(conf);
            if (null != instanceId) {
                builder.setInstanceId(instanceId);
            }
            Cookie masterCookie = builder.build();
            try {
                Versioned<Cookie> zkCookie = Cookie.readFromZooKeeper(zk, conf);
                masterCookie.verify(zkCookie.getValue());
            } catch (KeeperException.NoNodeException nne) {
                // can occur in cases:
                // 1) new environment or
                // 2) done only metadata format and started bookie server.
            }

            for (File journalDirectory : journalDirectories) {
                checkDirectoryStructure(journalDirectory);
            }

            if (!newEnv){
                for (Cookie journalCookie : journalCookies) {
                    journalCookie.verify(masterCookie);
                }
            }
            for (File dir : allLedgerDirs) {
                checkDirectoryStructure(dir);
                try {
                    Cookie c = Cookie.readFromDirectory(dir);
                    c.verify(masterCookie);
                } catch (FileNotFoundException fnf) {
                    missedCookieDirs.add(dir);
                }
            }

            if (!newEnv && missedCookieDirs.size() > 0){
                LOG.error("Cookie exists in zookeeper, but not in all local directories. "
                        + " Directories missing cookie file are " + missedCookieDirs);
                throw new BookieException.InvalidCookieException();
            }
            if (newEnv) {
                if (missedCookieDirs.size() > 0) {
                    LOG.debug("Directories missing cookie file are {}", missedCookieDirs);
                    for (File journalDirectory : journalDirectories) {
                        masterCookie.writeToDirectory(journalDirectory);
                    }

                    for (File dir : allLedgerDirs) {
                        masterCookie.writeToDirectory(dir);
                    }
                }
                masterCookie.writeToZooKeeper(zk, conf, Version.NEW);
            }
        } catch (KeeperException ke) {
            LOG.error("Couldn't access cookie in zookeeper", ke);
            throw new BookieException.InvalidCookieException(ke);
        } catch (UnknownHostException uhe) {
            LOG.error("Couldn't check cookies, networking is broken", uhe);
            throw new BookieException.InvalidCookieException(uhe);
        } catch (IOException ioe) {
            LOG.error("Error accessing cookie on disks", ioe);
            throw new BookieException.InvalidCookieException(ioe);
        } catch (InterruptedException ie) {
            LOG.error("Thread interrupted while checking cookies, exiting", ie);
            throw new BookieException.InvalidCookieException(ie);
        }
    }

    /**
     * Return the configured address of the bookie.
     */
    public static BookieSocketAddress getBookieAddress(ServerConfiguration conf)
            throws UnknownHostException {
        // Advertised address takes precedence over the listening interface and the
        // useHostNameAsBookieID settings
        if (conf.getAdvertisedAddress() != null && conf.getAdvertisedAddress().trim().length() > 0) {
            String hostAddress = conf.getAdvertisedAddress().trim();
            return new BookieSocketAddress(hostAddress, conf.getBookiePort());
        }

        String iface = conf.getListeningInterface();
        if (iface == null) {
            iface = "default";
        }
        InetSocketAddress inetAddr = new InetSocketAddress(DNS.getDefaultHost(iface), conf.getBookiePort());
        String hostAddress = inetAddr.getAddress().getHostAddress();
        if (conf.getUseHostNameAsBookieID()) {
            hostAddress = inetAddr.getAddress().getCanonicalHostName();
        }

        BookieSocketAddress addr =
                new BookieSocketAddress(hostAddress, conf.getBookiePort());
        if (addr.getSocketAddress().getAddress().isLoopbackAddress()
            && !conf.getAllowLoopback()) {
            throw new UnknownHostException("Trying to listen on loopback address, "
                    + addr + " but this is forbidden by default "
                    + "(see ServerConfiguration#getAllowLoopback())");
        }
        return addr;
    }

    private String getInstanceId(ZooKeeper zk) throws KeeperException,
            InterruptedException {
        String instanceId = null;
        if (zk.exists(conf.getZkLedgersRootPath(), null) == null) {
            LOG.error("BookKeeper metadata doesn't exist in zookeeper. "
                      + "Has the cluster been initialized? "
                      + "Try running bin/bookkeeper shell metaformat");
            throw new KeeperException.NoNodeException("BookKeeper metadata");
        }
        try {
            byte[] data = zk.getData(conf.getZkLedgersRootPath() + "/"
                    + BookKeeperConstants.INSTANCEID, false, null);
            instanceId = new String(data, UTF_8);
        } catch (KeeperException.NoNodeException e) {
            LOG.info("INSTANCEID not exists in zookeeper. Not considering it for data verification");
        }
        return instanceId;
    }

    public LedgerDirsManager getLedgerDirsManager() {
        return ledgerDirsManager;
    }

    LedgerDirsManager getIndexDirsManager() {
        return indexDirsManager;
    }

    public static File getCurrentDirectory(File dir) {
        return new File(dir, BookKeeperConstants.CURRENT_DIR);
    }

    public static File[] getCurrentDirectories(File[] dirs) {
        File[] currentDirs = new File[dirs.length];
        for (int i = 0; i < dirs.length; i++) {
            currentDirs[i] = getCurrentDirectory(dirs[i]);
        }
        return currentDirs;
    }

    public Bookie(ServerConfiguration conf)
            throws IOException, KeeperException, InterruptedException, BookieException {
        this(conf, NullStatsLogger.INSTANCE);
    }

    public Bookie(ServerConfiguration conf, StatsLogger statsLogger)
            throws IOException, KeeperException, InterruptedException, BookieException {
        super("Bookie-" + conf.getBookiePort());
        this.conf = conf;
        this.journalDirectories = Lists.newArrayList();
        for (File journalDirectory : conf.getJournalDirs()) {
            journalDirectories.add(getCurrentDirectory(journalDirectory));
        }

        this.ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                statsLogger.scope(LD_LEDGER_SCOPE));
        File[] idxDirs = conf.getIndexDirs();
        if (null == idxDirs) {
            this.indexDirsManager = this.ledgerDirsManager;
        } else {
            this.indexDirsManager = new LedgerDirsManager(conf, idxDirs,
                    statsLogger.scope(LD_INDEX_SCOPE));
        }

        // instantiate zookeeper client to initialize ledger manager
        ZooKeeper zk = instantiateZookeeperClient(conf);
        checkEnvironment(zk);
        if (zk != null) {
            zk.close();
        }

        // Initialise ledgerDirManager. This would look through all the
        // configured directories. When disk errors or all the ledger
        // directories are full, would throws exception and fail bookie startup.
        this.ledgerDirsManager.init();

        // instantiate the journals
        if (journalDirectories.size() == 1) {
            journals = Lists.newArrayList(new Journal(journalDirectories.get(0), conf, ledgerDirsManager, statsLogger
                    .scope(JOURNAL_SCOPE)));
        } else {
            journals = Lists.newArrayList();
            for (int idx = 0; idx < journalDirectories.size(); idx++) {
                journals.add(new Journal(journalDirectories.get(idx), conf, ledgerDirsManager, statsLogger
                        .scope(JOURNAL_SCOPE + "-" + idx)));
            }
        }

        CheckpointSource checkpointSource = new CheckpointSourceList(journals);

        String ledgerStorageClass = conf.getLedgerStorageClass();
        // Check the type of storage.
        if (ledgerStorageClass.equals(InterleavedLedgerStorage.class.getName())
            && conf.getSortedLedgerStorageEnabled()) {
            ledgerStorage = new SortedLedgerStorage();
            ledgerStorage.initialize(conf, new GarbageCollectorThread.LedgerManagerProviderImpl(conf),
                                     ledgerDirsManager, indexDirsManager,
                                     checkpointSource, statsLogger);
        } else {
            LOG.info("using ledger storage: {}", ledgerStorageClass);
            ledgerStorage = LedgerStorageFactory.createLedgerStorage(ledgerStorageClass);
            ledgerStorage.initialize(conf, new GarbageCollectorThread.LedgerManagerProviderImpl(conf),
                    ledgerDirsManager, indexDirsManager, checkpointSource, statsLogger.scope(STORAGE_SCOPE));
        }
        syncThread = new SyncThread(conf, getLedgerDirsListener(),
                                    ledgerStorage, checkpointSource);

        handles = new HandleFactoryImpl(ledgerStorage);

        // Expose Stats
        writeBytes = statsLogger.getCounter(WRITE_BYTES);
        readBytes = statsLogger.getCounter(READ_BYTES);
        // 1 : up, 0 : readonly
        statsLogger.registerGauge(SERVER_STATUS, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return readOnly.get() ? 0 : 1;
            }
        });

        registrar = new Registrar(conf, getMyId(), new Registrar.FatalErrorHandler() {
                @Override
                public void fatalError(Throwable t) {
                    LOG.error("Unrecoverable registration error, shutting down");
                    triggerBookieShutdown(ExitCode.ZK_REG_FAIL);
                }
            });
    }

    private String getMyId() throws UnknownHostException {
        return Bookie.getBookieAddress(conf).toString();
    }

    void readJournal() throws IOException, BookieException {
        JournalScanner scanner = new JournalScanner() {
            @Override
            public void process(int journalVersion, long offset, ByteBuffer recBuff) throws IOException {
                long ledgerId = recBuff.getLong();
                long entryId = recBuff.getLong();
                try {
                    LOG.debug("Replay journal - ledger id : {}, entry id : {}.", ledgerId, entryId);
                    if (entryId == METAENTRY_ID_LEDGER_KEY) {
                        if (journalVersion >= JournalChannel.V3) {
                            int masterKeyLen = recBuff.getInt();
                            byte[] masterKey = new byte[masterKeyLen];

                            recBuff.get(masterKey);
                            masterKeyCache.put(ledgerId, masterKey);

                            // Force to re-insert the master key in ledger storage
                            handles.getHandle(ledgerId, masterKey);
                        } else {
                            throw new IOException("Invalid journal. Contains journalKey "
                                    + " but layout version (" + journalVersion
                                    + ") is too old to hold this");
                        }
                    } else if (entryId == METAENTRY_ID_FENCE_KEY) {
                        if (journalVersion >= JournalChannel.V4) {
                            byte[] key = masterKeyCache.get(ledgerId);
                            if (key == null) {
                                key = ledgerStorage.readMasterKey(ledgerId);
                            }
                            LedgerDescriptor handle = handles.getHandle(ledgerId, key);
                            handle.setFenced();
                        } else {
                            throw new IOException("Invalid journal. Contains fenceKey "
                                    + " but layout version (" + journalVersion
                                    + ") is too old to hold this");
                        }
                    } else {
                        byte[] key = masterKeyCache.get(ledgerId);
                        if (key == null) {
                            key = ledgerStorage.readMasterKey(ledgerId);
                        }
                        LedgerDescriptor handle = handles.getHandle(ledgerId, key);

                        recBuff.rewind();
                        handle.addEntry(Unpooled.wrappedBuffer(recBuff));
                    }
                } catch (NoLedgerException nsle) {
                    LOG.debug("Skip replaying entries of ledger {} since it was deleted.", ledgerId);
                } catch (BookieException be) {
                    throw new IOException(be);
                }
            }
        };

        for (Journal journal : journals) {
            journal.replay(scanner);
        }
    }

    @Override
    synchronized public void start() {
        setDaemon(true);
        LOG.debug("I'm starting a bookie with journal directories {}", journalDirectories);
        //Start DiskChecker thread
        ledgerDirsManager.start();
        if (indexDirsManager != ledgerDirsManager) {
            indexDirsManager.start();
        }

        // replay journals
        try {
            readJournal();
        } catch (IOException ioe) {
            LOG.error("Exception while replaying journals, shutting down", ioe);
            shutdown(ExitCode.BOOKIE_EXCEPTION);
            return;
        } catch (BookieException be) {
            LOG.error("Exception while replaying journals, shutting down", be);
            shutdown(ExitCode.BOOKIE_EXCEPTION);
            return;
        }
        LOG.info("Finished reading journal, starting bookie");
        // start bookie thread
        super.start();

        // After successful bookie startup, register listener for disk
        // error/full notifications.
        ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
        if (indexDirsManager != ledgerDirsManager) {
            indexDirsManager.addLedgerDirsListener(getLedgerDirsListener());
        }

        ledgerStorage.start();

        syncThread.start();
        // set running here.
        // since bookie server use running as a flag to tell bookie server whether it is alive
        // if setting it in bookie thread, the watcher might run before bookie thread.
        running = true;
        try {
            Future<Void> f;
            if (conf.isForceReadOnlyBookie() && conf.isReadOnlyModeEnabled()) {
                readOnly.set(true);
                f = registrar.registerReadOnly();
            } else {
                f = registrar.register();
            }
            f.get();
        } catch (ExecutionException ee) {
            LOG.error("Couldn't register bookie with zookeeper, shutting down", ee.getCause());
            shutdown(ExitCode.ZK_REG_FAIL);
        } catch (InterruptedException e) {
            LOG.error("Interrupted registering with zookeeper");
            shutdown(ExitCode.ZK_REG_FAIL);
        }
    }

    /*
     * Get the DiskFailure listener for the bookie
     */
    private LedgerDirsListener getLedgerDirsListener() {

        return new LedgerDirsListener() {

            @Override
            public void diskFull(File disk) {
                // Nothing needs to be handled here.
            }

            @Override
            public void diskAlmostFull(File disk) {
                // Nothing needs to be handled here.
            }

            @Override
            public void diskFailed(File disk) {
                // Shutdown the bookie on disk failure.
                triggerBookieShutdown(ExitCode.BOOKIE_EXCEPTION);
            }

            @Override
            public void allDisksFull() {
                // Transition to readOnly mode on all disks full
                transitionToReadOnlyMode();
            }

            @Override
            public void fatalError() {
                LOG.error("Fatal error reported by ledgerDirsManager");
                triggerBookieShutdown(ExitCode.BOOKIE_EXCEPTION);
            }

            @Override
            public void diskWritable(File disk) {
                // Transition to writable mode when a disk becomes writable again.
                transitionToWritableMode();
            }

            @Override
            public void diskJustWritable(File disk) {
                // Transition to writable mode when a disk becomes writable again.
                transitionToWritableMode();
            }
        };
    }

    /**
     * Register jmx with parent
     *
     * @param parent parent bk mbean info
     */
    public void registerJMX(BKMBeanInfo parent) {
        try {
            jmxBookieBean = new BookieBean(this);
            BKMBeanRegistry.getInstance().register(jmxBookieBean, parent);

            try {
                jmxLedgerStorageBean = this.ledgerStorage.getJMXBean();
                if (jmxLedgerStorageBean != null) {
                    BKMBeanRegistry.getInstance().register(jmxLedgerStorageBean, jmxBookieBean);
                }
            } catch (Exception e) {
                LOG.warn("Failed to register with JMX for ledger cache", e);
                jmxLedgerStorageBean = null;
            }
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxBookieBean = null;
        }
    }

    /**
     * Unregister jmx
     */
    public void unregisterJMX() {
        try {
            if (jmxLedgerStorageBean != null) {
                BKMBeanRegistry.getInstance().unregister(jmxLedgerStorageBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        try {
            if (jmxBookieBean != null) {
                BKMBeanRegistry.getInstance().unregister(jmxBookieBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxBookieBean = null;
        jmxLedgerStorageBean = null;
    }


    /**
     * Instantiate the ZooKeeper client for the Bookie.
     */
    private ZooKeeper instantiateZookeeperClient(ServerConfiguration conf)
            throws IOException, InterruptedException, KeeperException {
        if (conf.getZkServers() == null) {
            LOG.warn("No ZK servers passed to Bookie constructor so BookKeeper clients won't know about this server!");
            return null;
        }
        ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(conf.getZkTimeout()) {};
        return ZkUtils.createConnectedZookeeperClient(conf.getZkServers(), w);
    }

    /**
     * Transition the bookie from readOnly mode to writable
     */
    @VisibleForTesting
    public void transitionToWritableMode() {
        if (conf.isForceReadOnlyBookie() && conf.isReadOnlyModeEnabled()) {
            return;
        }
        if (!readOnly.compareAndSet(true, false)) {
            return;
        }
        LOG.info("Transitioning Bookie to Writable mode and will serve read/write requests.");
        try {
            Future<Void> f = registrar.register();
            f.get();
        } catch (ExecutionException ee) {
            LOG.error("Error in transition to writable mode."
                      + " Going read only", ee);
            transitionToReadOnlyMode();
            return;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted Exception while transitioning to writable Mode.");
            if (!shuttingdown) {
                transitionToReadOnlyMode();
            }
            return;
        }
    }

    /*
     * Transition the bookie to readOnly mode
     */
    @VisibleForTesting
    public void transitionToReadOnlyMode() {
        if (shuttingdown) {
            return;
        }

        if (!readOnly.compareAndSet(false, true)) {
            return;
        }
        if (!conf.isReadOnlyModeEnabled()) {
            LOG.warn("ReadOnly mode is not enabled. "
                    + "Can be enabled by configuring "
                    + "'readOnlyModeEnabled=true' in configuration."
                    + "Shutting down bookie");
            triggerBookieShutdown(ExitCode.BOOKIE_EXCEPTION);
            return;
        }
        LOG.info("Transitioning Bookie to ReadOnly mode,"
                + " and will serve only read requests from clients!");
        try {
            Future<Void> f = registrar.registerReadOnly();
            f.get();
        } catch (ExecutionException ee) {
            LOG.error("Error in transition to ReadOnly Mode."
                    + " Shutting down", ee);
            triggerBookieShutdown(ExitCode.BOOKIE_EXCEPTION);
            return;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted Exception while transitioning to ReadOnly Mode.");
            return;
        }
    }

    /*
     * Check whether Bookie is writable
     */
    public boolean isReadOnly() {
        return readOnly.get();
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public void run() {
        // bookie thread wait for journal thread
        try {
            // start journals
            for (Journal journal : journals) {
                journal.start();
            }

            // wait until journals quit
            for (Journal journal : journals) {
                journal.join();
            }
        } catch (InterruptedException ie) {
        }
        // if the journal thread quits due to shutting down, it is ok
        if (!shuttingdown) {
            // some error found in journal thread and it quits
            // following add operations to it would hang unit client timeout
            // so we should let bookie server exists
            LOG.error("Journal manager quits unexpectedly.");
            triggerBookieShutdown(ExitCode.BOOKIE_EXCEPTION);
        }
    }

    // Triggering the Bookie shutdown in its own thread,
    // because shutdown can be called from sync thread which would be
    // interrupted by shutdown call.
    AtomicBoolean shutdownTriggered = new AtomicBoolean(false);
    void triggerBookieShutdown(final int exitCode) {
        if (!shutdownTriggered.compareAndSet(false, true)) {
            return;
        }
        LOG.info("Triggering shutdown of Bookie-{} with exitCode {}",
                 conf.getBookiePort(), exitCode);
        BookieThread th = new BookieThread("BookieShutdownTrigger") {
            @Override
            public void run() {
                Bookie.this.shutdown(exitCode);
            }
        };
        th.start();
    }

    // provided a public shutdown method for other caller
    // to shut down bookie gracefully
    public int shutdown() {
        return shutdown(ExitCode.OK);
    }

    // internal shutdown method to let shutdown bookie gracefully
    // when encountering exception
    synchronized int shutdown(int exitCode) {
        try {
            if (running) { // avoid shutdown twice
                // the exitCode only set when first shutdown usually due to exception found
                LOG.info("Shutting down Bookie-{} with exitCode {}",
                         conf.getBookiePort(), exitCode);
                if (this.exitCode == ExitCode.OK) {
                    this.exitCode = exitCode;
                }
                // mark bookie as in shutting down progress
                shuttingdown = true;

                try {
                    registrar.close();
                } catch (IOException ioe) {
                    LOG.error("Failed to shutdown registrar cleanly", ioe);
                }

                // Shutdown journals
                for (Journal journal : journals) {
                    journal.shutdown();
                }

                this.join();
                syncThread.shutdown();

                // Shutdown the EntryLogger which has the GarbageCollector Thread running
                ledgerStorage.shutdown();

                //Shutdown disk checker
                ledgerDirsManager.shutdown();
                if (indexDirsManager != ledgerDirsManager) {
                    indexDirsManager.shutdown();
                }

            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted during shutting down bookie : ", ie);
        } finally {
            // setting running to false here, so watch thread
            // in bookie server know it only after bookie shut down
            running = false;
        }
        return this.exitCode;
    }

    /**
     * Retrieve the ledger descriptor for the ledger which entry should be added to.
     * The LedgerDescriptor returned from this method should be eventually freed with
     * #putHandle().
     *
     * @throws BookieException if masterKey does not match the master key of the ledger
     */
    private LedgerDescriptor getLedgerForEntry(ByteBuf entry, final byte[] masterKey)
            throws IOException, BookieException {
        // Read ledgerId without moving reader index
        final long ledgerId = entry.getLong(entry.readerIndex());

        LedgerDescriptor l = handles.getHandle(ledgerId, masterKey);
        if (masterKeyCache.get(ledgerId) == null) {
            // Force the load into masterKey cache
            byte[] oldValue = masterKeyCache.putIfAbsent(ledgerId, masterKey);
            if (oldValue == null) {
                // new handle, we should add the key to journal ensure we can rebuild
                ByteBuffer bb = ByteBuffer.allocate(8 + 8 + 4 + masterKey.length);
                bb.putLong(ledgerId);
                bb.putLong(METAENTRY_ID_LEDGER_KEY);
                bb.putInt(masterKey.length);
                bb.put(masterKey);
                bb.flip();

                getJournal(ledgerId).logAddEntry(bb, new NopWriteCallback(), null);
            }
        }

        return l;
    }

    /**
     * Add an entry to a ledger as specified by handle.
     */
    private void addEntryInternal(LedgerDescriptor handle, ByteBuf entry, WriteCallback cb, Object ctx)
            throws IOException, BookieException {
        long ledgerId = handle.getLedgerId();
        long entryId = handle.addEntry(entry);

        writeBytes.add(entry.readableBytes());

        if (LOG.isTraceEnabled()) {
            LOG.trace("Adding {}@{}", entryId, ledgerId);
        }

        getJournal(ledgerId).logAddEntry(entry, cb, ctx);
    }

    /**
     * Add entry to a ledger, even if the ledger has previous been fenced. This should only
     * happen in bookie recovery or ledger recovery cases, where entries are being replicates
     * so that they exist on a quorum of bookies. The corresponding client side call for this
     * is not exposed to users.
     */
    public void recoveryAddEntry(ByteBuf entry, WriteCallback cb, Object ctx, byte[] masterKey)
            throws IOException, BookieException {
        try {
            LedgerDescriptor handle = getLedgerForEntry(entry, masterKey);
            synchronized (handle) {
                addEntryInternal(handle, entry, cb, ctx);
            }
        } catch (NoWritableLedgerDirException e) {
            transitionToReadOnlyMode();
            throw new IOException(e);
        } finally {
            entry.release();
        }
    }

    /**
     * Add entry to a ledger.
     * @throws BookieException.LedgerFencedException if the ledger is fenced
     */
    public void addEntry(ByteBuf entry, WriteCallback cb, Object ctx, byte[] masterKey)
            throws IOException, BookieException {
        try {
            LedgerDescriptor handle = getLedgerForEntry(entry, masterKey);
            synchronized (handle) {
                if (handle.isFenced()) {
                    throw BookieException
                            .create(BookieException.Code.LedgerFencedException);
                }

                addEntryInternal(handle, entry, cb, ctx);
            }
        } catch (NoWritableLedgerDirException e) {
            transitionToReadOnlyMode();
            throw new IOException(e);
        } finally {
            entry.release();
        }
    }

    /**
     * Fences a ledger. From this point on, clients will be unable to
     * write to this ledger. Only recoveryAddEntry will be
     * able to add entries to the ledger.
     * This method is idempotent. Once a ledger is fenced, it can
     * never be unfenced. Fencing a fenced ledger has no effect.
     */
    public Future<Boolean> fenceLedger(long ledgerId, byte[] masterKey) throws IOException, BookieException {
        LedgerDescriptor handle = handles.getHandle(ledgerId, masterKey);
        boolean success;
        synchronized (handle) {
            success = handle.setFenced();
        }
        if (success) {
            // fenced first time, we should add the key to journal ensure we can rebuild
            ByteBuffer bb = ByteBuffer.allocate(8 + 8);
            bb.putLong(ledgerId);
            bb.putLong(METAENTRY_ID_FENCE_KEY);
            bb.flip();

            FutureWriteCallback fwc = new FutureWriteCallback();
            if (LOG.isDebugEnabled()) {
                LOG.debug("record fenced state for ledger {} in journal.", ledgerId);
            }
            getJournal(ledgerId).logAddEntry(bb, fwc, null);
            return fwc.getResult();
        } else {
            // already fenced
            return SUCCESS_FUTURE;
        }
    }

    public ByteBuf readEntry(long ledgerId, long entryId)
            throws IOException, NoLedgerException {
        LedgerDescriptor handle = handles.getReadOnlyHandle(ledgerId);
        if (LOG.isTraceEnabled()) {
            LOG.trace("Reading {}@{}", entryId, ledgerId);
        }
        ByteBuf entry = handle.readEntry(entryId);
        readBytes.add(entry.readableBytes());
        return entry;
    }

    @VisibleForTesting
    public LedgerStorage getLedgerStorage() {
        return ledgerStorage;
    }

    // The rest of the code is test stuff
    static class CounterCallback implements WriteCallback {
        int count;

        @Override
        synchronized public void writeComplete(int rc, long l, long e, BookieSocketAddress addr, Object ctx) {
            count--;
            if (count == 0) {
                notifyAll();
            }
        }

        synchronized public void incCount() {
            count++;
        }

        synchronized public void waitZero() throws InterruptedException {
            while (count > 0) {
                wait();
            }
        }
    }

    /**
     * Format the bookie server data
     *
     * @param conf
     *            ServerConfiguration
     * @param isInteractive
     *            Whether format should ask prompt for confirmation if old data
     *            exists or not.
     * @param force
     *            If non interactive and force is true, then old data will be
     *            removed without confirm prompt.
     * @return Returns true if the format is success else returns false
     */
    public static boolean format(ServerConfiguration conf,
            boolean isInteractive, boolean force) {
        for (File journalDir : conf.getJournalDirs()) {
            if (journalDir.exists() && journalDir.isDirectory() && journalDir.list().length != 0) {
                try {
                    boolean confirm = false;
                    if (!isInteractive) {
                        // If non interactive and force is set, then delete old
                        // data.
                        if (force) {
                            confirm = true;
                        } else {
                            confirm = false;
                        }
                    } else {
                        confirm = IOUtils.confirmPrompt("Are you sure to format Bookie data..?");
                    }

                    if (!confirm) {
                        LOG.error("Bookie format aborted!!");
                        return false;
                    }
                } catch (IOException e) {
                    LOG.error("Error during bookie format", e);
                    return false;
                }
            }
            if (!cleanDir(journalDir)) {
                LOG.error("Formatting journal directory failed");
                return false;
            }
        }

        File[] ledgerDirs = conf.getLedgerDirs();
        for (File dir : ledgerDirs) {
            if (!cleanDir(dir)) {
                LOG.error("Formatting ledger directory " + dir + " failed");
                return false;
            }
        }

        // Clean up index directories if they are separate from the ledger dirs
        File[] indexDirs = conf.getIndexDirs();
        if (null != indexDirs) {
            for (File dir : indexDirs) {
                if (!cleanDir(dir)) {
                    LOG.error("Formatting ledger directory " + dir + " failed");
                    return false;
                }
            }
        }

        LOG.info("Bookie format completed successfully");
        return true;
    }

    private static boolean cleanDir(File dir) {
        if (dir.exists()) {
            for (File child : dir.listFiles()) {
                boolean delete = FileUtils.deleteQuietly(child);
                if (!delete) {
                    LOG.error("Not able to delete " + child);
                    return false;
                }
            }
        } else if (!dir.mkdirs()) {
            LOG.error("Not able to create the directory " + dir);
            return false;
        }
        return true;
    }

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args)
            throws IOException, InterruptedException, BookieException, KeeperException {
        Bookie b = new Bookie(new ServerConfiguration());
        b.start();
        CounterCallback cb = new CounterCallback();
        long start = MathUtils.now();
        for (int i = 0; i < 100000; i++) {
            ByteBuf buff = Unpooled.buffer(1024);
            buff.writeLong(1);
            buff.writeLong(i);
            cb.incCount();
            b.addEntry(buff, cb, null, new byte[0]);
        }
        cb.waitZero();
        long end = MathUtils.now();
        System.out.println("Took " + (end-start) + "ms");
    }

    /**
     * Returns exit code - cause of failure
     *
     * @return {@link ExitCode}
     */
    public int getExitCode() {
        return exitCode;
    }

    Journal getJournal(long ledgerId) {
        return journals.get(MathUtils.signSafeMod(ledgerId, journals.size()));
    }
}
