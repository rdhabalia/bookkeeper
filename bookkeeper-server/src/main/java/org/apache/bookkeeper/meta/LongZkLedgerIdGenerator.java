package org.apache.bookkeeper.meta;

import java.io.IOException;
import java.util.Formatter;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongZkLedgerIdGenerator implements LedgerIdGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(LongZkLedgerIdGenerator.class);
    private ZooKeeper zk;
    private String ledgerIdGenPath;
    private ZkLedgerIdGenerator shortIdGen;

    public LongZkLedgerIdGenerator(ZooKeeper zk, String ledgersPath, String idGenZnodeName, ZkLedgerIdGenerator shortIdGen) {
        this.zk = zk;
        if (StringUtils.isBlank(idGenZnodeName)) {
            this.ledgerIdGenPath = ledgersPath;
        } else {
            this.ledgerIdGenPath = ledgersPath + "/" + idGenZnodeName;
        }
        this.shortIdGen = shortIdGen;
    }

    private void generateLongLedgerIdLowBits(final String ledgerPrefix, Long highBits, final GenericCallback<Long> cb) throws KeeperException, InterruptedException, IOException {
        String highPath = ledgerPrefix + formatHalfId(highBits.intValue());
        try (ZkLedgerIdGenerator subIdGenerator = new ZkLedgerIdGenerator(zk, highPath, null);) {
            subIdGenerator.generateLedgerId(new GenericCallback<Long>(){
                    @Override
                    public void operationComplete(int rc, Long result) {
                        if(rc == BKException.Code.OK) {
                            cb.operationComplete(rc, (highBits << 32) | result);
                        }
                        else if(rc == BKException.Code.LedgerIdOverflowException) {
                            // Lower bits are full. Need to expand and create another HOB node.
                            try {
                                Long newHighBits = highBits + 1;
                                generateHOBPath(ledgerPrefix, newHighBits.intValue(), cb);
                            }
                            catch (KeeperException e) {
                                LOG.error("Failed to create long ledger ID path", e);
                                cb.operationComplete(BKException.Code.ZKException, null);
                            }
                            catch (InterruptedException e) {
                                LOG.error("Failed to create long ledger ID path", e);
                                cb.operationComplete(BKException.Code.InterruptedException, null);
                            } catch (IOException e) {
                                LOG.error("Failed to create long ledger ID path", e);
                                cb.operationComplete(BKException.Code.IllegalOpException, null);
                            }

                        }
                    }

                });
        }
    }

    /**
     * Formats half an ID as 10-character 0-padded string
     * @param i - 32 bits of the ID to format
     * @return a 10-character 0-padded string.
     */
    private String formatHalfId(Integer i) {
        StringBuilder sb = new StringBuilder();
        try (Formatter fmt = new Formatter(sb, Locale.US);) {
            fmt.format("%010d", i);
            return sb.toString();
        }
    }

    private void generateHOBPath(String ledgerPrefix, int hob, final GenericCallback<Long> cb) throws KeeperException, InterruptedException, IOException {
        try {
            LOG.debug("Creating HOB path: {}", ledgerPrefix + formatHalfId(hob));
            zk.create(ledgerPrefix + formatHalfId(hob), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        catch(KeeperException.NodeExistsException e) {
            // It's fine if we lost a race to create the node (NodeExistsException).
            // All other exceptions should continue unwinding.
            LOG.warn("Tried to create High-order-bits node, but it already existed!", e);
        }
        generateLongLedgerId(cb); // Try again.
    }

    private void generateLongLedgerId(final GenericCallback<Long> cb) throws KeeperException, InterruptedException, IOException {
        final String hobPrefix = "HOB-";
        final String ledgerPrefix = this.ledgerIdGenPath + "/" + hobPrefix;
        List<String> children = zk.getChildren(ledgerIdGenPath, false);

        Optional<Long> largest = children.stream()
            .map((t) -> {
                    try {
                        return Long.parseLong(t.replace(hobPrefix, ""));
                    }
                    catch(NumberFormatException e) {
                        return null;
                    }
                })
            .filter((t) -> t != null)
            .reduce(Math::max);

        // If we didn't get any valid IDs from the directory, Start at 0000000001
        if(!largest.isPresent()) {
            //HOB-0000000001;
            generateHOBPath(ledgerPrefix, 1, cb);
        }
        else {
            // Found the largest.
            // Get the low-order bits.
            final Long highBits = largest.get();
            generateLongLedgerIdLowBits(ledgerPrefix, highBits, cb);
        }
    }

    private void createPathAndGenerateLongLedgerId(final GenericCallback<Long> cb, String createPath) {
        ZkUtils.asyncCreateFullPathOptimistic(zk, ledgerIdGenPath, new byte[0], Ids.OPEN_ACL_UNSAFE,
                                              CreateMode.PERSISTENT, new StringCallback() {
                                                      @Override
                                                      public void processResult(int rc, String path, Object ctx, String name) {
                                                          try {
                                                              generateLongLedgerId(cb);
                                                          } catch (KeeperException e) {
                                                              LOG.error("Failed to create long ledger ID path", e);
                                                              cb.operationComplete(BKException.Code.ZKException, null);
                                                          } catch (InterruptedException e) {
                                                              LOG.error("Failed to create long ledger ID path", e);
                                                              cb.operationComplete(BKException.Code.InterruptedException, null);
                                                          } catch (IOException e) {
                                                              LOG.error("Failed to create long ledger ID path", e);
                                                              cb.operationComplete(BKException.Code.IllegalOpException, null);
                                                          }
                                                      }
                                                  }, null);
    }

    @Override
    public void generateLedgerId(final GenericCallback<Long> cb) {
        try {
            if(zk.exists(ledgerIdGenPath, false) == null) {
                // We've not moved onto 63-bit ledgers yet.
                shortIdGen.generateLedgerId(new GenericCallback<Long>(){
                        @Override
                        public void operationComplete(int rc, Long result) {
                            if(rc == BKException.Code.LedgerIdOverflowException) {
                                // 31-bit IDs overflowed. Start using 63-bit ids.
                                createPathAndGenerateLongLedgerId(cb, ledgerIdGenPath);
                            }
                            else {
                                // 31-bit Generation worked OK, or had some other
                                // error that we will pass on.
                                cb.operationComplete(rc, result);
                            }
                        }
                    });
            }
            else {
                // We've already started generating 63-bit ledger IDs.
                // Keep doing that.
                generateLongLedgerId(cb);
            }
        } catch (KeeperException e) {
            LOG.error("Failed to create long ledger ID path", e);
            cb.operationComplete(BKException.Code.ZKException, null);
        }
        catch (InterruptedException e) {
            LOG.error("Failed to create long ledger ID path", e);
            cb.operationComplete(BKException.Code.InterruptedException, null);
        }
        catch (IOException e) {
            LOG.error("Failed to create long ledger ID path", e);
            cb.operationComplete(BKException.Code.IllegalOpException, null);
        }
    }

    @Override
    public void close() throws IOException {}

}
