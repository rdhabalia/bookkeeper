package org.apache.bookkeeper.meta;

import org.apache.zookeeper.ZooKeeper;

public class LongHierarchicalLedgerManagerFactory extends HierarchicalLedgerManagerFactory {

    public static final String NAME = "longhierarchical";

    @Override
    public LedgerManager newLedgerManager() {
        return new LongHierarchicalLedgerManager(conf, zk);
    }

    @Override
    public LedgerIdGenerator newLedgerIdGenerator() {
        ZkLedgerIdGenerator subIdGenerator = new ZkLedgerIdGenerator(zk, conf.getZkLedgersRootPath(), HierarchicalLedgerManager.IDGEN_ZNODE);
        return new LongZkLedgerIdGenerator(zk, conf.getZkLedgersRootPath(), LongHierarchicalLedgerManager.IDGEN_ZNODE, subIdGenerator);
    }
}
