package org.apache.bookkeeper.meta;

import org.apache.zookeeper.ZooKeeper;

public class LongHierarchicalLedgerManagerFactory extends HierarchicalLedgerManagerFactory {

    public static final String NAME = "longhierarchical";

    @Override
    public LedgerManager newLedgerManager() {
        return new LongHierarchicalLedgerManager(conf, zk);
    }
}
