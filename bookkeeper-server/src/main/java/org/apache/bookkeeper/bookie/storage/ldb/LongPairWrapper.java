package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

class LongPairWrapper {

    final byte[] array = new byte[16];

    public void set(long first, long second) {
        ArrayUtil.setLong(array, 0, first);
        ArrayUtil.setLong(array, 8, second);
    }

    public long getFirst() {
        return ArrayUtil.getLong(array, 0);
    }

    public long getSecond() {
        return ArrayUtil.getLong(array, 8);
    }

    public static LongPairWrapper get(long first, long second) {
        LongPairWrapper lp = RECYCLER.get();
        ArrayUtil.setLong(lp.array, 0, first);
        ArrayUtil.setLong(lp.array, 8, second);
        return lp;
    }

    public void recycle() {
        RECYCLER.recycle(this, handle);
    }

    private static Recycler<LongPairWrapper> RECYCLER = new Recycler<LongPairWrapper>() {
        @Override
        protected LongPairWrapper newObject(Handle handle) {
            return new LongPairWrapper(handle);
        }
    };

    private final Handle handle;

    private LongPairWrapper(Handle handle) {
        this.handle = handle;
    }
}