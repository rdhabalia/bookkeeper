package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

class LongWrapper {

    final byte[] array = new byte[8];

    public void set(long value) {
        ArrayUtil.setLong(array, 0, value);
    }

    public long getValue() {
        return ArrayUtil.getLong(array, 0);
    }

    public static LongWrapper get() {
        return RECYCLER.get();
    }

    public static LongWrapper get(long value) {
        LongWrapper lp = RECYCLER.get();
        ArrayUtil.setLong(lp.array, 0, value);
        return lp;
    }

    public void recycle() {
        RECYCLER.recycle(this, handle);
    }

    private static Recycler<LongWrapper> RECYCLER = new Recycler<LongWrapper>() {
        @Override
        protected LongWrapper newObject(Handle handle) {
            return new LongWrapper(handle);
        }
    };

    private final Handle handle;

    private LongWrapper(Handle handle) {
        this.handle = handle;
    }
}