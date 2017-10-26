package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

class LongWrapper {

    final byte[] array = new byte[8];

    private void reset() {
        this.set(-1L);
    }
    
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
        this.reset();
        handle.recycle(this);
    }

    private static Recycler<LongWrapper> RECYCLER = new Recycler<LongWrapper>() {
        @Override
        protected LongWrapper newObject(Handle<LongWrapper> handle) {
            return new LongWrapper(handle);
        }
    };

    private final Handle<LongWrapper> handle;

    private LongWrapper(Handle<LongWrapper> handle) {
        this.handle = handle;
    }
}
