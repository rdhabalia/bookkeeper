package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ReadCacheTest {

    @Test
    public void simple() {
        ReadCache cache = new ReadCache(10 * 1024);

        assertEquals(0, cache.count());
        assertEquals(0, cache.size());

        ByteBuf entry = Unpooled.wrappedBuffer(new byte[1024]);
        cache.put(1, 0, entry);

        assertEquals(1, cache.count());
        assertEquals(1024, cache.size());

        assertEquals(entry, cache.get(1, 0));
        assertNull(cache.get(1, 1));

        for (int i = 1; i < 10; i++) {
            cache.put(1, i, entry);
        }

        assertEquals(10, cache.count());
        assertEquals(10 * 1024, cache.size());

        cache.put(1, 10, entry);

        // First half of entries will have been evicted
        for (int i = 0; i < 5; i++) {
            assertNull(cache.get(1, i));
        }

        for (int i = 5; i < 11; i++) {
            assertEquals(entry, cache.get(1, i));
        }

        cache.close();
    }
}
