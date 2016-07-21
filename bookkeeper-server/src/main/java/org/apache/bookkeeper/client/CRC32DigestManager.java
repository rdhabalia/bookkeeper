package org.apache.bookkeeper.client;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.zip.CRC32;

/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

class CRC32DigestManager extends DigestManager {

    private static final Method updateByteBuffer;
    private static final Method updateBytes;

    static {
        // Access CRC32 class private native methods to compute the crc on the ByteBuf direct memory,
        // without necessity to convert to a nio ByteBuffer.
        Method updateByteBufferMethod = null;
        Method updateBytesMethod = null;
        try {
            updateByteBufferMethod = CRC32.class.getDeclaredMethod("updateByteBuffer", int.class, long.class, int.class,
                    int.class);
            updateByteBufferMethod.setAccessible(true);

            updateBytesMethod = CRC32.class.getDeclaredMethod("updateBytes", int.class, byte[].class, int.class,
                    int.class);
            updateBytesMethod.setAccessible(true);
        } catch (NoSuchMethodException | SecurityException e) {
            updateByteBufferMethod = null;
            updateBytesMethod = null;
        }

        updateByteBuffer = updateByteBufferMethod;
        updateBytes = updateBytesMethod;
    }

    public CRC32DigestManager(long ledgerId) {
        super(ledgerId);
    }

    @Override
    int getMacCodeLength() {
        return 8;
    }

    @Override
    Digest getDigest() {
        if (updateByteBuffer != null) {
            return DirectCRC32Digest.RECYCLER.get();
        } else {
            return StandardCRC32Digest.RECYCLER.get();
        }
    }

    private static class StandardCRC32Digest implements Digest {
        private final Handle recyclerHandle;
        private final CRC32 crc;

        public StandardCRC32Digest(Handle recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
            this.crc = new CRC32();
        }

        @Override
        public void getValue(ByteBuf buf) {
            buf.writeLong(crc.getValue());
        }

        @Override
        public void update(ByteBuf data) {
            crc.update(data.nioBuffer());
        }

        @Override
        public void update(ByteBuf data, int index, int length) {
            crc.update(data.nioBuffer(index, length));
        }

        @Override
        public void recycle() {
            crc.reset();
            RECYCLER.recycle(this, recyclerHandle);
        }

        private static final Recycler<StandardCRC32Digest> RECYCLER = new Recycler<StandardCRC32Digest>() {
            protected StandardCRC32Digest newObject(Recycler.Handle handle) {
                return new StandardCRC32Digest(handle);
            }
        };
    }

    private static class DirectCRC32Digest implements Digest {
        private final Handle recyclerHandle;
        private int crcValue;

        public DirectCRC32Digest(Handle recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
            this.crcValue = 0;
        }

        @Override
        public void getValue(ByteBuf buf) {
            buf.writeLong(crcValue & 0xffffffffL);
        }

        @Override
        public void update(ByteBuf data) {
            update(data, data.readerIndex(), data.readableBytes());
        }

        @Override
        public void update(ByteBuf data, int index, int length) {
            try {
                if (data.hasMemoryAddress()) {
                    crcValue = (int) updateByteBuffer.invoke(null, crcValue, data.memoryAddress(), index, length);
                } else if (data.hasArray()) {
                    crcValue = (int) updateBytes.invoke(null, crcValue, data.array(), data.arrayOffset() + index,
                            length);
                } else {
                    byte[] b = new byte[length];
                    data.getBytes(index, b, 0, length);
                    crcValue = (int) updateBytes.invoke(null, crcValue, b, 0, b.length);
                }
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void recycle() {
            crcValue = 0;
            RECYCLER.recycle(this, recyclerHandle);
        }

        private static final Recycler<DirectCRC32Digest> RECYCLER = new Recycler<DirectCRC32Digest>() {
            protected DirectCRC32Digest newObject(Recycler.Handle handle) {
                return new DirectCRC32Digest(handle);
            }
        };
    }

}
