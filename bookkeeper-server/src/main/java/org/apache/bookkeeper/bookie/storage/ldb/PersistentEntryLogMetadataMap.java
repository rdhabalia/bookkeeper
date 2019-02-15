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
package org.apache.bookkeeper.bookie.storage.ldb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.Map.Entry;
import java.util.function.BiConsumer;

import org.apache.bookkeeper.bookie.EntryLogMetadata;
import org.apache.bookkeeper.bookie.EntryLogMetadataMap;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.CloseableIterator;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory.DbConfigType;
import org.apache.bookkeeper.conf.ServerConfiguration;

import io.netty.util.concurrent.FastThreadLocal;

/**
 * Persistent entryLogMetadata-map that stores entry-loggers metadata into
 * rocksDB
 *
 */
public class PersistentEntryLogMetadataMap implements EntryLogMetadataMap {

    // persistent Rocksdb to store metadata-map
    private final KeyValueStorage metadataMapDB;

    private static final FastThreadLocal<ByteArrayOutputStream> baos = new FastThreadLocal<ByteArrayOutputStream>() {
        @Override
        protected ByteArrayOutputStream initialValue() {
            return new ByteArrayOutputStream();
        }
    };
    private static final FastThreadLocal<ByteArrayInputStream> bais = new FastThreadLocal<ByteArrayInputStream>() {
        @Override
        protected ByteArrayInputStream initialValue() {
            return new ByteArrayInputStream(new byte[1]);
        }
    };
    private static final FastThreadLocal<DataOutputStream> dataos = new FastThreadLocal<DataOutputStream>() {
        @Override
        protected DataOutputStream initialValue() {
            return new DataOutputStream(baos.get());
        }
    };
    private static final FastThreadLocal<DataInputStream> datais = new FastThreadLocal<DataInputStream>() {
        @Override
        protected DataInputStream initialValue() {
            return new DataInputStream(bais.get());
        }
    };

    public PersistentEntryLogMetadataMap(String path, ServerConfiguration conf) throws IOException {
        String metadataPath = FileSystems.getDefault().getPath(path, "metadata").toFile().toString();
        metadataMapDB = KeyValueStorageRocksDB.factory.newKeyValueStorage(metadataPath, DbConfigType.Small, conf);
    }

    @Override
    public boolean containsKey(long entryLogId) throws IOException {
        LongWrapper key = LongWrapper.get(entryLogId);
        try {
            boolean isExist = metadataMapDB.get(key.array) != null;
            return isExist;
        } finally {
            key.recycle();
        }
    }

    @Override
    public void put(long entryLogId, EntryLogMetadata entryLogMeta) throws IOException {
        LongWrapper key = LongWrapper.get(entryLogId);
        try {
            baos.get().reset();
            entryLogMeta.serialize(dataos.get());
            dataos.get().flush();
            metadataMapDB.put(key.array, baos.get().toByteArray());
        } finally {
            key.recycle();
        }

    }

    @Override
    public void forEach(BiConsumer<Long, EntryLogMetadata> action) throws IOException {
        CloseableIterator<Entry<byte[], byte[]>> iterator = metadataMapDB.iterator();
        try {
            while (iterator.hasNext()) {
                Entry<byte[], byte[]> entry = iterator.next();
                long entryLogId = ArrayUtil.getLong(entry.getKey(), 0);
                ByteArrayInputStream localBais = bais.get();
                DataInputStream localDatais = datais.get();
                if (localBais.available() < entry.getValue().length) {
                    localBais.close();
                    localDatais.close();
                    ByteArrayInputStream newBais = new ByteArrayInputStream(entry.getValue());
                    bais.set(newBais);
                    datais.set(new DataInputStream(newBais));
                } else {
                    localBais.read(entry.getValue(), 0, entry.getValue().length);
                }
                localBais.reset();
                localDatais.reset();
                action.accept(entryLogId, EntryLogMetadata.deserialize(datais.get()));
            }
        } finally {
            iterator.close();
        }
    }

    @Override
    public void remove(long entryLogId) throws IOException {
        LongWrapper key = LongWrapper.get(entryLogId);
        try {
            metadataMapDB.delete(key.array);
        } finally {
            key.recycle();
        }
    }

    @Override
    public int size() throws IOException {
        return (int) metadataMapDB.count();
    }

    @Override
    public void close() throws IOException {
        metadataMapDB.close();
    }

}
