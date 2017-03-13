/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.bokkeeper.stats.datasketches;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.StampedLock;

import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.stats.OpStatsLogger;

import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesSketchBuilder;
import com.yahoo.sketches.quantiles.DoublesUnion;
import com.yahoo.sketches.quantiles.DoublesUnionBuilder;

import io.netty.util.concurrent.FastThreadLocal;

public class DataSketchesOpStatsLogger implements OpStatsLogger {
    private volatile ThreadLocalAccessor current;
    private volatile ThreadLocalAccessor replacement;

    private volatile DoublesSketch result;
    private double successRate = 0.0;
    private long successCount = 0;
    private long failCount = 0;
    private long lastRateUpdateTime = System.nanoTime();
    private final LongAdder successCountAdder = new LongAdder();
    private final LongAdder failCountAdder = new LongAdder();

    DataSketchesOpStatsLogger() {
        this.current = new ThreadLocalAccessor();
        this.replacement = new ThreadLocalAccessor();
    }

    @Override
    public void registerFailedEvent(long eventLatency, TimeUnit unit) {
        failCountAdder.increment();
    }

    @Override
    public void registerSuccessfulEvent(long eventLatency, TimeUnit unit) {
        successCountAdder.increment();
        LocalData localData = current.localData.get();

        long stamp = localData.lock.readLock();
        try {
            localData.sketch.update(unit.toMicros(eventLatency) / 1000.0);
        } finally {
            localData.lock.unlockRead(stamp);
        }
    }

    @Override
    public void registerSuccessfulValue(long value) {
        successCountAdder.increment();

        LocalData localData = current.localData.get();

        long stamp = localData.lock.readLock();
        try {
            localData.sketch.update(value);
        } finally {
            localData.lock.unlockRead(stamp);
        }
    }

    @Override
    public OpStatsData toOpStatsData() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    public void updateRate() {
        // Swap current with replacement
        ThreadLocalAccessor local = current;
        current = replacement;
        replacement = local;

        final DoublesUnion aggregate = new DoublesUnionBuilder().build();
        local.map.forEach((localData, b) -> {
            long stamp = localData.lock.writeLock();
            try {
                aggregate.update(localData.sketch);
                localData.sketch.reset();
            } finally {
                localData.lock.unlockWrite(stamp);
            }
        });

        result = aggregate.getResultAndReset();

        successCount = successCountAdder.sumThenReset();
        successRate = getRate(successCount);
        failCount = failCountAdder.sumThenReset();
        lastRateUpdateTime = System.nanoTime();
    }

    private double getRate(long value) {
        double durationSeconds = (System.nanoTime() - lastRateUpdateTime) / 1e9;
        // Cap to 3 decimal digits
        return ((long) (1000 * (value / durationSeconds))) / 1000.0;
    }

    public double getRate() {
        return successRate;
    }

    public long getCount() {
        return successCount;
    }

    public long getFailCount() {
        return failCount;
    }

    public double getMin() {
        return result != null ? result.getMinValue() : 0;
    }

    public double getMedian() {
        return result != null ? result.getQuantile(0.50) : 0;
    }

    public double getPct95() {
        return result != null ? result.getQuantile(0.95) : 0;
    }

    public double getPct99() {
        return result != null ? result.getQuantile(0.99) : 0;
    }

    public double getPct999() {
        return result != null ? result.getQuantile(0.999) : 0;
    }

    public double getPct9999() {
        return result != null ? result.getQuantile(0.9999) : 0;
    }

    public double getMax() {
        return result != null ? result.getMaxValue() : 0;
    }

    private static class LocalData {
        private final DoublesSketch sketch = new DoublesSketchBuilder().build();
        private final StampedLock lock = new StampedLock();
    }

    private static class ThreadLocalAccessor {
        private final Map<LocalData, Boolean> map = new ConcurrentHashMap<>();
        private final FastThreadLocal<LocalData> localData = new FastThreadLocal<LocalData>() {

            @Override
            protected LocalData initialValue() throws Exception {
                LocalData localData = new LocalData();
                map.put(localData, Boolean.TRUE);
                return localData;
            }

            @Override
            protected void onRemoval(LocalData value) throws Exception {
                map.remove(value);
            }
        };
    }
}
