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

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.DefaultThreadFactory;

public class DataSketchesMetricsProvider implements StatsProvider {

    final ConcurrentMap<String, DataSketchesCounter> counters = new ConcurrentSkipListMap<>();
    final ConcurrentMap<String, DataSketchesGauge<? extends Number>> gauges = new ConcurrentSkipListMap<>();
    final ConcurrentMap<String, DataSketchesOpStatsLogger> opStats = new ConcurrentSkipListMap<>();

    List<Reporter> reporters = new ArrayList<>();

    private ScheduledExecutorService executor;

    private static final String DATASKETCHES_METRICS_UPDATE_INTERVAL_SECONDS = "dataSketchesMetricsUpdateIntervalSeconds";

    private static final String DATASKETCHES_METRICS_JSON_FILE_REPORTER = "dataSketchesMetricsJsonFileReporter";

    private static final int DEFAULT_DATASKETCHES_METRICS_UPDATE_INTERVAL_SECONDS = 60;

    @Override
    public void start(Configuration conf) {
        int updateIntervalInSeconds = conf.getInt(DATASKETCHES_METRICS_UPDATE_INTERVAL_SECONDS,
                DEFAULT_DATASKETCHES_METRICS_UPDATE_INTERVAL_SECONDS);

        String jsonFileReporterPath = conf.getString(DATASKETCHES_METRICS_JSON_FILE_REPORTER);
        if (jsonFileReporterPath != null) {
            JsonFileReporter jsonFileReporter = new JsonFileReporter(Paths.get(jsonFileReporterPath));
            log.info("Collecting stats into {} every {} seconds", jsonFileReporterPath, updateIntervalInSeconds);

            reporters.add(jsonFileReporter);
        }

        executor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("metrics"));

        executor.scheduleAtFixedRate(() -> {
            opStats.forEach((name, metric) -> {
                metric.updateRate();
            });

            reporters.forEach(reporter -> {
                try {
                    reporter.report(counters, gauges, opStats);
                } catch (Throwable e) {
                    log.warn("Failed to report stats: {}", e.getMessage(), e);
                }
            });
        }, 5, updateIntervalInSeconds, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        executor.shutdown();
    }

    @Override
    public StatsLogger getStatsLogger(String name) {
        return new DataSketchesStatsLogger(this, name);
    }

    private static final Logger log = LoggerFactory.getLogger(DataSketchesMetricsProvider.class);
}
