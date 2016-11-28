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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Date;
import java.util.Map;

public class JsonFileReporter implements Reporter {

    private final Path jsonFilePath;

    public JsonFileReporter(Path jsonFilePath) {
        this.jsonFilePath = jsonFilePath;
    }

    @Override
    public void report(Map<String, DataSketchesCounter> counters,
            Map<String, DataSketchesGauge<? extends Number>> gauges, Map<String, DataSketchesOpStatsLogger> opStats)
            throws IOException {

        Path tempFilePath = Paths.get(jsonFilePath.toString() + ".temp");
        PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(tempFilePath.toFile())));

        out.println("{");

        out.format("  \"reportDate\" : \"%s\",\n", new Date());
        out.format("  \"reportTimestamp\" : %d,\n", System.currentTimeMillis());
        out.format("  \"processId\" : %s", ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);

        counters.forEach((name, counter) -> {
            out.format(",\n  \"%s\" : %s", name, counter.get());
        });

        gauges.forEach((name, gauge) -> {
            out.format(",\n  \"%s\" : %s", name, gauge.getSample());
        });

        opStats.forEach((name, opStat) -> {
            out.format(",\n  \"%s\" : {\n         ", name);
            out.format("  \"rate\" : %.1f, ", opStat.getRate());
            out.format("  \"count\" : %d, ", opStat.getCount());
            out.format("  \"failCount\" : %d, ", opStat.getFailCount());
            out.format("  \"min\" : %.1f, ", opStat.getMin());
            out.format("  \"pct50\" : %.1f, ", opStat.getMedian());
            out.format("  \"pct95\" : %.1f, ", opStat.getPct95());
            out.format("  \"pct99\" : %.1f, ", opStat.getPct99());
            out.format("  \"pct999\" : %.1f, ", opStat.getPct999());
            out.format("  \"pct9999\" : %.1f, ", opStat.getPct9999());
            out.format("  \"max\" : %.1f }", opStat.getMax());
        });

        out.println("\n}");
        out.close();

        Files.move(tempFilePath, jsonFilePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

}
