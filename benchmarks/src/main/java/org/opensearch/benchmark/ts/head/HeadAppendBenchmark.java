/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.ts.head;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.ts.head.Head;
import org.opensearch.ts.head.HeadAppender;
import org.opensearch.ts.model.Labels;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.Random;

@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class HeadAppendBenchmark {

    @Param({ "10000", "100000" })
    private int numSeries; // numbe rof unique series

    @Param({ "10000"})
    private int sampleBatchSize; // number of samples to append in one batch

    @Param({ "12" })
    private int labelsPerSeries; // number of labels per series (label is k:v pair)

    @Param({ "400", "1200" })
    private int labelLengthBytes; // approx bytes length of all labels

    @Param({ "0.05", "0.5" })
    private double labelOverlapFactor; // fraction of total labels in the shared pool relative to total (smaller => more overlap)


    public Head head;

    private final List<Labels> labelsList = new ArrayList<>();

    private final Random random = new Random(42);

    private long timestamp = 0L; // start timestamp for all samples
    private double value = 0.0; // start value for all samples
    private int batch = 0;

    @Setup
    public void setup() throws IOException {
        Path headDir = Files.createTempDirectory("head-benchmark");
        head = new Head(headDir);

        int labelsUpperBound = numSeries * labelsPerSeries;

        // Size of shared label pool controls overlap frequency:
        // Smaller pool = more overlap, larger pool = less overlap
        int sharedPoolSize = (int) Math.max(labelsPerSeries, labelsUpperBound * labelOverlapFactor);

        // Generate shared pool of unique label pairs (key-value)
        List<String[]> sharedLabelPool = new ArrayList<>(sharedPoolSize);
        int avgLabelLength = labelLengthBytes / labelsPerSeries / 2;
        for (int i = 0; i < sharedPoolSize; i++) {
            String key = generateLabelString("k", i, 0, avgLabelLength);
            String value = generateLabelString("v", i, 0, avgLabelLength);
            sharedLabelPool.add(new String[] { key, value });
        }

        for (int seriesIndex = 0; seriesIndex < numSeries; seriesIndex++) {
            List<String> labelPairs = new ArrayList<>(labelsPerSeries * 2);

            // Pick (numLabels - 1) labels randomly from shared pool to create overlap
            for (int labelIndex = 0; labelIndex < labelsPerSeries - 1; labelIndex++) {
                String[] label = sharedLabelPool.get(random.nextInt(sharedPoolSize));
                labelPairs.add(label[0]);
                labelPairs.add(label[1]);
            }

            // Add one unique label per series to ensure uniqueness
            String uniqueKey = generateLabelString("unique_k", 0, seriesIndex, avgLabelLength);
            String uniqueValue = generateLabelString("unique_v", 0, seriesIndex, avgLabelLength);
            labelPairs.add(uniqueKey);
            labelPairs.add(uniqueValue);

            labelsList.add(Labels.fromStrings(labelPairs.toArray(new String[0])));
        }
    }


    @Benchmark
    public void appendHead(Blackhole blackhole) throws IOException {
        HeadAppender appender = head.newAppender();
        // append a point to each series
        for (int i = 0; i < sampleBatchSize; i++) {
            blackhole.consume(appender.append(-1, labelsList.get((batch * sampleBatchSize + i) % labelsList.size()), timestamp, value++));
        }
        batch++;
        timestamp += 10000L; // 10s step
        appender.commit();
    }

    /**
     * Generate a label string of approx targetLength bytes
     */
    private String generateLabelString(String prefix, int labelIndex, int seriesIndex, int targetLength) {
        String base = prefix + labelIndex + "_" + seriesIndex;
        int baseLength = base.length();

        if (baseLength > targetLength) {
            // Truncate if base string is longer than target
            return base.substring(0, targetLength);
        }

        // Pad with 'a' to reach target length
        StringBuilder sb = new StringBuilder(base);
        while (sb.length() < targetLength) {
            sb.append((char) random.nextInt('a', 'z' + 1));
        }
        return sb.toString();
    }
}
