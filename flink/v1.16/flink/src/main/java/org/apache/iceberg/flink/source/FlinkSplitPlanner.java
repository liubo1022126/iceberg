/*
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
 */
package org.apache.iceberg.flink.source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.Scan;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class FlinkSplitPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkSplitPlanner.class);

  private FlinkSplitPlanner() {}

  static FlinkInputSplit[] planInputSplits(
      Table table, ScanContext context, ExecutorService workerPool) {
    try (CloseableIterable<CombinedScanTask> tasksIterable =
        planTasks(table, context, workerPool)) {
      List<CombinedScanTask> tasks = Lists.newArrayList(tasksIterable);
      FlinkInputSplit[] splits = new FlinkInputSplit[tasks.size()];
      boolean exposeLocality = context.exposeLocality();

      LOG.info("begin to build FlinkInputSplit[].");
      Tasks.range(tasks.size())
          .stopOnFailure()
          .executeWith(exposeLocality ? workerPool : null)
          .run(
              index -> {
                CombinedScanTask task = tasks.get(index);
                String[] hostnames = null;
                if (exposeLocality) {
                  hostnames = Util.blockLocationsMutiFs(table.io(), task);
                }
                splits[index] = new FlinkInputSplit(index, task, hostnames);
              });
      LOG.info("end to build FlinkInputSplit[].");
      return splits;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to process tasks iterable", e);
    }
  }

  /** This returns splits for the FLIP-27 source */
  public static List<IcebergSourceSplit> planIcebergSourceSplits(
      Table table, ScanContext context, ExecutorService workerPool) {
    try (CloseableIterable<CombinedScanTask> tasksIterable =
        planTasks(table, context, workerPool)) {
      return Lists.newArrayList(
          CloseableIterable.transform(tasksIterable, IcebergSourceSplit::fromCombinedScanTask));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to process task iterable: ", e);
    }
  }

  static IncrementalAppendScan betweenStrategyPadding(
      IncrementalAppendScan scan, Long startSnapshotId, Long endSnapshotId, String betweenMode) {
    IncrementalAppendScan paddingScan = scan;

    if (BetweenMode.l0r1.toString().equals(betweenMode)) {
      if (startSnapshotId != null) {
        paddingScan = paddingScan.fromSnapshotExclusive(startSnapshotId);
      }
      if (endSnapshotId != null) {
        paddingScan = paddingScan.toSnapshot(endSnapshotId);
      }
    }
    if (BetweenMode.l1r1.toString().equals(betweenMode)) {
      if (startSnapshotId != null) {
        paddingScan = paddingScan.fromSnapshotInclusive(startSnapshotId);
      }
      if (endSnapshotId != null) {
        paddingScan = paddingScan.toSnapshot(endSnapshotId);
      }
    }
    return paddingScan;
  }

  static CloseableIterable<CombinedScanTask> planTasks(
      Table table, ScanContext context, ExecutorService workerPool) {
    ScanMode scanMode = checkScanMode(context);
    if (scanMode == ScanMode.INCREMENTAL_APPEND_SCAN) {
      IncrementalAppendScan scan = table.newIncrementalAppendScan();
      scan = refineScanWithBaseConfigs(scan, context, workerPool);

      Long startSnapshotId = null;
      Long endSnapshotId = null;

      if (context.startTag() != null) {
        Preconditions.checkArgument(
            table.snapshot(context.startTag()) != null,
            "Cannot find snapshot with tag %s",
            context.startTag());
        startSnapshotId = table.snapshot(context.startTag()).snapshotId();
      }

      if (context.startSnapshotId() != null) {
        Preconditions.checkArgument(
            context.startTag() == null, "START_SNAPSHOT_ID and START_TAG cannot both be set");
        startSnapshotId = context.startSnapshotId();
      }

      if (context.endTag() != null) {
        Preconditions.checkArgument(
            table.snapshot(context.endTag()) != null,
            "Cannot find snapshot with tag %s",
            context.endTag());
        endSnapshotId = table.snapshot(context.endTag()).snapshotId();
      }

      if (context.endSnapshotId() != null) {
        Preconditions.checkArgument(
            context.endTag() == null, "END_SNAPSHOT_ID and END_TAG cannot both be set");
        endSnapshotId = context.endSnapshotId();
      }

      if (context.streamingStartingStrategy()
          == StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP) {
        if (context.startSnapshotTimestamp() != null) {
          startSnapshotId =
              SnapshotUtil.snapshotIdAsOfTime(table, context.startSnapshotTimestamp());
        }
        if (context.endSnapshotTimestamp() != null) {
          endSnapshotId = SnapshotUtil.snapshotIdAsOfTime(table, context.endSnapshotTimestamp());
        }
      }

      scan = betweenStrategyPadding(scan, startSnapshotId, endSnapshotId, context.betweenMode());

      return scan.planTasks();
    } else {
      TableScan scan = table.newScan();
      scan = refineScanWithBaseConfigs(scan, context, workerPool);

      if (context.snapshotId() != null) {
        scan = scan.useSnapshot(context.snapshotId());
      } else if (context.tag() != null) {
        scan = scan.useRef(context.tag());
      } else if (context.branch() != null) {
        scan = scan.useRef(context.branch());
      }

      if (context.asOfTimestamp() != null) {
        scan = scan.asOfTime(context.asOfTimestamp());
      }

      return scan.planTasks();
    }
  }

  @VisibleForTesting
  enum ScanMode {
    BATCH,
    INCREMENTAL_APPEND_SCAN
  }

  private enum BetweenMode {
    l0r1,
    l1r1
  }

  @VisibleForTesting
  static ScanMode checkScanMode(ScanContext context) {
    if (context.startSnapshotId() != null
        || context.endSnapshotId() != null
        || context.startTag() != null
        || context.endTag() != null
        || context.startSnapshotTimestamp() != null
        || context.endSnapshotTimestamp() != null) {
      return ScanMode.INCREMENTAL_APPEND_SCAN;
    } else {
      return ScanMode.BATCH;
    }
  }

  /** refine scan with common configs */
  private static <T extends Scan<T, FileScanTask, CombinedScanTask>> T refineScanWithBaseConfigs(
      T scan, ScanContext context, ExecutorService workerPool) {
    T refinedScan =
        scan.caseSensitive(context.caseSensitive()).project(context.project()).planWith(workerPool);

    if (context.includeColumnStats()) {
      refinedScan = refinedScan.includeColumnStats();
    }

    refinedScan = refinedScan.option(TableProperties.SPLIT_SIZE, context.splitSize().toString());

    refinedScan =
        refinedScan.option(TableProperties.SPLIT_LOOKBACK, context.splitLookback().toString());

    refinedScan =
        refinedScan.option(
            TableProperties.SPLIT_OPEN_FILE_COST, context.splitOpenFileCost().toString());

    if (context.filters() != null) {
      for (Expression filter : context.filters()) {
        refinedScan = refinedScan.filter(filter);
      }
    }

    return refinedScan;
  }
}
