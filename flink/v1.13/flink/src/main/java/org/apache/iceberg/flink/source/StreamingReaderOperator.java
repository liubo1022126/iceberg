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

import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.JavaSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamSourceContexts;
import org.apache.flink.streaming.api.operators.YieldingOperatorFactory;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The operator that reads the {@link FlinkInputSplit splits} received from the preceding {@link
 * StreamingMonitorFunction}. Contrary to the {@link StreamingMonitorFunction} which has a parallelism of 1,
 * this operator can have multiple parallelism.
 *
 * <p>As soon as a split descriptor is received, it is put in a queue, and use {@link MailboxExecutor}
 * read the actual data of the split. This architecture allows the separation of the reading thread from the one split
 * processing the checkpoint barriers, thus removing any potential back-pressure.
 */
public class StreamingReaderOperator extends AbstractStreamOperator<RowData>
    implements OneInputStreamOperator<FlinkInputSplit, RowData> {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingReaderOperator.class);

  // It's the same thread that is running this operator and checkpoint actions. we use this executor to schedule only
  // one split for future reading, so that a new checkpoint could be triggered without blocking long time for exhausting
  // all scheduled splits.
  private final MailboxExecutor executor;
  private final long readLimitPerSecond;
  private final long readSplitWaitTime;
  private FlinkInputFormat format;

  private transient SourceFunction.SourceContext<RowData> sourceContext;

  private transient ListState<FlinkInputSplit> inputSplitsState;
  private transient Queue<FlinkInputSplit> splits;

  private AtomicLong readLimitPerSecondCurrent = new AtomicLong(0L);

  // Splits are read by the same thread that calls processElement. Each read task is submitted to that thread by adding
  // them to the executor. This state is used to ensure that only one read task is in that queue at a time, so that read
  // tasks do not accumulate ahead of checkpoint tasks. When there is a read task in the queue, this is set to RUNNING.
  // When there are no more files to read, this will be set to IDLE.
  private transient SplitState currentSplitState;

  private StreamingReaderOperator(
      FlinkInputFormat format, ProcessingTimeService timeService,
      MailboxExecutor mailboxExecutor, long readLimitPerSecond, long readSplitWaitTime) {
    this.format = Preconditions.checkNotNull(format, "The InputFormat should not be null.");
    this.processingTimeService = timeService;
    this.executor = Preconditions.checkNotNull(mailboxExecutor, "The mailboxExecutor should not be null.");
    this.readLimitPerSecond = readLimitPerSecond;
    this.readSplitWaitTime = readSplitWaitTime;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    // TODO Replace Java serialization with Avro approach to keep state compatibility.
    // See issue: https://github.com/apache/iceberg/issues/1698
    inputSplitsState = context.getOperatorStateStore().getListState(
        new ListStateDescriptor<>("splits", new JavaSerializer<>()));

    // Initialize the current split state to IDLE.
    currentSplitState = SplitState.IDLE;

    // Recover splits state from flink state backend if possible.
    splits = Lists.newLinkedList();
    if (context.isRestored()) {
      int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
      LOG.info("Restoring state for the {} (taskIdx: {}).", getClass().getSimpleName(), subtaskIdx);

      for (FlinkInputSplit split : inputSplitsState.get()) {
        splits.add(split);
      }
    }

    this.sourceContext = StreamSourceContexts.getSourceContext(
        getOperatorConfig().getTimeCharacteristic(),
        getProcessingTimeService(),
        new Object(), // no actual locking needed
        getContainingTask().getStreamStatusMaintainer(),
        output,
        getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval(),
        -1);

    // Enqueue to process the recovered input splits.
    enqueueProcessSplits();

    if (readLimitPerSecond > 0) {
      readLimitTimerSetup();
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);

    inputSplitsState.clear();
    inputSplitsState.addAll(Lists.newArrayList(splits));
  }

  @Override
  public void processElement(StreamRecord<FlinkInputSplit> element) {
    splits.add(element.getValue());
    enqueueProcessSplits();
  }

  private void enqueueProcessSplits() {
    if (currentSplitState == SplitState.IDLE && !splits.isEmpty()) {
      currentSplitState = SplitState.RUNNING;
      executor.execute(this::processSplits, this.getClass().getSimpleName());
    }
  }

  private void processSplits() throws Exception {
    FlinkInputSplit split = splits.poll();
    if (split == null) {
      currentSplitState = SplitState.IDLE;
      return;
    }

    format.open(split);
    try {
      RowData nextElement = null;
      while (!format.reachedEnd()) {
        while (readLimitPerSecond > 0 && readLimitPerSecondCurrent.incrementAndGet() > readLimitPerSecond) {
          Thread.sleep(100);
        }
        nextElement = format.nextRecord(nextElement);
        sourceContext.collect(nextElement);
      }
    } finally {
      currentSplitState = SplitState.IDLE;
      format.close();
    }

    if (readSplitWaitTime > 0) {
      Thread.sleep(readSplitWaitTime);
    }

    // Re-schedule to process the next split.
    enqueueProcessSplits();
  }

  private void readLimitTimerSetup() {
    new Timer("readLimitPerSecondTimer").schedule(new TimerTask() {
      @Override
      public void run() {
        readLimitPerSecondCurrent.set(0L);
      }
    }, 1000, 1000);
  }

  @Override
  public void processWatermark(Watermark mark) {
    // we do nothing because we emit our own watermarks if needed.
  }

  @Override
  public void dispose() throws Exception {
    super.dispose();

    if (format != null) {
      format.close();
      format.closeInputFormat();
      format = null;
    }

    sourceContext = null;
  }

  @Override
  public void close() throws Exception {
    super.close();
    output.close();
    if (sourceContext != null) {
      sourceContext.emitWatermark(Watermark.MAX_WATERMARK);
      sourceContext.close();
      sourceContext = null;
    }
  }

  static OneInputStreamOperatorFactory<FlinkInputSplit, RowData> factory(
      FlinkInputFormat format, long readLimitPerSecond, long readSplitWaitTime) {
    OperatorFactory operatorFactory = new OperatorFactory(format, readLimitPerSecond, readSplitWaitTime);
    return operatorFactory;
  }

  private enum SplitState {
    IDLE,
    RUNNING
  }

  private static class OperatorFactory extends AbstractStreamOperatorFactory<RowData>
      implements YieldingOperatorFactory<RowData>, OneInputStreamOperatorFactory<FlinkInputSplit, RowData> {

    private final FlinkInputFormat format;
    private final long readLimitPerSecond;
    private final long readSplitWaitTime;

    private transient MailboxExecutor mailboxExecutor;

    private OperatorFactory(FlinkInputFormat format, long readLimitPerSecond, long readSplitWaitTime) {
      this.format = format;
      this.readLimitPerSecond = readLimitPerSecond;
      this.readSplitWaitTime = readSplitWaitTime;
    }

    @Override
    public void setMailboxExecutor(MailboxExecutor mailboxExecutor) {
      this.mailboxExecutor = mailboxExecutor;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <O extends StreamOperator<RowData>> O createStreamOperator(StreamOperatorParameters<RowData> parameters) {
      StreamingReaderOperator operator =
          new StreamingReaderOperator(
              format,
              processingTimeService,
              mailboxExecutor,
              readLimitPerSecond,
              readSplitWaitTime);
      operator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
      return (O) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return StreamingReaderOperator.class;
    }
  }
}
