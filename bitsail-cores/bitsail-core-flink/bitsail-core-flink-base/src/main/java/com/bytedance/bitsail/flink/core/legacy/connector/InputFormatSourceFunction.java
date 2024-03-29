/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.flink.core.legacy.connector;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProviderException;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A {@link SourceFunction} that reads data using an {@link InputFormat}.
 */
@Internal
public class InputFormatSourceFunction<OUT> extends org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction<OUT> {
  //RichParallelSourceFunction<OUT> { //
  private static final long serialVersionUID = 1L;

  private TypeInformation<OUT> typeInfo;
  private transient TypeSerializer<OUT> serializer;

  private InputFormat<OUT, InputSplit> format;

  private transient InputSplitProvider provider;
  private transient Iterator<InputSplit> splitIterator;

  private volatile boolean isRunning = true;

  private volatile boolean invokedOpenInputFormat = false;

  @SuppressWarnings("unchecked")
  public InputFormatSourceFunction(InputFormat<OUT, ?> format, TypeInformation<OUT> typeInfo) {
    super(format, typeInfo);
    this.format = (InputFormat<OUT, InputSplit>) format;
    this.typeInfo = typeInfo;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void open(Configuration parameters) throws Exception {
    StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();

    if (format instanceof RichInputFormat) {
      ((RichInputFormat) format).setRuntimeContext(context);
    }
    format.configure(parameters);

    if (isRunning && format instanceof RichInputFormat) {
      ((RichInputFormat) format).openInputFormat();
      invokedOpenInputFormat = true;
    }

    provider = context.getInputSplitProvider();
    serializer = typeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
    splitIterator = getInputSplits();
    isRunning = splitIterator.hasNext();
  }

  @Override
  public void run(SourceContext<OUT> ctx) throws Exception {
    try {

      Counter completedSplitsCounter = getRuntimeContext().getMetricGroup().counter("numSplitsProcessed");
      if (isRunning && !invokedOpenInputFormat && format instanceof RichInputFormat) {
        ((RichInputFormat) format).openInputFormat();
      }

      OUT nextElement = serializer.createInstance();
      OUT returned;
      while (isRunning) {
        format.open(splitIterator.next());

        // for each element we also check if cancel
        // was called by checking the isRunning flag

        while (isRunning && !format.reachedEnd()) {
          returned = format.nextRecord(nextElement);
          if (returned != null) {
            ctx.collect(returned);
          }
        }
        format.close();
        completedSplitsCounter.inc();

        if (isRunning) {
          isRunning = splitIterator.hasNext();
        }
      }
    } finally {
      format.close();
      if (format instanceof RichInputFormat) {
        ((RichInputFormat) format).closeInputFormat();
      }
      isRunning = false;
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }

  @Override
  public void close() throws Exception {
    format.close();
    if (format instanceof RichInputFormat) {
      ((RichInputFormat) format).closeInputFormat();
    }
  }

  /**
   * Returns the {@code InputFormat}. This is only needed because we need to set the input
   * split assigner on the {@code StreamGraph}.
   */
  public InputFormat<OUT, InputSplit> getFormat() {
    return format;
  }

  protected Iterator<InputSplit> getInputSplits() {

    return new Iterator<InputSplit>() {

      private InputSplit nextSplit;

      private boolean exhausted;

      @Override
      public boolean hasNext() {
        if (exhausted) {
          return false;
        }

        if (nextSplit != null) {
          return true;
        }

        final InputSplit split;
        try {
          split = provider.getNextInputSplit(getRuntimeContext().getUserCodeClassLoader());
        } catch (InputSplitProviderException e) {
          throw new RuntimeException("Could not retrieve next input split.", e);
        }

        if (split != null) {
          this.nextSplit = split;
          return true;
        } else {
          exhausted = true;
          return false;
        }
      }

      @Override
      public InputSplit next() {
        if (this.nextSplit == null && !hasNext()) {
          throw new NoSuchElementException();
        }

        final InputSplit tmp = this.nextSplit;
        this.nextSplit = null;
        return tmp;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
