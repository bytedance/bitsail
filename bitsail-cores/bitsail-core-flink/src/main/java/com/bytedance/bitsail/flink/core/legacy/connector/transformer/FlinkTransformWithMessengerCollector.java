/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.flink.core.legacy.connector.transformer;

import com.bytedance.bitsail.base.dirty.AbstractDirtyCollector;
import com.bytedance.bitsail.base.dirty.DirtyCollectorFactory;
import com.bytedance.bitsail.base.messenger.Messenger;
import com.bytedance.bitsail.base.messenger.MessengerFactory;
import com.bytedance.bitsail.base.messenger.common.MessengerGroup;
import com.bytedance.bitsail.base.messenger.context.MessengerContext;
import com.bytedance.bitsail.base.messenger.context.SimpleMessengerContext;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.flink.core.runtime.RuntimeContextInjectable;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.IOException;

@Slf4j
public class FlinkTransformWithMessengerCollector extends ProcessFunction<Row, Row> implements ResultTypeQueryable<Row> {

  private final String instanceId;
  private final String messageGroup;
  private Messenger<Row> messenger;
  private MessengerContext messengerContext;
  private AbstractDirtyCollector dirtyCollector;
  private AbstractTransform transform;

  private BitSailConfiguration commonConfiguration;

  public FlinkTransformWithMessengerCollector(BitSailConfiguration commonConf) {
    this.instanceId = commonConf.getNecessaryOption(CommonOptions.INTERNAL_INSTANCE_ID, CommonErrorCode.CONFIG_ERROR);
    this.messageGroup = MessengerGroup.WRITER.toString();
    this.commonConfiguration = commonConf;

    messengerContext = SimpleMessengerContext
        .builder()
        .messengerGroup(MessengerGroup.WRITER)
        .instanceId(instanceId)
        .build();
    messenger = MessengerFactory.initMessenger(commonConfiguration, messengerContext);
    dirtyCollector = DirtyCollectorFactory.initDirtyCollector(commonConfiguration, messengerContext);
  }

  public void setTransform(AbstractTransform abstractTransform) {
    this.transform = abstractTransform;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    if (messenger instanceof RuntimeContextInjectable) {
      ((RuntimeContextInjectable) messenger).setRuntimeContext(getRuntimeContext());
    }
    if (dirtyCollector instanceof RuntimeContextInjectable) {
      ((RuntimeContextInjectable) dirtyCollector).setRuntimeContext(getRuntimeContext());
    }
    messenger.open();

    transform.open();
  }

  @Override
  public void close() throws Exception {
    messenger.close();
    dirtyCollector.close();
    transform.close();
  }

  @Override
  public TypeInformation<Row> getProducedType() {
    return transform.getProducedType();
  }

  @Override
  public void processElement(Row row, Context context, Collector<Row> collector) throws Exception {
    try {
      Row output = transform.process(row);
      collector.collect(output);
    } catch (BitSailException e) {
      messenger.addFailedRecord(row, e);
      dirtyCollector.collectDirty(row, e, System.currentTimeMillis());
      log.debug("Transform one record failed. - " + row.toString(), e);
    } catch (Exception e) {
      throw new IOException("Couldn't transform data - " + row.toString(), e);
    }
  }
}
