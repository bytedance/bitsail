/*
 *       Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.bytedance.bitsail.connector.elasticsearch.sink.listener;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultBulkListener implements BulkProcessor.Listener {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultBulkListener.class);

  private final int subTaskId;

  private final AtomicReference<Throwable> reference;

  public DefaultBulkListener(int subTaskId) {
    this.subTaskId = subTaskId;
    this.reference = new AtomicReference<>();
  }

  @Override
  public void beforeBulk(long executionId, BulkRequest request) {
    LOG.debug("Subtask {} prepare bulk requests with execution id {}.", subTaskId, executionId);
  }

  //Invoke after succeed.
  @Override
  public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
    LOG.info("Subtask {} finished bulk request with execution id {}, bulk request actions size {}(s).", subTaskId, executionId, request.requests().size());
    if (response.hasFailures()) {
      String message = response.buildFailureMessage();
      LOG.error("Subtask {} bulk request actions has some failures, failure message {}.", subTaskId, message);
      reference.set(new RuntimeException(message));
    }
  }

  //Invoke after failed
  @Override
  public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
    LOG.error("Subtask {} failed bulk with execution id {}.", subTaskId, executionId, failure);
    reference.set(failure);
  }

  public void checkErroneous() {
    Throwable throwable = reference.get();
    if (Objects.isNull(throwable)) {
      return;
    }
    throw new RuntimeException(throwable);
  }
}
