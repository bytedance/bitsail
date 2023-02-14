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

package com.bytedance.bitsail.test.e2e.datasource;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class DataSourceFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DataSourceFactory.class);

  public static AbstractDataSource getAsSource(BitSailConfiguration jobConf) {
    return getDataSource(jobConf, AbstractDataSource.Role.SOURCE);
  }

  public static AbstractDataSource getAsSink(BitSailConfiguration jobConf) {
    return getDataSource(jobConf, AbstractDataSource.Role.SINK);
  }

  private static AbstractDataSource getDataSource(BitSailConfiguration jobConf,
                                                  AbstractDataSource.Role role) {
    ServiceLoader<AbstractDataSource> loader = ServiceLoader.load(AbstractDataSource.class);

    List<AbstractDataSource> acceptedDataSource = new ArrayList<>();
    for (AbstractDataSource dataSource : loader) {
      if (dataSource != null && dataSource.accept(jobConf, role)) {
        acceptedDataSource.add(dataSource);
      }
    }

    if (acceptedDataSource.size() > 1) {
      throw new IllegalStateException("Multiple data sources are accepted.");
    }
    if (acceptedDataSource.isEmpty()) {
      throw BitSailException.asBitSailException(CommonErrorCode.CONFIG_ERROR,
          "No matched data source found.");
    }

    AbstractDataSource accept = acceptedDataSource.get(0);
    accept.setRole(role);
    LOG.info("Load data source [{}] as {}.", accept.getContainerName(), accept.role);
    return accept;
  }
}
