/*
 *     Licensed to the Apache Software Foundation (ASF) under one or more
 *     contributor license agreements.  See the NOTICE file distributed with
 *     this work for additional information regarding copyright ownership.
 *     The ASF licenses this file to You under the Apache License, Version 2.0
 *     (the "License"); you may not use this file except in compliance with
 *     the License.  You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.bytedance.bitsail.connector.jdbc.util;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class JdbcConnectionHolder implements AutoCloseable {

  private static final long serialVersionUID = 1L;

  private final BitSailConfiguration jdbcOptions;

  private transient Driver loadedDriver;
  private transient Connection connection;

  static {
    // Load DriverManager first to avoid deadlock between DriverManager's
    // static initialization block and specific driver class's static
    // initialization block when two different driver classes are loading
    // concurrently using Class.forName while DriverManager is uninitialized
    // before.
    //
    // This could happen in JDK 8 but not above as driver loading has been
    // moved out of DriverManager's static initialization block since JDK 9.
    DriverManager.getDrivers();
  }

  public JdbcConnectionHolder(@NonNull BitSailConfiguration jdbcOptions) {
    this.jdbcOptions = jdbcOptions;
  }

  public Connection getConnection() {
    return connection;
  }

  public boolean isConnectionValid()
      throws SQLException {
    return connection != null
        && connection.isValid(jdbcOptions.getConnectionCheckTimeoutSeconds());
  }

  private static Driver loadDriver(String driverName)
      throws ClassNotFoundException {
    checkNotNull(driverName);
    Enumeration<Driver> drivers = DriverManager.getDrivers();
    while (drivers.hasMoreElements()) {
      Driver driver = drivers.nextElement();
      if (driver.getClass().getName().equals(driverName)) {
        return driver;
      }
    }

    // We could reach here for reasons:
    // * Class loader hell of DriverManager(see JDK-8146872).
    // * driver is not installed as a service provider.
    Class<?> clazz =
        Class.forName(driverName, true, Thread.currentThread().getContextClassLoader());
    try {
      return (Driver) clazz.getDeclaredConstructor().newInstance();
    } catch (Exception ex) {
      throw new JdbcConnectorException(JdbcConnectorErrorCode.CREATE_DRIVER_FAILED, "Fail to create driver of class " + driverName, ex);
    }
  }

  private Driver getLoadedDriver()
      throws SQLException, ClassNotFoundException {
    if (loadedDriver == null) {
      loadedDriver = loadDriver(jdbcOptions.getDriverName());
    }
    return loadedDriver;
  }

  public Connection getOrEstablishConnection()
      throws SQLException, ClassNotFoundException {
    if (connection != null) {
      return connection;
    }
    Driver driver = getLoadedDriver();
    Properties info = new Properties();
    if (jdbcOptions.getUsername().isPresent()) {
      info.setProperty("user", jdbcOptions.getUsername().get());
    }
    if (jdbcOptions.getPassword().isPresent()) {
      info.setProperty("password", jdbcOptions.getPassword().get());
    }
    connection = driver.connect(jdbcOptions.getUrl(), info);
    if (connection == null) {
      // Throw same exception as DriverManager.getConnection when no driver found to match
      // caller expectation.
      throw new JdbcConnectorException(
          JdbcConnectorErrorCode.NO_SUITABLE_DRIVER, "No suitable driver found for " + jdbcOptions.getUrl());
    }

    connection.setAutoCommit(jdbcOptions.isAutoCommit());

    return connection;
  }

  public Connection reestablishConnection()
      throws SQLException, ClassNotFoundException {
    close();
    return getOrEstablishConnection();
  }

  @Override
  public void close() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        log.warn("JDBC connection close failed.", e);
      } finally {
        connection = null;
      }
    }
  }
}
