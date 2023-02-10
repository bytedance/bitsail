package com.bytedance.bitsail.connector.cdc.jdbc.source.config;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.cdc.error.BinlogReaderErrorCode;
import com.bytedance.bitsail.connector.cdc.model.ClusterInfo;
import com.bytedance.bitsail.connector.cdc.model.ConnectionInfo;
import com.bytedance.bitsail.connector.cdc.option.BinlogReaderOptions;

import io.debezium.config.Configuration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.time.ZoneId;
import java.util.List;
import java.util.Properties;

@Getter
@Builder
public abstract class AbstractJdbcDebeziumConfig {

  private static final long serialVersionUID = 1L;

  public static final String DEBEZIUM_PREFIX = "job.reader.debezium.";

  private final String hostname;
  private final int port;
  private final String username;
  private final String password;

  // debezium configuration
  private final Properties dbzProperties;
  private final Configuration dbzConfiguration;

  public static AbstractJdbcDebeziumConfig fromBitSailConf(BitSailConfiguration jobConf) {
    List<ClusterInfo> clusterInfo = jobConf.getNecessaryOption(BinlogReaderOptions.CONNECTIONS, BinlogReaderErrorCode.REQUIRED_VALUE);
    //Only support one DB
    assert (clusterInfo.size() == 1);
    ConnectionInfo connectionInfo = clusterInfo.get(0).getMaster();
    assert (connectionInfo != null);
    Properties props = extractProps(jobConf);
    String username = jobConf.getNecessaryOption(BinlogReaderOptions.USER_NAME, BinlogReaderErrorCode.REQUIRED_VALUE);
    String password = jobConf.getNecessaryOption(BinlogReaderOptions.PASSWORD, BinlogReaderErrorCode.REQUIRED_VALUE);
    String timezone = jobConf.get(BinlogReaderOptions.CONNECTION_TIMEZONE);
    fillConnectionInfo(props, connectionInfo, username, password, timezone);

    Configuration config = Configuration.from(props);
    return AbstractJdbcDebeziumConfig.builder()
            .hostname(connectionInfo.getHost())
            .port(connectionInfo.getPort())
            .username(username)
            .password(password)
            .dbzProperties(props)
            .dbzConfiguration(config)
            .build();
  }

  public static Properties extractProps(BitSailConfiguration jobConf) {
    Properties props = new Properties();
    jobConf.getKeys().stream()
            .filter(s -> s.startsWith(DEBEZIUM_PREFIX))
            .map(s -> StringUtils.substringAfter(s, DEBEZIUM_PREFIX))
            .forEach(s -> props.setProperty(s, jobConf.getString(DEBEZIUM_PREFIX + s)));
    return props;
  }

  public abstract RelationalDatabaseConnectorConfig getConnectorConfig();

  public static void fillConnectionInfo(Properties props, ConnectionInfo connectionInfo, String username, String password, String timezone) {
    props.put("database.hostname", connectionInfo.getHost());
    props.put("database.port", String.valueOf(connectionInfo.getPort()));
    props.put("database.user", username);
    props.put("database.password", password);
    props.put("database.serverTimezone", ZoneId.of(timezone).toString());
  }

}
