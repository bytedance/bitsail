package com.bytedance.connector.hbase.option;

import com.alibaba.fastjson.TypeReference;
import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.common.option.ReaderOptions;

import java.util.Map;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;
import static com.bytedance.bitsail.common.option.ReaderOptions.READER_PREFIX;

public interface HBaseReaderOptions extends ReaderOptions.BaseReaderOptions {

    /**
     * HBase configurations for creating connections.
     * For example: <br/>
     * {
     * "hbase.zookeeper.property.clientPort": "2181",
     * "hbase.rootdir": "hdfs://ns1/hbase",
     * "hbase.cluster.distributed": "true",
     * "hbase.zookeeper.quorum": "node01,node02,node03",
     * "zookeeper.znode.parent": "/hbase"
     * }
     */
    ConfigOption<Map<String, Object>> HBASE_CONF =
            key(READER_PREFIX + "hbase_conf")
                    .onlyReference(new TypeReference<Map<String, Object>>(){});

    /**
     * Table to read.
     */
    ConfigOption<String> TABLE =
            key(READER_PREFIX + "table")
                    .noDefaultValue(String.class);

    /**
     * Support UTF-8, UTF-16, ISO-8859-1 and <i>etc.</i>.<br/>
     * Default encoding is UTF-8.
     */
    ConfigOption<String> ENCODING =
            key(READER_PREFIX + "encoding")
                    .defaultValue("UTF-8");
}
