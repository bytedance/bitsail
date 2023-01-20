package com.bytedance.bitsail.connector.hbase.constant;

public class HBaseConstants {
    public static String HBASE_CONNECTOR_NAME = "hbase";
    public static final String AUTHENTICATION_TYPE = "Kerberos";
    public static final String KEY_HBASE_SECURITY_AUTHENTICATION = "hbase.security.authentication";
    public static final String KEY_HBASE_SECURITY_AUTHORIZATION = "hbase.security.authorization";

    public static final String KEY_PRINCIPAL = "principal";
    public static final String KEY_USE_LOCAL_FILE = "useLocalFile";
    public static final String KEY_PRINCIPAL_FILE = "principalFile";
    public static final String KEY_USE_BASE64_CONTENT = "useBase64Content";
    public static final String KEY_KRB5_CONTENT = "krb5file_content";
    public static final String KEY_KEYTAB_CONTENT = "keytab_content";
    public static final String KEY_KRB5_CONTENT_TMP_FILEPATH = "/tmp/kerberos-bitsail/krb5.conf";
    public static final String KEY_KEYTAB_CONTENT_TMP_FILEPATH = "/tmp/kerberos-bitsail/principal.keytab";
    public static final String KEY_JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";

    public static final String KEY_FS_HDFS_IMPL_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";

    public static final int MAX_PARALLELISM_OUTPUT_HBASE = 5;
}
