package com.bytedance.bitsail.connector.kudu.type;

import com.bytedance.bitsail.common.type.filemapping.FileMappingTypeInfoConverter;

public class KuduTypeConverter extends FileMappingTypeInfoConverter {
  public KuduTypeConverter() {
    super("kudu");
  }
}
