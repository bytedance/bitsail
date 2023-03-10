package com.bytedance.bitsail.connector.kudu.sink.schema;

import com.bytedance.bitsail.base.extension.SchemaAlignmentable;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.connector.kudu.error.KuduErrorCode;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduTable;

import java.util.List;

public class KuduSchemaAlignment implements SchemaAlignmentable<List<ColumnSchema>> {
  private final transient KuduClient kuduClient;
  private final transient KuduTable kuduTable;

  public KuduSchemaAlignment(KuduClient kuduClient, KuduTable kuduTable) {
    this.kuduClient = kuduClient;
    this.kuduTable = kuduTable;
  }

  @Override
  public void align(List<ColumnSchema> currentColumnSchema,  List<ColumnSchema> targetColumnSchema) {
    try {
      AlterTableOptions alterTableOptions = new AlterTableOptions();
      for (ColumnSchema newColumn : targetColumnSchema) {
        if (currentColumnSchema.stream().noneMatch(column -> column.getName().equals(newColumn.getName()))) {
          alterTableOptions.addColumn(newColumn);
        }
      }
      kuduClient.alterTable(kuduTable.getName(), alterTableOptions);
    } catch (Exception e) {
      throw BitSailException.asBitSailException(KuduErrorCode.ALTER_TABLE_ERROR, e);
    }
  }
}
