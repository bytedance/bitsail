/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.connector.doris.backend.model;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.connector.doris.error.DorisErrorCode;
import com.bytedance.bitsail.connector.doris.rest.model.Schema;
import com.bytedance.bitsail.connector.doris.thrift.TScanBatchResult;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * row batch data container.
 */
public class RowBatch {
  private static final Logger LOGGER = LoggerFactory.getLogger(RowBatch.class);
  // offset for iterate the rowBatch
  private int offsetInRowBatch = 0;
  private int rowCountInOneBatch = 0;
  private int readRowCount = 0;
  private final List<Row> rowBatch = new ArrayList<>();
  private final ArrowStreamReader arrowStreamReader;
  private VectorSchemaRoot root;
  private List<FieldVector> fieldVectors;
  private final RootAllocator rootAllocator;
  private final Schema schema;
  private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

  public RowBatch(TScanBatchResult nextResult, Schema schema) {
    this.schema = schema;
    this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
    this.arrowStreamReader = new ArrowStreamReader(
        new ByteArrayInputStream(nextResult.getRows()),
        rootAllocator
    );
    this.offsetInRowBatch = 0;
  }

  public RowBatch readArrow() {
    try {
      this.root = arrowStreamReader.getVectorSchemaRoot();
      while (arrowStreamReader.loadNextBatch()) {
        fieldVectors = root.getFieldVectors();
        if (fieldVectors.size() != schema.size()) {
          LOGGER.error("Schema size '{}' is not equal to arrow field size '{}'.",
              fieldVectors.size(), schema.size());
          throw new BitSailException(DorisErrorCode.FAILED_LOAD_DATA,
              "Load Doris data failed, schema size of fetch data is wrong.");
        }
        if (fieldVectors.size() == 0 || root.getRowCount() == 0) {
          LOGGER.debug("One batch in arrow has no data.");
          continue;
        }
        rowCountInOneBatch = root.getRowCount();
        // init the rowBatch
        for (int i = 0; i < rowCountInOneBatch; ++i) {
          rowBatch.add(new Row(fieldVectors.size()));
        }
        convertArrowToRowBatch();
        readRowCount += root.getRowCount();
      }
      return this;
    } catch (Exception e) {
      LOGGER.error("Read Doris Data failed because: ", e);
      throw new RuntimeException("Read Doris Data failed", e);
    } finally {
      close();
    }
  }

  public boolean hasNext() {
    return offsetInRowBatch < readRowCount;
  }

  private void addValueToRow(int rowIndex, Object obj) {
    if (rowIndex > rowCountInOneBatch) {
      String errMsg = "Get row offset: " + rowIndex + " larger than row size: " +
          rowCountInOneBatch;
      LOGGER.error(errMsg);
      throw new NoSuchElementException(errMsg);
    }
    rowBatch.get(readRowCount + rowIndex).put(obj);
  }

  public void convertArrowToRowBatch() {
    try {
      for (int col = 0; col < fieldVectors.size(); col++) {
        FieldVector curFieldVector = fieldVectors.get(col);
        Types.MinorType mt = curFieldVector.getMinorType();

        final String currentType = schema.get(col).getType();
        switch (currentType) {
          case "NULL_TYPE":
            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
              addValueToRow(rowIndex, null);
            }
            break;
          case "BOOLEAN":
            Preconditions.checkArgument(mt.equals(Types.MinorType.BIT),
                typeMismatchMessage(currentType, mt));
            BitVector bitVector = (BitVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
              Object fieldValue = bitVector.isNull(rowIndex) ? null : bitVector.get(rowIndex) != 0;
              addValueToRow(rowIndex, fieldValue);
            }
            break;
          case "TINYINT":
            Preconditions.checkArgument(mt.equals(Types.MinorType.TINYINT),
                typeMismatchMessage(currentType, mt));
            TinyIntVector tinyIntVector = (TinyIntVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
              Object fieldValue = tinyIntVector.isNull(rowIndex) ? null : tinyIntVector.get(rowIndex);
              addValueToRow(rowIndex, fieldValue);
            }
            break;
          case "SMALLINT":
            Preconditions.checkArgument(mt.equals(Types.MinorType.SMALLINT),
                typeMismatchMessage(currentType, mt));
            SmallIntVector smallIntVector = (SmallIntVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
              Object fieldValue = smallIntVector.isNull(rowIndex) ? null : smallIntVector.get(rowIndex);
              addValueToRow(rowIndex, fieldValue);
            }
            break;
          case "INT":
            Preconditions.checkArgument(mt.equals(Types.MinorType.INT),
                typeMismatchMessage(currentType, mt));
            IntVector intVector = (IntVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
              Object fieldValue = intVector.isNull(rowIndex) ? null : intVector.get(rowIndex);
              addValueToRow(rowIndex, fieldValue);
            }
            break;
          case "BIGINT":

            Preconditions.checkArgument(mt.equals(Types.MinorType.BIGINT),
                typeMismatchMessage(currentType, mt));
            BigIntVector bigIntVector = (BigIntVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
              Object fieldValue = bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex);
              addValueToRow(rowIndex, fieldValue);
            }
            break;
          case "FLOAT":
            Preconditions.checkArgument(mt.equals(Types.MinorType.FLOAT4),
                typeMismatchMessage(currentType, mt));
            Float4Vector float4Vector = (Float4Vector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
              Object fieldValue = float4Vector.isNull(rowIndex) ? null : float4Vector.get(rowIndex);
              addValueToRow(rowIndex, fieldValue);
            }
            break;
          case "TIME":
          case "DOUBLE":
            Preconditions.checkArgument(mt.equals(Types.MinorType.FLOAT8),
                typeMismatchMessage(currentType, mt));
            Float8Vector float8Vector = (Float8Vector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
              Object fieldValue = float8Vector.isNull(rowIndex) ? null : float8Vector.get(rowIndex);
              addValueToRow(rowIndex, fieldValue);
            }
            break;
          case "BINARY":
            Preconditions.checkArgument(mt.equals(Types.MinorType.VARBINARY),
                typeMismatchMessage(currentType, mt));
            VarBinaryVector varBinaryVector = (VarBinaryVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
              Object fieldValue = varBinaryVector.isNull(rowIndex) ? null : varBinaryVector.get(rowIndex);
              addValueToRow(rowIndex, fieldValue);
            }
            break;
          case "DECIMAL":
          case "DECIMALV2":
            Preconditions.checkArgument(mt.equals(Types.MinorType.DECIMAL),
                typeMismatchMessage(currentType, mt));
            DecimalVector decimalVector = (DecimalVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
              if (decimalVector.isNull(rowIndex)) {
                addValueToRow(rowIndex, null);
                continue;
              }
              BigDecimal value = decimalVector.getObject(rowIndex).stripTrailingZeros();
              addValueToRow(rowIndex, value);
            }
            break;
          case "DATE":
            Preconditions.checkArgument(mt.equals(Types.MinorType.VARCHAR),
                typeMismatchMessage(currentType, mt));
            VarCharVector date = (VarCharVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
              if (date.isNull(rowIndex)) {
                addValueToRow(rowIndex, null);
                continue;
              }
              String value = new String(date.get(rowIndex));
              LocalDate localDate = LocalDate.parse(value, dateFormatter);
              addValueToRow(rowIndex, localDate);
            }
            break;
          case "DATETIME":
            Preconditions.checkArgument(mt.equals(Types.MinorType.VARCHAR),
                typeMismatchMessage(currentType, mt));
            VarCharVector timeStampSecVector = (VarCharVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
              if (timeStampSecVector.isNull(rowIndex)) {
                addValueToRow(rowIndex, null);
                continue;
              }
              String value = new String(timeStampSecVector.get(rowIndex));
              LocalDateTime parse = LocalDateTime.parse(value, dateTimeFormatter);
              addValueToRow(rowIndex, parse);
            }
            break;
          case "LARGEINT":
          case "CHAR":
          case "VARCHAR":
          case "STRING":
            Preconditions.checkArgument(mt.equals(Types.MinorType.VARCHAR),
                typeMismatchMessage(currentType, mt));
            VarCharVector varCharVector = (VarCharVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
              if (varCharVector.isNull(rowIndex)) {
                addValueToRow(rowIndex, null);
                continue;
              }
              String value = new String(varCharVector.get(rowIndex));
              addValueToRow(rowIndex, value);
            }
            break;
          default:
            String errMsg = "Unsupported type " + schema.get(col).getType();
            LOGGER.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        }
      }
    } catch (Exception e) {
      close();
      throw e;
    }
  }

  public List<Object> next() {
    if (!hasNext()) {
      String errMsg = "Get row offset:" + offsetInRowBatch + " larger than row size: " + readRowCount;
      LOGGER.error(errMsg);
      throw new NoSuchElementException(errMsg);
    }
    return rowBatch.get(offsetInRowBatch++).getCols();
  }

  private String typeMismatchMessage(final String sparkType, final Types.MinorType arrowType) {
    final String messageTemplate = "FLINK type is %1$s, but arrow type is %2$s.";
    return String.format(messageTemplate, sparkType, arrowType.name());
  }

  public int getReadRowCount() {
    return readRowCount;
  }

  public void close() {
    try {
      if (arrowStreamReader != null) {
        arrowStreamReader.close();
      }
      if (rootAllocator != null) {
        rootAllocator.close();
      }
    } catch (IOException ioe) {
      // do nothing
    }
  }

  public static class Row {
    private final List<Object> cols;

    Row(int colCount) {
      this.cols = new ArrayList<>(colCount);
    }

    public List<Object> getCols() {
      return cols;
    }

    public void put(Object o) {
      cols.add(o);
    }
  }
}
