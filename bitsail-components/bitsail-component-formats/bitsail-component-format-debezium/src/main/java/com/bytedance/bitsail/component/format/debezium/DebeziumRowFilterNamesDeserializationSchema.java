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

package com.bytedance.bitsail.component.format.debezium;

import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.row.RowKind;
import com.bytedance.bitsail.component.format.debezium.option.DebeziumReaderOptions;

import io.debezium.data.Enum;
import io.debezium.data.EnumSet;
import io.debezium.data.Envelope;
import io.debezium.data.Json;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.Year;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Objects;

import static io.debezium.data.Envelope.FieldName.AFTER;
import static io.debezium.data.Envelope.FieldName.BEFORE;
import static io.debezium.data.Envelope.FieldName.OPERATION;
import static org.apache.kafka.connect.data.Values.convertToDate;
import static org.apache.kafka.connect.data.Values.convertToTime;
import static org.apache.kafka.connect.data.Values.convertToTimestamp;

public class DebeziumRowFilterNamesDeserializationSchema implements DeserializationSchema<String, Row> {
  private static final Logger LOG = LoggerFactory.getLogger(DebeziumRowFilterNamesDeserializationSchema.class);

  private final BitSailConfiguration jobConf;
  private final JsonConverter jsonConverter;

  public DebeziumRowFilterNamesDeserializationSchema(BitSailConfiguration jobConf) {
    this.jobConf = jobConf;
    this.jsonConverter = new JsonConverter();
    boolean includeSchema = jobConf.get(DebeziumReaderOptions.DEBEZIUM_JSON_INCLUDE_SCHEMA);
    final HashMap<String, Object> configs = new HashMap<>();
    configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
    configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, includeSchema);
    jsonConverter.configure(configs);
  }

  @Override
  public Row deserialize(String message) {
    throw new UnsupportedOperationException("Please invoke DeserializationSchema#deserialize(byte[], fieldNames) instead.");
  }

  public Row deserialize(String message, String[] fieldNames) {
    SchemaAndValue schemaAndValue;
    try {
      schemaAndValue = jsonConverter.toConnectData(null, message.getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      LOG.error("Can't parse content from format [debezium], content: {}.", message, e);
      throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
          String.format("Can't parse debezium json: %s.", message), e);
    }
    Struct value = (Struct) schemaAndValue.value();
    Envelope.Operation operation = Envelope.Operation.forCode(value.getString(OPERATION));

    if (operation == Envelope.Operation.CREATE || operation == Envelope.Operation.READ) {
      Struct after = value.getStruct(AFTER);
      return convert(after, after.schema(), fieldNames, RowKind.INSERT);
    }

    if (operation == Envelope.Operation.DELETE) {
      Struct before = value.getStruct(BEFORE);
      return convert(before, before.schema(), fieldNames, RowKind.DELETE);
    }

    if (operation == Envelope.Operation.UPDATE) {
      Struct after = value.getStruct(AFTER);
      return convert(after, after.schema(), fieldNames, RowKind.UPDATE_AFTER);
    }

    throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
        String.format("Not support operation: %s right now.", operation));
  }

  public Row convert(Struct struct, Schema schema, String[] fieldNames, RowKind rowKind) {
    Row row = new Row(fieldNames.length);
    row.setKind(rowKind);
    for (int index = 0; index < fieldNames.length; index++) {
      Field field = schema.field(fieldNames[index]);
      if (Objects.isNull(field)) {
        row.setField(index, null);
      } else {
        Object withoutDefault = struct.getWithoutDefault(fieldNames[index]);
        try {
          withoutDefault = Objects.isNull(withoutDefault) ? null
              : convert(field.schema(), withoutDefault);
          row.setField(index, withoutDefault);
        } catch (BitSailException e) {
          LOG.error("Failed to parse field {} from value {}.", field.name(), withoutDefault);
          throw e;
        }
      }
    }
    return row;
  }

  private Object convert(Schema fieldSchema, Object withoutDefault) {
    if (isPrimitiveType(fieldSchema)) {
      return convertPrimitiveType(fieldSchema, withoutDefault);
    } else {
      //todo support local timestamp zone.
      return convertOtherType(fieldSchema, withoutDefault, null);
    }
  }

  private static boolean isPrimitiveType(Schema fieldSchema) {
    return fieldSchema.name() == null;
  }

  private Object convertPrimitiveType(Schema fieldSchema, Object fieldValue) {
    switch (fieldSchema.type()) {
      case BOOLEAN:
        return convertToBoolean(fieldValue);
      case INT8:
      case INT16:
      case INT32:
        return convertToInteger(fieldValue);
      case INT64:
        return convertToLong(fieldValue);
      case FLOAT32:
        return convertToFloat(fieldValue);
      case FLOAT64:
        return convertToDouble(fieldValue);
      case STRING:
        return convertToString(fieldValue);
      case BYTES:
        return convertToBinary(fieldValue);
      default:
        throw new UnsupportedOperationException("Not support type: " + fieldSchema.type());
    }
  }

  private Object convertOtherType(Schema fieldSchema, Object fieldValue, ZoneId serverTimeZone) {
    switch (fieldSchema.name()) {
      case Enum.LOGICAL_NAME:
      case Json.LOGICAL_NAME:
      case EnumSet.LOGICAL_NAME:
        return convertToString(fieldValue);
      case Time.SCHEMA_NAME:
      case MicroTime.SCHEMA_NAME:
      case NanoTime.SCHEMA_NAME:
        return convertToTime(fieldSchema, fieldValue);
      case Timestamp.SCHEMA_NAME:
      case MicroTimestamp.SCHEMA_NAME:
      case NanoTimestamp.SCHEMA_NAME:
        return convertToTimestamp(fieldSchema, fieldValue);
      case Decimal.LOGICAL_NAME:
        return convertToDecimal(fieldSchema, fieldValue);
      case Date.SCHEMA_NAME:
        return convertToDate(fieldSchema, fieldValue);
      case Year.SCHEMA_NAME:
        return convertToInteger(fieldValue);
      case ZonedTime.SCHEMA_NAME:
      case ZonedTimestamp.SCHEMA_NAME:
        return convertToZoneTimeStamp(fieldSchema, fieldValue);
      default:
        throw BitSailException.asBitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
            String.format("Field name %s not support schema %s.",
                fieldSchema.name(),
                fieldSchema.schema()
            )
        );
    }
  }

  private byte[] convertToBinary(Object fieldValue) {
    if (fieldValue instanceof byte[]) {
      return (byte[]) fieldValue;
    } else if (fieldValue instanceof ByteBuffer) {
      ByteBuffer byteBuffer = (ByteBuffer) fieldValue;
      byte[] bytes = new byte[byteBuffer.remaining()];
      byteBuffer.get(bytes);
      return bytes;
    } else {
      throw new UnsupportedOperationException(
          "Unsupported Binary value type: " + fieldValue.getClass().getSimpleName());
    }
  }

  private String convertToString(Object fieldValue) {
    return fieldValue.toString();
  }

  private Double convertToDouble(Object fieldValue) {
    if (fieldValue instanceof Float) {
      return ((Float) fieldValue).doubleValue();
    } else if (fieldValue instanceof Double) {
      return (Double) fieldValue;
    } else {
      return Double.parseDouble(fieldValue.toString());
    }
  }

  private Float convertToFloat(Object fieldValue) {
    if (fieldValue instanceof Float) {
      return (Float) fieldValue;
    } else if (fieldValue instanceof Double) {
      return ((Double) fieldValue).floatValue();
    } else {
      return Float.parseFloat(fieldValue.toString());
    }
  }

  private Long convertToLong(Object fieldValue) {
    if (fieldValue instanceof Integer) {
      return ((Integer) fieldValue).longValue();
    } else if (fieldValue instanceof Long) {
      return (Long) fieldValue;
    } else {
      return Long.parseLong(fieldValue.toString());
    }
  }

  private Boolean convertToBoolean(Object fieldValue) {
    if (fieldValue instanceof Integer) {
      return (Integer) fieldValue != 0;
    } else if (fieldValue instanceof Long) {
      return (Long) fieldValue != 0;
    } else {
      String str = fieldValue.toString();
      if (NumberUtils.isNumber(str)) {
        return NumberUtils.createNumber(str).intValue() != 0;
      }
      return Boolean.parseBoolean(fieldValue.toString());
    }
  }

  private Integer convertToInteger(Object fieldValue) {
    if (fieldValue instanceof Integer) {
      return (Integer) (fieldValue);
    } else if (fieldValue instanceof Long) {
      return ((Long) fieldValue).intValue();
    } else {
      return Integer.parseInt(fieldValue.toString());
    }
  }

  public Object convertToDecimal(Schema schema, Object fieldValue) {
    BigDecimal bigDecimal;
    if (fieldValue instanceof byte[]) {
      // for decimal.handling.mode=precise
      bigDecimal = Decimal.toLogical(schema, (byte[]) fieldValue);
    } else if (fieldValue instanceof String) {
      // for decimal.handling.mode=string
      bigDecimal = new BigDecimal((String) fieldValue);
    } else if (fieldValue instanceof Double) {
      // for decimal.handling.mode=double
      bigDecimal = BigDecimal.valueOf((Double) fieldValue);
    } else {
      if (VariableScaleDecimal.LOGICAL_NAME.equals(schema.name())) {
        SpecialValueDecimal decimal =
            VariableScaleDecimal.toLogical((Struct) fieldValue);
        bigDecimal = decimal.getDecimalValue().orElse(BigDecimal.ZERO);
      } else {
        // fallback to string
        bigDecimal = new BigDecimal(fieldValue.toString());
      }
    }
    return bigDecimal;
  }

  private Object convertToZoneTimeStamp(Schema fieldSchema, Object fieldValue) {
    if (fieldValue instanceof String) {
      String str = (String) fieldValue;
      Instant instant = Instant.parse(str);
      //TODO zone timestamp
    }
    throw BitSailException.asBitSailException(CommonErrorCode.RUNTIME_ERROR,
        String.format("Can't parse field [%s] from value %s to zone timestamp.",
            fieldSchema.name(),
            fieldValue
        ));
  }

  @Override
  public boolean isEndOfStream(Row nextElement) {
    return false;
  }
}
