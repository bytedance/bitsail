package com.bytedance.bitsail.connector.kudu.util;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.connector.kudu.error.KuduErrorCode;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KuduPredicateUtil {
  private static final Logger LOG = LoggerFactory.getLogger(KuduPredicateUtil.class);
  private static final Map<String, KuduPredicate.ComparisonOp> KUDU_COMP_PREDICATE_MAP = new HashMap(10);
  private static final int KUDU_PREDICATE_JSON_MIN_ELE = 2;
  private static final int KUDU_PREDICATE_JSON_COMP_OR_IN_OPERAND_NUM = 3;

  static {
    KUDU_COMP_PREDICATE_MAP.put("<", KuduPredicate.ComparisonOp.LESS);
    KUDU_COMP_PREDICATE_MAP.put("<=", KuduPredicate.ComparisonOp.LESS_EQUAL);
    KUDU_COMP_PREDICATE_MAP.put("=", KuduPredicate.ComparisonOp.EQUAL);
    KUDU_COMP_PREDICATE_MAP.put(">=", KuduPredicate.ComparisonOp.GREATER_EQUAL);
    KUDU_COMP_PREDICATE_MAP.put(">", KuduPredicate.ComparisonOp.GREATER);
  }

  public static List<KuduPredicate> parseFromConfig(final String predicateConfigJson, final Schema schema) {
    JSONArray operatorJsonArray = JSON.parseArray(predicateConfigJson);
    List<KuduPredicate> predicates = new ArrayList<>();
    int operatorIndex = 0;
    if (operatorJsonArray.get(operatorIndex) instanceof String) {
      if ("AND".equals(operatorJsonArray.get(operatorIndex))) {
        operatorIndex++;
      } else {
        throw new BitSailException(KuduErrorCode.CONFIG_ERROR, "only support AND in kudu predicate config");
      }
    }
    for (; operatorIndex < operatorJsonArray.size(); operatorIndex++) {
      JSONArray operatorJson = operatorJsonArray.getJSONArray(operatorIndex);
      if (operatorJson.size() < KUDU_PREDICATE_JSON_MIN_ELE) {
        throw new BitSailException(KuduErrorCode.CONFIG_ERROR, "kudu predicate config error");
      }
      // config format : operator columnName operand
      String kuduPredicateOperatorName = operatorJson.getString(0);
      ColumnSchema column = schema.getColumn(operatorJson.getString(1));
      if (KUDU_COMP_PREDICATE_MAP.containsKey(kuduPredicateOperatorName)) {
        // simple
        if (operatorJson.size() < KUDU_PREDICATE_JSON_COMP_OR_IN_OPERAND_NUM) {
          throw new BitSailException(KuduErrorCode.CONFIG_ERROR, String.format("'%s' predicate need one operand", kuduPredicateOperatorName));
        }
        predicates.add(KuduPredicate.newComparisonPredicate(column, KUDU_COMP_PREDICATE_MAP.get(kuduPredicateOperatorName), operatorJson.get(2)));
      } else if ("NULL".equals(kuduPredicateOperatorName)) {
        predicates.add(KuduPredicate.newIsNullPredicate(column));
      } else if ("NOTNULL".equals(kuduPredicateOperatorName)) {
        predicates.add(KuduPredicate.newIsNotNullPredicate(column));
      } else if ("IN".equals(kuduPredicateOperatorName)) {
        if (operatorJson.size() < KUDU_PREDICATE_JSON_COMP_OR_IN_OPERAND_NUM) {
          throw new BitSailException(KuduErrorCode.CONFIG_ERROR, "'IN' predicate need one operand");
        }
        JSONArray inElements = operatorJson.getJSONArray(2);
        List<Object> values = parseInPredicateOperandFromConfig(inElements, column);
        if (values.isEmpty()) {
          throw new BitSailException(KuduErrorCode.CONFIG_ERROR, "'IN' predicate config error");
        }
        predicates.add(KuduPredicate.newInListPredicate(column, values));
      } else {
        throw new BitSailException(KuduErrorCode.CONFIG_ERROR, String.format("'%s' predicate not support", kuduPredicateOperatorName));
      }
    }
    return predicates;
  }

  private static List<Object> parseInPredicateOperandFromConfig(JSONArray inElements, ColumnSchema kuduColumn) {
    List<Object> values = new ArrayList<>();
    switch (kuduColumn.getType()) {
      case BOOL:
        for (int i = 0; i < inElements.size(); i++) {
          values.add(inElements.getBooleanValue(i));
        }
        break;
      case INT8:
      case INT16:
      case INT32:
        for (int i = 0; i < inElements.size(); i++) {
          values.add(inElements.getIntValue(i));
        }
        break;
      case INT64:
        for (int i = 0; i < inElements.size(); i++) {
          values.add(inElements.getLongValue(i));
        }
        break;
      case UNIXTIME_MICROS:
        if (inElements.get(0) instanceof Long) {
          for (int i = 0; i < inElements.size(); i++) {
            values.add(inElements.getLongValue(i));
          }
        } else {
          LOG.warn("UNIXTIME_MICROS type for 'IN' predicate error");
          throw new BitSailException(KuduErrorCode.CONFIG_ERROR, "'IN' predicate config error");
        }
        break;
      case STRING:
        for (int i = 0; i < inElements.size(); i++) {
          values.add(inElements.getString(i));
        }
        break;
      default:
        LOG.warn("not support column type for 'IN' predicate. name='{}', type='{}'", kuduColumn.getName(), kuduColumn.getType());
        throw new BitSailException(KuduErrorCode.CONFIG_ERROR, "'IN' predicate config error");
    }
    return values;
  }
}