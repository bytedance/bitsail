package com.bytedance.bitsail.connector.mongodb.constant;

/**
 * Split mode for MongoDB
 */
public class MongoDBSplitMode {
  /**
   * Default split strategy: totalRecords / batchSize
   */
  public static final String PAGINATING = "paginating";

  /**
   * parallelism split strategy: totalRecords / parallelism
   */
  public static final String PARALLELISM = "parallelism";
}
