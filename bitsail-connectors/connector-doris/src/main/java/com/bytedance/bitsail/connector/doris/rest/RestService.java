/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.connector.doris.rest;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.connector.doris.config.DorisExecutionOptions;
import com.bytedance.bitsail.connector.doris.config.DorisOptions;
import com.bytedance.bitsail.connector.doris.error.DorisErrorCode;
import com.bytedance.bitsail.connector.doris.rest.model.Backend;
import com.bytedance.bitsail.connector.doris.rest.model.Backend.BackendRow;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RestService {

  private static final String BACKENDS = "/api/backends?is_alive=true";
  private static List<Backend.BackendRow> backendRows;
  private static long pos;

  public static String getBackend(DorisOptions options, DorisExecutionOptions executionOptions, Logger logger) {
    try {
      return randomBackend(options, executionOptions, logger);
    } catch (Exception e) {
      throw BitSailException.asBitSailException(DorisErrorCode.GET_BACKEND_FAILED, "Failed to get backend" + options.getFeNodes(), e);
    }
  }

  /**
   * choice a Doris BE node to request.
   *
   * @param options configuration of request
   * @param logger  slf4j logger
   * @return the chosen one Doris BE node
   * @throws IllegalArgumentException BE nodes is illegal
   */
  @VisibleForTesting
  public static String randomBackend(DorisOptions options, DorisExecutionOptions executionOptions, Logger logger) throws IOException {
    List<BackendRow> backends = getBackends(options, executionOptions, logger);
    backendRows = backends;
    logger.trace("Parse beNodes '{}'.", backends);
    if (backends == null || backends.isEmpty()) {
      String errMsg = String.format("illegal argument message, beNodes=%s", backends);
      logger.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
    Collections.shuffle(backends);
    BackendRow backend = backends.get(0);
    return backend.getIp() + ":" + backend.getHttpPort();
  }

  @VisibleForTesting
  static List<Backend.BackendRow> getBackends(DorisOptions options, DorisExecutionOptions executionOptions, Logger logger) {
    String feNodes = options.getFeNodes();
    List<String> feNodeList = allEndpoints(feNodes, logger);
    for (String feNode : feNodeList) {
      try {
        String beUrl = "http://" + feNode + BACKENDS;
        HttpGet httpGet = new HttpGet(beUrl);
        String response = send(options, executionOptions, httpGet, logger);
        logger.info("Backend Info:{}", response);
        return parseBackend(response, logger);
      } catch (Exception e) {
        logger.info("Doris FE node {} is unavailable: {}, Request the next Doris FE node", feNode, e.getMessage());
      }
    }
    String errMsg = "No Doris FE is available, please check configuration";
    logger.error(errMsg);
    throw new BitSailException(DorisErrorCode.FE_NOT_AVAILABLE, errMsg);
  }

  public static String getAvailableHost() {
    long tmp = pos + backendRows.size();
    if (pos < tmp) {
      BackendRow backend = backendRows.get((int) (pos % backendRows.size()));
      pos++;
      return backend.getIp() + ":" + backend.getHttpPort();
    }
    return null;
  }

  static List<Backend.BackendRow> parseBackend(String response, Logger logger) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    Backend backend;
    try {
      backend = mapper.readValue(response, Backend.class);
    } catch (JsonParseException e) {
      String errMsg = "Doris BE's response is not a json. res: " + response;
      logger.error(errMsg, e);
      throw BitSailException.asBitSailException(DorisErrorCode.PARSE_FAILED, errMsg, e);
    } catch (JsonMappingException e) {
      String errMsg = "Doris BE's response cannot map to schema. res: " + response;
      logger.error(errMsg, e);
      throw BitSailException.asBitSailException(DorisErrorCode.PARSE_FAILED, errMsg, e);
    } catch (IOException e) {
      String errMsg = "Parse Doris BE's response to json failed. res: " + response;
      logger.error(errMsg, e);
      throw BitSailException.asBitSailException(DorisErrorCode.PARSE_FAILED, errMsg, e);
    }

    List<Backend.BackendRow> backendRows = backend.getBackends();
    logger.debug("Parsing schema result is '{}'.", backendRows);
    return backendRows;
  }


  /**
   * choice a Doris FE node to request.
   *
   * @param feNodes Doris FE node list, separate be comma
   * @param logger  slf4j logger
   * @return the array of Doris FE nodes
   * @throws IllegalArgumentException fe nodes is illegal
   */
  @VisibleForTesting
  static List<String> allEndpoints(String feNodes, Logger logger) throws IllegalArgumentException {
    logger.trace("Parse fenodes '{}'.", feNodes);
    if (StringUtils.isEmpty(feNodes)) {
      String errMsg = String.format("illegal argument message, fenodes=%s", feNodes);
      logger.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
    List<String> nodes = Arrays.stream(feNodes.split(",")).map(String::trim).collect(Collectors.toList());
    Collections.shuffle(nodes);
    return nodes;
  }

  /**
   * send request to Doris FE and get response json string.
   *
   * @param dorisOptions configuration of request
   * @param request      {@link HttpRequestBase} real request
   * @param logger       {@link Logger}
   * @return Doris FE response in json string
   */
  private static String send(DorisOptions dorisOptions, DorisExecutionOptions executionOptions, HttpRequestBase request, Logger logger) throws BitSailException {
    int connectTimeout = executionOptions.getRequestConnectTimeoutMs();
    int socketTimeout = executionOptions.getRequestReadTimeoutMs();
    logger.trace("connect timeout set to '{}'. socket timeout set to '{}'.", connectTimeout, socketTimeout);

    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(connectTimeout)
        .setSocketTimeout(socketTimeout)
        .build();

    request.setConfig(requestConfig);
    logger.info("Send request to Doris FE '{}' with user '{}'.", request.getURI(), dorisOptions.getUsername());
    IOException ex = null;
    int statusCode = -1;

    for (int attempt = 0; attempt < executionOptions.getRequestRetries(); attempt++) {
      logger.debug("Attempt {} to request {}.", attempt, request.getURI());
      try {
        String response;
        if (request instanceof HttpGet) {
          response = getConnectionGet(request.getURI().toString(), dorisOptions.getUsername(), dorisOptions.getPassword(), logger);
        } else {
          response = getConnectionPost(request, dorisOptions.getUsername(), dorisOptions.getPassword(), logger);
        }
        if (response == null) {
          logger.warn("Failed to get response from Doris FE {}, http code is {}",
              request.getURI(), statusCode);
          continue;
        }
        logger.trace("Success get response from Doris FE: {}, response is: {}.", request.getURI(), response);
        //Handle the problem of inconsistent data format returned by http v1 and v2
        ObjectMapper mapper = new ObjectMapper();
        Map map = mapper.readValue(response, Map.class);
        if (map.containsKey("code") && map.containsKey("msg")) {
          Object data = map.get("data");
          return mapper.writeValueAsString(data);
        } else {
          return response;
        }
      } catch (IOException e) {
        ex = e;
        logger.warn("Failed to connect message, url={}", request.getURI(), e);
      }
    }

    String errMsg = String.format("Failed to connected doris, url=%s, statusCode=%s", request.getURI(), statusCode);
    logger.error(errMsg, ex);
    throw BitSailException.asBitSailException(DorisErrorCode.CONNECTED_FAILED, errMsg, ex);
  }

  private static String getConnectionPost(HttpRequestBase request, String user, String passwd, Logger logger) throws IOException {
    URL url = new URL(request.getURI().toString());
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setInstanceFollowRedirects(false);
    conn.setRequestMethod(request.getMethod());
    String authEncoding = Base64.getEncoder().encodeToString(String.format("%s:%s", user, passwd)
        .getBytes(StandardCharsets.UTF_8));
    conn.setRequestProperty("Authorization", "Basic " + authEncoding);
    InputStream content = ((HttpPost) request).getEntity().getContent();
    String res = IOUtils.toString(content);
    conn.setDoOutput(true);
    conn.setDoInput(true);
    PrintWriter out = new PrintWriter(conn.getOutputStream());
    // send request params
    out.print(res);
    // flush
    out.flush();
    // read response
    return parseResponse(conn, logger);
  }

  private static String getConnectionGet(String request, String user, String passwd, Logger logger) throws IOException {
    URL realUrl = new URL(request);
    // open connection
    HttpURLConnection connection = (HttpURLConnection) realUrl.openConnection();
    String authEncoding = Base64.getEncoder().encodeToString(String.format("%s:%s", user, passwd).getBytes(StandardCharsets.UTF_8));
    connection.setRequestProperty("Authorization", "Basic " + authEncoding);

    connection.connect();
    return parseResponse(connection, logger);
  }

  private static String parseResponse(HttpURLConnection connection, Logger logger) throws IOException {
    if (connection.getResponseCode() != HttpStatus.SC_OK) {
      logger.warn("Failed to get response from Doris  {}, http code is {}", connection.getURL(), connection.getResponseCode());
      throw new IOException("Failed to get response from Doris");
    }
    String result = "";
    BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8));
    String line;
    while ((line = in.readLine()) != null) {
      result += line;
    }
    if (in != null) {
      in.close();
    }
    return result;
  }

}
