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

package com.bytedance.bitsail.connector.legacy.streamingfile.common.filesystem;

import com.bytedance.bitsail.connector.legacy.streamingfile.common.validator.StreamingFileSystemValidator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utils for file system.
 */
@SuppressWarnings({"checkstyle:MagicNumber", "checkstyle:IllegalTokenText"})
public class PartitionPathUtils {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionPathUtils.class);

  private static final Pattern PARTITION_NAME_PATTERN = Pattern.compile("([^/]+)=([^/]+)");

  private static final BitSet CHAR_TO_ESCAPE = new BitSet(128);

  static {
    for (char c = 0; c < ' '; c++) {
      CHAR_TO_ESCAPE.set(c);
    }

    /*
     * ASCII 01-1F are HTTP control characters that need to be escaped.
     * \u000A and \u000D are \n and \r, respectively.
     */
    char[] charList = new char[] {'\u0001', '\u0002', '\u0003', '\u0004',
        '\u0005', '\u0006', '\u0007', '\u0008', '\u0009', '\n', '\u000B',
        '\u000C', '\r', '\u000E', '\u000F', '\u0010', '\u0011', '\u0012',
        '\u0013', '\u0014', '\u0015', '\u0016', '\u0017', '\u0018', '\u0019',
        '\u001A', '\u001B', '\u001C', '\u001D', '\u001E', '\u001F',
        '"', '#', '%', '\'', '*', '/', ':', '=', '?', '\\', '\u007F', '{',
        '[', ']', '^'};

    for (char c : charList) {
      CHAR_TO_ESCAPE.set(c);
    }
  }

  private static boolean needsEscaping(char c) {
    return c < CHAR_TO_ESCAPE.size() && CHAR_TO_ESCAPE.get(c);
  }

  /**
   * Make partition path from partition spec.
   *
   * @param partitionSpec The partition spec.
   * @return An escaped, valid partition name.
   */
  public static String generatePartitionPath(LinkedHashMap<String, String> partitionSpec) {
    StringBuilder suffixBuf = new StringBuilder();
    int i = 0;
    for (Map.Entry<String, String> e : partitionSpec.entrySet()) {
      if (i > 0) {
        suffixBuf.append(Path.SEPARATOR);
      }
      suffixBuf.append(escapePathName(e.getKey()));
      suffixBuf.append('=');
      suffixBuf.append(escapePathName(e.getValue()));
      i++;
    }
    suffixBuf.append(Path.SEPARATOR);
    return suffixBuf.toString();
  }

  /**
   * Escapes a path name.
   *
   * @param path The path to escape.
   * @return An escaped path name.
   */
  private static String escapePathName(String path) {
    if (path == null || path.length() == 0) {
      throw new TableException("Path should not be null or empty: " + path);
    }

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (needsEscaping(c)) {
        sb.append('%');
        sb.append(String.format("%1$02X", (int) c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  /**
   * Make partition spec from path.
   *
   * @param currPath partition file path.
   * @return Sequential partition specs.
   */
  public static LinkedHashMap<String, String> extractPartitionSpecFromPath(Path currPath) {
    LinkedHashMap<String, String> fullPartSpec = new LinkedHashMap<>();
    List<String[]> kvs = new ArrayList<>();
    do {
      String component = currPath.getName();
      Matcher m = PARTITION_NAME_PATTERN.matcher(component);
      if (m.matches()) {
        String k = unescapePathName(m.group(1));
        String v = unescapePathName(m.group(2));
        String[] kv = new String[2];
        kv[0] = k;
        kv[1] = v;
        kvs.add(kv);
      }
      currPath = currPath.getParent();
    } while (currPath != null && !currPath.getName().isEmpty());

    // reverse the list since we checked the part from leaf dir to table's base dir
    for (int i = kvs.size(); i > 0; i--) {
      fullPartSpec.put(kvs.get(i - 1)[0], kvs.get(i - 1)[1]);
    }

    return fullPartSpec;
  }

  private static String unescapePathName(String path) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (c == '%' && i + 2 < path.length()) {
        int code = -1;
        try {
          code = Integer.parseInt(path.substring(i + 1, i + 3), 16);
        } catch (Exception ignored) {
          //
        }
        if (code >= 0) {
          sb.append((char) code);
          i += 2;
          continue;
        }
      }
      sb.append(c);
    }
    return sb.toString();
  }

  /**
   * List file status without hidden files.
   */
  public static FileStatus[] listStatusWithoutHidden(FileSystem fs, Path dir) throws IOException {
    FileStatus[] statuses = fs.listStatus(dir);
    if (statuses == null) {
      return null;
    }
    return Arrays.stream(statuses).filter(fileStatus -> {
      String name = fileStatus.getPath().getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }).toArray(FileStatus[]::new);
  }

  /**
   * Search all partitions in this path.
   *
   * @param path            search path.
   * @param partitionNumber partition number, it will affect path structure.
   * @return all partition specs to its path.
   */
  public static List<Tuple2<LinkedHashMap<String, String>, Path>> searchPartSpecAndPaths(
      FileSystem fs, Path path, int partitionNumber) {
    FileStatus[] generatedParts = getFileStatusRecurse(path, partitionNumber, fs);
    List<Tuple2<LinkedHashMap<String, String>, Path>> ret = new ArrayList<>();
    for (FileStatus part : generatedParts) {
      ret.add(new Tuple2<>(extractPartitionSpecFromPath(part.getPath()), part.getPath()));
    }
    return ret;
  }

  public static List<Path> searchPaths(FileSystem fs, Path path, int partitionNumber) {
    FileStatus[] generatedParts = getFileStatusRecurse(path, partitionNumber, fs);
    return Arrays.stream(generatedParts)
        .map(FileStatus::getPath)
        .collect(Collectors.toList());
  }

  private static FileStatus[] getFileStatusRecurse(Path path, int expectLevel, FileSystem fs) {
    ArrayList<FileStatus> result = new ArrayList<>();

    try {
      FileStatus fileStatus = fs.getFileStatus(path);
      listStatusRecursively(fs, fileStatus, 0, expectLevel, result);
    } catch (IOException e) {
      LOG.error("list parent path: {} status recursively failed.", path, e);
      throw new IllegalStateException(e);
    }

    return result.toArray(new FileStatus[0]);
  }

  private static void listStatusRecursively(
      FileSystem fs,
      FileStatus fileStatus,
      int level,
      int expectLevel,
      List<FileStatus> results) throws IOException {
    if (expectLevel == level) {
      results.add(fileStatus);
      return;
    }

    if (fileStatus.isDir()) {
      for (FileStatus stat : fs.listStatus(fileStatus.getPath())) {
        if (!StreamingFileSystemValidator.DUMP_DEFAULT_KEYWORD.contains(stat.getPath().getName())) {
          listStatusRecursively(fs, stat, level + 1, expectLevel, results);
        }
      }
    }
  }
}
