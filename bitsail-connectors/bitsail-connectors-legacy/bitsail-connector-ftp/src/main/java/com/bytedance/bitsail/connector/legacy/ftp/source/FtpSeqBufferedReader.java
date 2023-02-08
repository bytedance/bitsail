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

package com.bytedance.bitsail.connector.legacy.ftp.source;

import com.bytedance.bitsail.connector.legacy.ftp.client.FtpHandler;
import com.bytedance.bitsail.connector.legacy.ftp.client.IFtpHandler;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

@Slf4j
public class FtpSeqBufferedReader {
  private IFtpHandler ftpHandler;
  private Iterator<String> iter;
  private int fromLine = 0;
  private BufferedReader br;
  private String charsetName = "utf-8";

  public FtpSeqBufferedReader(IFtpHandler ftpHandler, Iterator<String> iter) {
    this.ftpHandler = ftpHandler;
    this.iter = iter;
  }

  public String readLine() throws IOException {
    if (br == null) {
      nextStream();
    }

    if (br != null) {
      String line = br.readLine();
      if (line == null) {
        close();
        return readLine();
      }

      return line;
    } else {
      return null;
    }
  }

  private void nextStream() throws IOException {
    if (iter.hasNext()) {
      String file = iter.next();
      InputStream in = ftpHandler.getInputStream(file);
      if (in == null) {
        throw new NullPointerException();
      }

      br = new BufferedReader(new InputStreamReader(in, charsetName));

      for (int i = 0; i < fromLine; i++) {
        String skipLine = br.readLine();
        log.info("Skip line:{}", skipLine);
      }
    } else {
      br = null;
    }
  }

  public void close() throws IOException {
    if (br != null) {
      br.close();
      br = null;

      if (ftpHandler instanceof FtpHandler) {
        ((FtpHandler) ftpHandler).getFtpClient().completePendingCommand();
      }
    }
  }

  public void setFromLine(int fromLine) {
    this.fromLine = fromLine;
  }

  public void setCharsetName(String charsetName) {
    this.charsetName = charsetName;
  }
}
