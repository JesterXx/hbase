/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.mob;

import java.security.InvalidParameterException;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;

public class MobFilePath {

  private String date;
  private int startKey;
  private String uuid;
  private long count;

  static public MobFilePath create(String startKey, long count, Date date, String uuid) {
    String dateString = null;
    if (null != date) {
      dateString = MobUtils.formatDate(date);
    }
    return new MobFilePath(dateString, startKey, count, uuid);
  }

  static public MobFilePath create(String filePath) {
    int slashPosition = filePath.indexOf(Path.SEPARATOR);
    String parent = null;
    String fileName = null;
    if (-1 != slashPosition) {
      parent = filePath.substring(0, slashPosition);
      fileName = filePath.substring(slashPosition + 1);
    } else {
      fileName = filePath;
    }
    return MobFilePath.create(parent, fileName);
  }

  static public MobFilePath create(String parentName, String fileName) {
    String date = parentName;
    int startKey = hexString2Int(fileName.substring(0, 8));
    int count = hexString2Int(fileName.substring(8, 16));
    String uuid = fileName.substring(16);
    return new MobFilePath(date, startKey, count, uuid);
  }

  public static String int2HexString(int i) {
    int shift = 4;
    char[] buf = new char[8];

    int charPos = 8;
    int radix = 1 << shift;
    int mask = radix - 1;
    do {
      buf[--charPos] = digits[i & mask];
      i >>>= shift;
    } while (charPos > 0);

    return new String(buf);
  }

  public static int hexString2Int(String hex) {
    byte[] buffer = Bytes.toBytes(hex);
    if (buffer.length != 8) {
      throw new InvalidParameterException("hexString2Int length not valid");
    }

    for (int i = 0; i < buffer.length; i++) {
      byte ch = buffer[i];
      if (ch >= 'a' && ch <= 'z') {
        buffer[i] = (byte) (ch - 'a' + 10);
      } else {
        buffer[i] = (byte) (ch - '0');
      }
    }

    buffer[0] = (byte) ((buffer[0] << 4) ^ buffer[1]);
    buffer[1] = (byte) ((buffer[2] << 4) ^ buffer[3]);
    buffer[2] = (byte) ((buffer[4] << 4) ^ buffer[5]);
    buffer[3] = (byte) ((buffer[6] << 4) ^ buffer[7]);
    return Bytes.toInt(buffer, 0, 4);
  }

  final static char[] digits = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c',
      'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u',
      'v', 'w', 'x', 'y', 'z' };

  public MobFilePath(String date, String startKey, long count, String uuid) {
    this(date, hexString2Int(startKey), count, uuid);
  }

  public MobFilePath(String date, int startKey, long count, String uuid) {

    this.startKey = startKey;
    this.count = count;
    this.uuid = uuid;
    this.date = date;
  }

  public String getStartKey() {
    return int2HexString(startKey);
  }

  public String getDate() {
    return this.date;
  }

  @Override
  public int hashCode() {
    StringBuilder builder = new StringBuilder();
    builder.append(date);
    builder.append(startKey);
    builder.append(uuid);
    builder.append(count);
    return builder.toString().hashCode();
  }

  @Override
  public boolean equals(Object anObject) {
    if (this == anObject) {
      return true;
    }
    if (anObject instanceof MobFilePath) {
      MobFilePath another = (MobFilePath) anObject;
      if (this.date.equals(another.date) && this.startKey == another.startKey
          && this.uuid.equals(another.uuid) && this.count == another.count) {
        return true;
      }
    }
    return false;
  }

  public long getRecordCount() {
    return this.count;
  }

  public Path getAbsolutePath(Path rootPath) {
    if (null == date) {
      return new Path(rootPath, getFileName());
    } else {
      return new Path(rootPath, this.date + Path.SEPARATOR + getFileName());
    }
  }

  public String getFileName() {
    return int2HexString(this.startKey) + Long.toHexString(this.count) + this.uuid;
  }
}
