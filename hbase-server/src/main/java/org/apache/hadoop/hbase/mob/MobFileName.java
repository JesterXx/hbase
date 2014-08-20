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

import org.apache.hadoop.hbase.util.Bytes;

/**
 * The mob file name.
 * It consists of a checksum of a start key, a date and an uuid.
 * It looks like checksum(start) + date + uuid.
 * <ol>
 * <li>0-7 characters: checksum of a start key. Since the length of the start key is not
 * fixed, have to use the checksum instead which has a fix length.</li>
 * <li>8-15 characters: a string of a date with format yyyymmdd. The date is the latest timestamp
 * of cells in this file</li>
 * <li>the remaining characters: the uuid.</li>
 * </ol>
 * Has the checksum of the start key in the file name in order to keep region information in the
 * file name. This might be useful in bulkload to minimize the target regions.
 * The cells come from different regions might be in the same mob file, this is allowed.
 * Has the latest timestamp of cells in the file name in order to clean the expired mob files by
 * TTL easily. If this timestamp is older than the TTL, it's regarded as expired.
 */
public class MobFileName {

  private final String date;
  private final int startKey;
  private final String uuid;
  private String fileName;

  private final static char[] digits = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a',
      'b', 'c', 'd', 'e', 'f' };

  /**
   * @param startKey
   *          The checksum of the start key.
   * @param date
   *          The string of the latest timestamp of cells in this file, the format is yyyymmdd.
   * @param uuid
   *          The uuid
   */
  private MobFileName(int startKey, String date, String uuid) {
    this.startKey = startKey;
    this.uuid = uuid;
    this.date = date;
    this.fileName = MobUtils.int2HexString(startKey) + date + uuid;
  }

  /**
   * Creates an instance of MobFileName
   * 
   * @param startKey
   *          The start key.
   * @param date
   *          The string of the latest timestamp of cells in this file, the format is yyyymmdd.
   * @param uuid The uuid.
   * @return An instance of a MobFileName.
   */
  public static MobFileName create(String startKey, String date, String uuid) {
    return new MobFileName(MobUtils.hexString2Int(startKey), date, uuid);
  }

  /**
   * Creates an instance of MobFileName.
   * @param fileName The string format of a file name.
   * @return An instance of a MobFileName.
   */
  public static MobFileName create(String fileName) {
    int startKey = MobUtils.hexString2Int(fileName.substring(0, 8));
    String date = fileName.substring(8, 16);
    String uuid = fileName.substring(16);
    return new MobFileName(startKey, date, uuid);
  }

  /**
   * Gets the hex string of the checksum for a start key.
   * @return The hex string of the checksum for a start key.
   */
  public String getStartKey() {
    return MobUtils.int2HexString(startKey);
  }

  /**
   * Gets the date string. Its format is yyyymmdd.
   * @return The date string.
   */
  public String getDate() {
    return this.date;
  }

  @Override
  public int hashCode() {
    StringBuilder builder = new StringBuilder();
    builder.append(startKey);
    builder.append(date);
    builder.append(uuid);
    return builder.toString().hashCode();
  }

  @Override
  public boolean equals(Object anObject) {
    if (this == anObject) {
      return true;
    }
    if (anObject instanceof MobFileName) {
      MobFileName another = (MobFileName) anObject;
      if (this.startKey == another.startKey && this.date.equals(another.date)
          && this.uuid.equals(another.uuid)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Gets the file name.
   * @return The file name.
   */
  public String getFileName() {
    return this.fileName;
  }
}
