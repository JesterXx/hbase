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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.MobFileStore;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.Strings;

public class MobUtils {

  private static final Log LOG = LogFactory.getLog(MobUtils.class);
  public final static String TMP = ".tmp";

  private static final ThreadLocal<SimpleDateFormat> LOCAL_FORMAT = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyyMMdd");
    }
  };

  public static boolean isMobFamily(HColumnDescriptor hcd) {
    String is_mob = hcd.getValue(MobConstants.IS_MOB);
    if (is_mob != null) {
      return Boolean.parseBoolean(is_mob);
    }
    return false;
  }

  public static HColumnDescriptor getColumnDescriptor(FileSystem fs, Path homeDir,
      TableName tableName, String familyName) throws IOException {
    TableDescriptors htds = new FSTableDescriptors(fs, homeDir.getParent());
    HTableDescriptor htd = htds.get(tableName);
    return htd == null ? null : htd.getFamily(Bytes.toBytes(familyName));
  }

  public static String formatDate(Date date) {
    return LOCAL_FORMAT.get().format(date);
  }

  public static Date parseDate(String dateString) throws ParseException {
    return LOCAL_FORMAT.get().parse(dateString);
  }

  public static boolean isMobReferenceKeyValue(KeyValue kv) {
    boolean isMob = false;
    List<Tag> tags = kv.getTags();
    if (!tags.isEmpty()) {
      for (Tag tag : tags) {
        if (tag.getType() == MobConstants.MOB_REFERENCE_TAG_TYPE) {
          isMob = true;
          break;
        }
      }
    }
    return isMob;
  }

  public static boolean isRawMobScan(Scan scan) {
    byte[] raw = scan.getAttribute(MobConstants.MOB_SCAN_RAW);
    try {
      return raw == null ? false : Bytes.toBoolean(raw);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  public static boolean isCacheMobBlocks(Scan scan) {
    byte[] cache = scan.getAttribute(MobConstants.MOB_CACHE_BLOCKS);
    try {
      return cache == null ? false : Bytes.toBoolean(cache);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  public static void setCacheMobBlocks(Scan scan, boolean cacheBlocks) {
    scan.setAttribute(MobConstants.MOB_CACHE_BLOCKS, Bytes.toBytes(cacheBlocks));
  }

  public static void cleanExpiredData(FileSystem fs, MobFileStore store) throws IOException {
    cleanExpiredDate(fs, store, new Date());
  }

  public static void cleanExpiredDate(FileSystem fs, MobFileStore store, Date current)
      throws IOException {
    HColumnDescriptor columnDescriptor = store.getColumnDescriptor();

    if (current == null) {
      current = new Date();
    }
    // Here the default value of columnDescriptor.getTimeToLive() is
    // Integer.MAX_VALUE
    // It must be parsed as long before computing in case of overflow
    long timeToLive = columnDescriptor.getTimeToLive();

    if (Integer.MAX_VALUE == timeToLive) {
      // no need to clean, because the TTL is not set.
      return;
    }

    Date expireDate = new Date(current.getTime() - timeToLive * 1000);
    expireDate = new Date(expireDate.getYear(), expireDate.getMonth(), expireDate.getDate());
    LOG.info("[MOB] File before " + expireDate.toGMTString() + " will be deleted!");

    FileStatus[] stats = fs.listStatus(store.getHomePath());
    if (null == stats) {
      // no file found
      return;
    }
    for (int i = 0; i < stats.length; i++) {
      FileStatus file = stats[i];
      if (!file.isDirectory()) {
        continue;
      }
      String fileName = file.getPath().getName();
      if (".tmp".equals(fileName)) {
        continue;
      }
      try {
        Date fileDate = parseDate(fileName);
        LOG.info("[MOB] Checking folder " + fileName);
        if (fileDate.getTime() < expireDate.getTime()) {
          LOG.info("[MOB] Delete expired folder " + fileName);
          fs.delete(file.getPath(), true);
        }
      } catch (ParseException e) {
        LOG.error("Cannot parse the fileName as date" + fileName, e);
        continue;
      }
    }
  }

  public static String getStoreLockName(String tableName, String columnName) {
    return tableName + ":" + columnName;
  }

  public static Path getMobHome(Configuration conf) {
    String mobRootdir = conf.get(MobConstants.MOB_ROOTDIR);
    Path mobHome;
    if (Strings.isEmpty(mobRootdir)) {
      Path hbaseDir = new Path(conf.get(HConstants.HBASE_DIR));
      mobHome = new Path(hbaseDir, MobConstants.DEFAULT_MOB_ROOTDIR_NAME);
    } else {
      mobHome = new Path(mobRootdir);
    }
    return mobHome;
  }

  public static Path getCompactionOutputPath(String root, String jobName) {
    Path parent = new Path(root, jobName);
    return new Path(parent, "output");
  }

  public static Path getCompactionWorkingPath(String root, String jobName) {
    Path parent = new Path(root, jobName);
    return new Path(parent, "working");
  }
}
