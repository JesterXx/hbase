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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;

/**
 * The mob utilities
 */
@InterfaceAudience.Private
public class MobUtils {

  private static final ThreadLocal<SimpleDateFormat> LOCAL_FORMAT =
      new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyyMMdd");
    }
  };

  /**
   * Indicates whether the column family is a mob one.
   * @param hcd The descriptor of a column family.
   * @return True if this column family is a mob one, false if it's not.
   */
  public static boolean isMobFamily(HColumnDescriptor hcd) {
    String isMob = hcd.getValue(MobConstants.IS_MOB);
    return isMob != null && Boolean.parseBoolean(isMob);
  }

  /**
   * Gets the mob threshold.
   * If the size of a cell value is larger than this threshold, it's regarded as a mob.
   * @param hcd The descriptor of a column family.
   * @return The threshold.
   */
  public static long getMobThreshold(HColumnDescriptor hcd) {
    String threshold = hcd.getValue(MobConstants.MOB_THRESHOLD);
    return Strings.isEmpty(threshold) ? 0 : Long.parseLong(threshold);
  }

  /**
   * Formats a date to a string.
   * @param date The date.
   * @return The string format of the date, it's yyyymmdd.
   */
  public static String formatDate(Date date) {
    return LOCAL_FORMAT.get().format(date);
  }

  /**
   * Parses the string to a date.
   * @param dateString The string format of a date, it's yyyymmdd.
   * @return A date.
   * @throws ParseException
   */
  public static Date parseDate(String dateString) throws ParseException {
    return LOCAL_FORMAT.get().parse(dateString);
  }

  /**
   * Whether the current cell is a mob reference cell.
   * @param cell The current cell.
   * @return True if the cell has a mob reference tag, false if it doesn't.
   */
  public static boolean isMobReferenceCell(Cell cell) {
    List<Tag> tags = Tag.asList(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength());
    return hasMobReferenceTag(tags);
  }

  /**
   * Whether the tag list has a mob reference tag.
   * @param tags The tag list.
   * @return True if the list has a mob reference tag, false if it doesn't.
   */
  public static boolean hasMobReferenceTag(List<Tag> tags) {
    boolean isMob = false;
    if (!tags.isEmpty()) {
      for (Tag tag : tags) {
        if (tag.getType() == TagType.MOB_REFERENCE_TAG_TYPE) {
          isMob = true;
          break;
        }
      }
    }
    return isMob;
  }

  /**
   * Indicates whether it's a raw scan.
   * The information is set in the attribute "hbase.mob.scan.raw" of scan.
   * For a mob cell, in a normal scan the scanners retrieves the mob cell from the mob file.
   * In a raw scan, the scanner directly returns cell in HBase without retrieve the one in
   * the mob file.
   * @param scan The current scan.
   * @return True if it's a raw scan.
   */
  public static boolean isRawMobScan(Scan scan) {
    byte[] raw = scan.getAttribute(MobConstants.MOB_SCAN_RAW);
    try {
      return raw != null && Bytes.toBoolean(raw);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Indicates whether the scan contains the information of caching blocks.
   * The information is set in the attribute "hbase.mob.cache.blocks" of scan.
   * @param scan The current scan.
   * @return True if the scan contains the information of caching blocks.
   */
  public static boolean isCacheMobBlocks(Scan scan) {
    byte[] cache = scan.getAttribute(MobConstants.MOB_CACHE_BLOCKS);
    try {
      return cache != null && Bytes.toBoolean(cache);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Sets the attribute of caching blocks in the scan.
   * 
   * @param scan
   *          The current scan.
   * @param cacheBlocks
   *          True, set the attribute of caching blocks into the scan, the scanner with this scan
   *          caches blocks.
   */
  public static void setCacheMobBlocks(Scan scan, boolean cacheBlocks) {
    scan.setAttribute(MobConstants.MOB_CACHE_BLOCKS, Bytes.toBytes(cacheBlocks));
  }

  /**
   * Gets the root dir of the mob files.
   * It's {HBASE_DIR}/mobdir.
   * @param conf The current configuration.
   * @return the root dir of the mob file.
   */
  public static Path getMobHome(Configuration conf) {
    Path hbaseDir = new Path(conf.get(HConstants.HBASE_DIR));
    return new Path(hbaseDir, MobConstants.MOB_DIR_NAME);
  }

  /**
   * Gets the region dir of the mob files.
   * It's {HBASE_DIR}/mobdir/{namespace}/{tableName}/.mob.
   * @param conf The current configuration.
   * @param tableName The current table name.
   * @return The region dir of the mob files.
   */
  public static Path getMobRegionPath(Configuration conf, TableName tableName) {
    Path tablePath = new Path(MobUtils.getMobHome(conf), new Path(tableName.getNamespaceAsString(),
        tableName.getQualifierAsString()));
    return new Path(tablePath, MobConstants.MOB_REGION_NAME);
  }

  /**
   * Gets the absolute path according to the root path and file name.
   * 
   * @param rootPath
   *          The root path.
   * @param The
   *          file name.
   * @return The absolute file path.
   */
  public static Path getAbsolutePath(Path rootPath, String fileName) {
    return new Path(rootPath, fileName);
  }
}
