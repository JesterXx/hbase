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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.Strings;

/**
 * The mob utilities
 */
@InterfaceAudience.Private
public class MobUtils {

  private static final Log LOG = LogFactory.getLog(MobUtils.class);

  private static final ThreadLocal<SimpleDateFormat> LOCAL_FORMAT =
      new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyyMMdd");
    }
  };
  private final static char[] digits = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a',
      'b', 'c', 'd', 'e', 'f' };

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
   * Gets the qualified root dir of the mob files.
   * @param conf The current configuration.
   * @return The qualified root dir.
   * @throws IOException
   */
  public static Path getQualifiedMobRootDir(Configuration conf) throws IOException {
    Path hbaseDir = new Path(conf.get(HConstants.HBASE_DIR));
    Path mobRootDir = new Path(hbaseDir, MobConstants.MOB_DIR_NAME);
    FileSystem fs = mobRootDir.getFileSystem(conf);
    return mobRootDir.makeQualified(fs);
  }

  /**
   * Gets the region dir of the mob files.
   * It's {HBASE_DIR}/mobdir/{namespace}/{tableName}/{regionEncodedName}.
   * @param conf The current configuration.
   * @param tableName The current table name.
   * @return The region dir of the mob files.
   */
  public static Path getMobRegionPath(Configuration conf, TableName tableName) {
    Path tablePath = FSUtils.getTableDir(MobUtils.getMobHome(conf), tableName);
    HRegionInfo regionInfo = getMobRegionInfo(tableName);
    return new Path(tablePath, regionInfo.getEncodedName());
  }

  /**
   * Gets the family dir of the mob files.
   * It's {HBASE_DIR}/mobdir/{namespace}/{tableName}/{regionEncodedName}/{columnFamilyName}.
   * @param conf The current configuration.
   * @param tableName The current table name.
   * @param familyName The current family name.
   * @return The family dir of the mob files.
   */
  public static Path getMobFamilyPath(Configuration conf, TableName tableName, String familyName) {
    return new Path(getMobRegionPath(conf, tableName), familyName);
  }

  /**
   * Gets the family dir of the mob files.
   * It's {HBASE_DIR}/mobdir/{namespace}/{tableName}/{regionEncodedName}/{columnFamilyName}.
   * @param regionPath The path of mob region which is a dummy one.
   * @param familyName The current family name.
   * @return The family dir of the mob files.
   */
  public static Path getMobFamilyPath(Path regionPath, String familyName) {
    return new Path(regionPath, familyName);
  } 

  /**
   * Gets the HRegionInfo of the mob files.
   * This is a dummy region. The mob files are not saved in a region in HBase.
   * This is only used in mob snapshot. It's internally used only.
   * @param tableName
   * @return
   */
  public static HRegionInfo getMobRegionInfo(TableName tableName) {
    HRegionInfo info = new HRegionInfo(tableName, MobConstants.MOB_REGION_NAME_BYTES,
        HConstants.EMPTY_END_ROW, false, 0);
    return info;
  }

  /**
   * Gets whether the current HRegionInfo is a mob one.
   * @param regionInfo The current HRegionInfo.
   * @return If true, the current HRegionInfo is a mob one.
   */
  public static boolean isMobRegionInfo(HRegionInfo regionInfo) {
    return regionInfo == null ? false : getMobRegionInfo(regionInfo.getTable()).getEncodedName()
        .equals(regionInfo.getEncodedName());
  }

  /**
   * Archives the mob files.
   * @param conf The current configuration.
   * @param fs The current file system.
   * @param tableName The table name.
   * @param family The name of the column family.
   * @param storeFiles The files to be deleted.
   * @throws IOException
   */
  public static void removeMobFiles(Configuration conf, FileSystem fs, TableName tableName,
      byte[] family, Collection<StoreFile> storeFiles) throws IOException {
    HFileArchiver.archiveStoreFiles(conf, fs, getMobRegionInfo(tableName), family, storeFiles);
  }

  /**
   * Opens existing files.
   * The file to be opened might be unavailable. Instead the file in another location
   * will be opened.
   * The possible locations for a file could be either in mob directory or the archive directory.
   * @param manager The current MobFileManager.
   * @param path The path of the file to be opened.
   * @return The opened MobFile.
   * @throws IOException
   */
  public static MobFile openExistFile(MobFileManager manager, Path path) throws IOException {
    boolean findArchive = false;
    MobCacheConfig cacheConf = manager.getCacheConfig();
    FileSystem fs = manager.getFileSystem();
    try {
      return cacheConf.getMobFileCache().openFile(fs, path, cacheConf);
    } catch (IOException e) {
      if (e.getCause() instanceof FileNotFoundException) {
        logFileNotFoundException(e.getCause());
        findArchive = true;
      } else {
        throw e;
      }
    }
    if (findArchive) {
      // find from archive
      // Evict the cached file
      String fileName = path.getName();
      evictFile(cacheConf, fileName);
      Path archivePath = HFileArchiveUtil.getStoreArchivePath(manager.getConfiguration(),
          manager.getTableName(), getMobRegionInfo(manager.getTableName())
              .getEncodedName(), manager.getColumnDescriptor().getName());
      try {
        // Open and cache
        return cacheConf.getMobFileCache().openFile(fs, archivePath, cacheConf);
      } catch (IOException e) {
        if (e.getCause() instanceof FileNotFoundException) {
          logFileNotFoundException(e.getCause());
          return null;
        }
        throw e;
      }
    }
    // never come here
    return null;
  }

  /**
   * Reads the mob cells from the existing file.
   * The file to be opened might be unavailable. Instead the file in another location
   * will be opened and read.
   * The possible locations for a file could be either in mob directory or the archive directory.
   * @param manager The current MobFileManager.
   * @param file The file to be read.
   * @param search The cell to be searched.
   * @param cacheMobBlocks Whether the scanner should cache blocks.
   * @return The found cell.
   * @throws IOException
   */
  public static Cell readCellFromExistFile(MobFileManager manager, MobFile file, Cell search,
      boolean cacheMobBlocks) throws IOException {
    boolean findArchive = false;
    MobCacheConfig cacheConf = manager.getCacheConfig();
    FileSystem fs = manager.getFileSystem();
    try {
      return file.readCell(search, cacheMobBlocks);
    } catch (IOException e) {
      if (e.getCause() instanceof FileNotFoundException) {
        logFileNotFoundException(e.getCause());
        findArchive = true;
      }
      throw e;
    } catch (NullPointerException e) {
      logNullPointerException(e);
      findArchive = true;
    }
    if (findArchive) {
      evictFile(cacheConf, file.getName());
      Path archivePath = HFileArchiveUtil.getStoreArchivePath(manager.getConfiguration(),
          manager.getTableName(), getMobRegionInfo(manager.getTableName())
              .getEncodedName(), manager.getColumnDescriptor().getName());
      try {
        MobFile archive = cacheConf.getMobFileCache().openFile(fs, archivePath, cacheConf);
        return archive.readCell(search, cacheMobBlocks);
      } catch (IOException e) {
        if (e.getCause() instanceof FileNotFoundException) {
          logFileNotFoundException(e.getCause());
          evictFile(cacheConf, file.getName());
          return null;
        }
        throw e;
      } catch (NullPointerException e) {
        logNullPointerException(e);
        evictFile(cacheConf, file.getName());
        return null;
      }
    }
    // never come here
    return null;
  }

  /**
   * Logs the exception.
   * @param e The exception to be logged.
   */
  private static void logNullPointerException(Throwable e) {
    // When delete the file during the scan, the hdfs getBlockRange will
    // throw NullPointerException, catch it and manage it.
    LOG.error("Fail to read Cell", e);
  }

  /**
   * Logs the exception.
   * @param e The exception to be logged.
   */
  private static void logFileNotFoundException(Throwable e) {
    LOG.error("Fail to read Cell, this mob file doesn't exist", e);
  }

  /**
   * Evicts the cached file.
   * @param cacheConf The current MobCachConfig.
   * @param fileName The name of the file to be evicted.
   */
  private static void evictFile(MobCacheConfig cacheConf, String fileName) {
    try {
      cacheConf.getMobFileCache().evictFile(fileName);
    } catch (IOException e) {
      LOG.error("Fail to evict the file " + fileName, e);
    }
  }

  /**
   * Converts an integer to a hex string.
   * @param i An integer.
   * @return A hex string.
   */
  public static String int2HexString(int i) {
    int shift = 4;
    char[] buf = new char[8];

    int charPos = 8;
    int mask = 15;
    do {
      buf[--charPos] = digits[i & mask];
      i >>>= shift;
    } while (charPos > 0);

    return new String(buf);
  }

  /**
   * Converts a hex string to an integer.
   * @param hex A hex string.
   * @return An integer.
   */
  public static int hexString2Int(String hex) {
    byte[] buffer = Bytes.toBytes(hex);
    if (buffer.length != 8) {
      throw new InvalidParameterException("hexString2Int length not valid");
    }

    for (int i = 0; i < buffer.length; i++) {
      byte ch = buffer[i];
      if (ch >= 'a' && ch <= 'f') {
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
}
