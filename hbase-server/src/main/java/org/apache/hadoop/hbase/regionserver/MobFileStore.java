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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.mob.MobCacheConfig;
import org.apache.hadoop.hbase.mob.MobFile;
import org.apache.hadoop.hbase.mob.MobFileName;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A manager of mob files.
 * It provides the information of the MOB column family in HBase, validates the mob files and
 * reads the MOB cells from the MOB files.
 */
public class MobFileStore {

  private static final Log LOG = LogFactory.getLog(MobFileStore.class);
  private Configuration conf;
  private FileSystem fs;
  private Path homePath;
  private MobCacheConfig cacheConf;
  private TableName tableName;
  private HColumnDescriptor family;
  private String familyAsString;
  private Path mobFamilyPath;
  private final static String TMP = ".tmp";

  private MobFileStore(Configuration conf, FileSystem fs, TableName tableName,
      HColumnDescriptor family) {
    this.fs = fs;
    this.homePath = MobUtils.getMobHome(conf);
    this.conf = conf;
    this.cacheConf = new MobCacheConfig(conf, family);
    this.tableName = tableName;
    this.family = family;
    this.familyAsString = family.getNameAsString();
    this.mobFamilyPath = MobUtils.getMobFamilyPath(conf, tableName, family.getNameAsString());
  }

  /**
   * Creates an instance of MobFileStore.
   * @param conf The current configuration.
   * @param fs The current file system.
   * @param tableName The table name.
   * @param family The column family.
   * @return An instance of MobFileStore.
   * @throws IOException
   */
  public static MobFileStore create(Configuration conf, FileSystem fs, TableName tableName,
      HColumnDescriptor family) throws IOException {
    if (null == family) {
      LOG.warn("fail to create the MobFileStore because the family is null in table [" + tableName
          + "]!");
      return null;
    }
    String familyName = family.getNameAsString();
    if (!MobUtils.isMobFamily(family)) {
      LOG.warn("failed to create the MobFileStore because the family [" + familyName
          + "] in table [" + tableName + "] is not a mob one!");
      return null;
    }
    return new MobFileStore(conf, fs, tableName, family);
  }

  /**
   * Gets the current MobCacheConfig.
   * @return A MobCacheConfig
   */
  public MobCacheConfig getCacheConfig() {
    return this.cacheConf;
  }

  /**
   * Gets the current column descriptor.
   * @return A descriptor.
   */
  public HColumnDescriptor getColumnDescriptor() {
    return this.family;
  }

  /**
   * Gets the mob file path.
   * @return The mob file path.
   */
  public Path getPath() {
    return mobFamilyPath;
  }

  /**
   * Gets the temp directory.
   * @return The temp directory.
   */
  private Path getTmpDir() {
    return new Path(homePath, TMP);
  }

  /**
   * Gets the table name.
   * @return A table name.
   */
  public TableName getTableName() {
    return tableName;
  }

  /**
   * Gets the family name.
   * @return The family name.
   */
  public String getFamilyName() {
    return familyAsString;
  }

  /**
   * Gets the current configuration.
   * @return The current configuration.
   */
  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * Gets the current file system.
   * @return The current file system.
   */
  public FileSystem getFileSystem() {
    return fs;
  }

  /**
   * Creates the temp directory of mob files for flushing.
   * @param date The latest date of cells in the flushing.
   * @param maxKeyCount The key count.
   * @param compression The compression algorithm.
   * @param startKey The start key.
   * @return The writer for the mob file.
   * @throws IOException
   */
  public StoreFile.Writer createWriterInTmp(Date date, long maxKeyCount,
      Compression.Algorithm compression, byte[] startKey) throws IOException {
    if (null == startKey) {
      startKey = HConstants.EMPTY_START_ROW;
    }

    CRC32 crc = new CRC32();
    crc.update(startKey);
    int checksum = (int) crc.getValue();
    return createWriterInTmp(date, maxKeyCount, compression, MobFileName.int2HexString(checksum));
  }

  /**
   * Creates the temp directory of mob files for flushing.
   * @param date The latest date of cells in the flushing.
   * @param maxKeyCount The key count.
   * @param compression The compression algorithm.
   * @param startKey The hex string of the checksum for the start key.
   * @return The writer for the mob file.
   * @throws IOException
   */
  public StoreFile.Writer createWriterInTmp(Date date, long maxKeyCount,
      Compression.Algorithm compression, String startKey) throws IOException {
    Path path = getTmpDir();
    return createWriterInTmp(MobUtils.formatDate(date), path, maxKeyCount, compression, startKey);
  }

  /**
   * Creates the temp directory of mob files for flushing.
   * @param date The date string, its format is yyyymmmdd.
   * @param basePath The basic path for a temp directory.
   * @param maxKeyCount The key count.
   * @param compression The compression algorithm.
   * @param startKey The hex string of the checksum for the start key.
   * @return The writer for the mob file.
   * @throws IOException
   */
  public StoreFile.Writer createWriterInTmp(String date, Path basePath, long maxKeyCount,
      Compression.Algorithm compression, String startKey) throws IOException {
    MobFileName mobFileName = MobFileName.create(startKey, date, UUID.randomUUID()
        .toString().replaceAll("-", ""));
    final CacheConfig writerCacheConf = cacheConf;
    HFileContext hFileContext = new HFileContextBuilder().withCompression(compression)
        .withIncludesMvcc(false).withIncludesTags(true)
        .withChecksumType(HFile.DEFAULT_CHECKSUM_TYPE)
        .withBytesPerCheckSum(HFile.DEFAULT_BYTES_PER_CHECKSUM)
        .withBlockSize(family.getBlocksize())
        .withHBaseCheckSum(true).withDataBlockEncoding(DataBlockEncoding.NONE).build();

    StoreFile.Writer w = new StoreFile.WriterBuilder(conf, writerCacheConf, fs)
        .withFilePath(MobUtils.getAbsolutePath(mobFamilyPath, mobFileName.getFileName()))
        .withComparator(KeyValue.COMPARATOR).withBloomType(BloomType.NONE)
        .withMaxKeyCount(maxKeyCount).withFileContext(hFileContext).build();
    return w;
  }

  /**
   * Commits the mob file.
   * @param sourceFile The source file.
   * @param targetPath The directory path where the source file is renamed to.
   * @throws IOException
   */
  public void commitFile(final Path sourceFile, Path targetPath) throws IOException {
    if (sourceFile == null) {
      return;
    }
    Path dstPath = new Path(targetPath, sourceFile.getName());
    validateMobFile(sourceFile);
    String msg = "Renaming flushed file from " + sourceFile + " to " + dstPath;
    LOG.info(msg);
    Path parent = dstPath.getParent();
    if (!fs.exists(parent)) {
      fs.mkdirs(parent);
    }
    if (!fs.rename(sourceFile, dstPath)) {
      LOG.warn("Unable to rename " + sourceFile + " to " + dstPath);
    }
  }

  /**
   * Validates a mob file by opening and closing it.
   *
   * @param path the path to the mob file
   */
  private void validateMobFile(Path path) throws IOException {
    StoreFile storeFile = null;
    try {
      storeFile = new StoreFile(this.fs, path, conf, this.cacheConf, BloomType.NONE);
      storeFile.createReader();
    } catch (IOException e) {
      LOG.error("Fail to open mob store file[" + path + "], keeping it in tmp location["
          + getTmpDir() + "].", e);
      throw e;
    } finally {
      if (storeFile != null) {
        storeFile.closeReader(false);
      }
    }
  }

  /**
   * Reads the cell from the mob file.
   * @param reference The cell found in the HBase, its value is a path to a mob file.
   * @param cacheBlocks Whether the scanner should cache blocks.
   * @return The cell found in the mob file.
   * @throws IOException
   */
  public Cell resolve(Cell reference, boolean cacheBlocks) throws IOException {
    Cell result = null;
    if (reference.getValueLength() > Bytes.SIZEOF_LONG) {
      String fileName = Bytes.toString(reference.getValueArray(), reference.getValueOffset()
          + Bytes.SIZEOF_LONG, reference.getValueLength() - Bytes.SIZEOF_LONG);
      Path targetPath = new Path(mobFamilyPath, fileName);
      MobFile file = null;
      try {
        file = cacheConf.getMobFileCache().openFile(fs, targetPath, cacheConf);
        result = file.readCell(reference, cacheBlocks);
      } catch (IOException e) {
        LOG.error("Fail to open/read the mob file " + targetPath.toString(), e);
      } catch (NullPointerException e) {
        // When delete the file during the scan, the hdfs getBlockRange will
        // throw NullPointerException, catch it and manage it.
        LOG.error("Fail to read the mob file " + targetPath.toString(), e);
      } finally {
        if (file != null) {
          cacheConf.getMobFileCache().closeFile(file);
        }
      }
    }

    if (result == null) {
      LOG.warn("The KeyValue result is null, assemble a new KeyValue with the same row,family,"
          + "qualifier,timestamp,type and tags but with an empty value to return.");
      result = new KeyValue(reference.getRowArray(), reference.getRowOffset(),
          reference.getRowLength(), reference.getFamilyArray(), reference.getFamilyOffset(),
          reference.getFamilyLength(), reference.getQualifierArray(),
          reference.getQualifierOffset(), reference.getQualifierLength(), reference.getTimestamp(),
          Type.codeToType(reference.getTypeByte()), HConstants.EMPTY_BYTE_ARRAY,
          reference.getValueOffset(), 0, reference.getTagsArray(), reference.getTagsOffset(),
          reference.getTagsLength());
    }
    return result;
  }
}
