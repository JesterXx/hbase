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
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMobFileCache extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestMobFileCache.class);
  private MobCacheConfig mobCacheConf;
  private MobFileCache mobFileCache;
  private Date currentDate = new Date();
  private final String TEST_CACHE_SIZE = "2";
  private final int EXPECTED_CACHE_SIZE_ZERO = 0;
  private final int EXPECTED_CACHE_SIZE_ONE = 1;
  private final int EXPECTED_CACHE_SIZE_TWO = 2;
  private final int EXPECTED_CACHE_SIZE_THREE = 3;
  private final long EXPECTED_REFERENCE_ONE = 1;
  private final long EXPECTED_REFERENCE_TWO = 2;
  
  private final String TABLE = "tableName";
  private final String FAMILY1 = "family1";
  private final String FAMILY2 = "family2";
  private final String FAMILY3 = "family3";
  
  private final byte[] ROW = Bytes.toBytes("row");
  private final byte[] ROW2 = Bytes.toBytes("row2");
  private final byte[] VALUE = Bytes.toBytes("value");
  private final byte[] VALUE2 = Bytes.toBytes("value2");
  private final byte[] QF1 = Bytes.toBytes("qf1");
  private final byte[] QF2 = Bytes.toBytes("qf2");
  private final byte[] QF3 = Bytes.toBytes("qf3");

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Create the mob store file.
   * @param family
   */
  private Path createMobStoreFile(String family) throws IOException {
    return createMobStoreFile(HBaseConfiguration.create(), family);
  }

  /**
   * Create the mob store file
   * @param conf
   * @param family
   */
  private Path createMobStoreFile(Configuration conf, String family) throws IOException {
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    hcd.setMaxVersions(4);
    hcd.setValue(MobConstants.IS_MOB, "true");
    mobCacheConf = new MobCacheConfig(conf, hcd);
    return createMobStoreFile(conf, hcd);
  }

  /**
   * Create the mob store file
   * @param conf
   * @param hcd
   */
  private Path createMobStoreFile(Configuration conf, HColumnDescriptor hcd)
      throws IOException {
    // Setting up a Store
    Path basedir = this.testDir;
    fs = FileSystem.get(conf);

    HTableDescriptor htd = new HTableDescriptor(TABLE);
    htd.addFamily(hcd);
    Path homePath = new Path(basedir, TABLE + Path.SEPARATOR
        + Bytes.toString(hcd.getName()));
    MobFileManager mobFileManager = MobFileManager.create(conf, fs,
        TableName.valueOf(TABLE), hcd);
    if (mobFileManager == null) {
      fs.mkdirs(homePath);
      mobFileManager = MobFileManager.create(conf, fs, TableName.valueOf(TABLE), hcd);
    }

    KeyValue key1 = new KeyValue(ROW, hcd.getName(), QF1, 1, VALUE);
    KeyValue key2 = new KeyValue(ROW, hcd.getName(), QF2, 1, VALUE);
    KeyValue key3 = new KeyValue(ROW2, hcd.getName(), QF3, 1, VALUE2);
    KeyValue[] keys = new KeyValue[] { key1, key2, key3 };
    int maxKeyCount = keys.length;
    HRegionInfo regionStartKey = new HRegionInfo();
    StoreFile.Writer mobWriter = mobFileManager.createWriterInTmp(currentDate, 
        maxKeyCount, hcd.getCompactionCompression(), regionStartKey.getStartKey());
    Path mobFilePath = mobWriter.getPath();
    String fileName = mobFilePath.getName();
    mobWriter.append(key1);
    mobWriter.append(key2);
    mobWriter.append(key3);
    mobWriter.close();
    String targetPathName = MobUtils.formatDate(currentDate);
    Path targetPath = new Path(mobFileManager.getPath(), targetPathName);
    mobFileManager.commitFile(mobFilePath, targetPath);
    return new Path(targetPath, fileName);
  }

  @Test
  public void testMobFileCache() throws Exception {
    conf.set(MobConstants.MOB_FILE_CACHE_SIZE_KEY, TEST_CACHE_SIZE);
    mobFileCache = new MobFileCache(conf);
    Path file1Path = createMobStoreFile(FAMILY1);
    Path file2Path = createMobStoreFile(FAMILY2);
    Path file3Path = createMobStoreFile(FAMILY3);
    
    // Before open one file by the MobFileCache
    assertEquals(EXPECTED_CACHE_SIZE_ZERO, mobFileCache.getCacheSize());
    // Open one file by the MobFileCache
    CachedMobFile cachedMobFile1 = (CachedMobFile) mobFileCache.openFile(
        this.fs, file1Path, mobCacheConf);
    assertEquals(EXPECTED_CACHE_SIZE_ONE, mobFileCache.getCacheSize());
    assertNotNull(cachedMobFile1);
    assertEquals(EXPECTED_REFERENCE_TWO, cachedMobFile1.getReference());
    
    // The evict is also managed by a schedule thread pool.
    // And its check period is set as 3600 seconds by default.
    // This evict should get the lock at the most time
    mobFileCache.evict();  // Cache not full, evict it
    assertEquals(EXPECTED_CACHE_SIZE_ONE, mobFileCache.getCacheSize());
    assertEquals(EXPECTED_REFERENCE_TWO, cachedMobFile1.getReference());
    
    mobFileCache.evictFile(file1Path.getName());  // Evict one file
    assertEquals(EXPECTED_CACHE_SIZE_ZERO, mobFileCache.getCacheSize());
    assertEquals(EXPECTED_REFERENCE_ONE, cachedMobFile1.getReference());
    
    cachedMobFile1.close();  // Close the cached mob file
    
    // Reopen three cached file
    cachedMobFile1 = (CachedMobFile) mobFileCache.openFile(
        this.fs, file1Path, mobCacheConf);
    assertEquals(EXPECTED_CACHE_SIZE_ONE, mobFileCache.getCacheSize());
    CachedMobFile cachedMobFile2 = (CachedMobFile) mobFileCache.openFile(
        this.fs, file2Path, mobCacheConf);
    assertEquals(EXPECTED_CACHE_SIZE_TWO, mobFileCache.getCacheSize());
    CachedMobFile cachedMobFile3 = (CachedMobFile) mobFileCache.openFile(
        this.fs, file3Path, mobCacheConf);
    // Before the evict
    // Evict the cache, should clost the first file 1
    assertEquals(EXPECTED_CACHE_SIZE_THREE, mobFileCache.getCacheSize());
    assertEquals(EXPECTED_REFERENCE_TWO, cachedMobFile1.getReference());
    assertEquals(EXPECTED_REFERENCE_TWO, cachedMobFile2.getReference());
    assertEquals(EXPECTED_REFERENCE_TWO, cachedMobFile3.getReference());
    mobFileCache.evict();
    assertEquals(EXPECTED_CACHE_SIZE_ONE, mobFileCache.getCacheSize());
    assertEquals(EXPECTED_REFERENCE_ONE, cachedMobFile1.getReference());
    assertEquals(EXPECTED_REFERENCE_ONE, cachedMobFile2.getReference());
    assertEquals(EXPECTED_REFERENCE_TWO, cachedMobFile3.getReference());
  }
}
