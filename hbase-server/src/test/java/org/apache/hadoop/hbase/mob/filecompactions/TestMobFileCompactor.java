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
package org.apache.hadoop.hbase.mob.filecompactions;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestMobFileCompactor {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private Configuration conf = null;
  private String tableNameAsString;
  private TableName tableName;
  private static HTable hTable;
  private static Admin admin;
  private static HTableDescriptor desc;
  private static HColumnDescriptor hcd;
  private static FileSystem fs;
  private final static String row = "row_";
  private final static String family = "family";
  private final static String column = "column";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    fs = TEST_UTIL.getTestFileSystem();
    conf = TEST_UTIL.getConfiguration();
    long tid = System.currentTimeMillis();
    tableNameAsString = "testMob" + tid;
    tableName = TableName.valueOf(tableNameAsString);
    desc = new HTableDescriptor(tableName);
    hcd = new HColumnDescriptor(family);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(0L);
    hcd.setMaxVersions(4);
    desc.addFamily(hcd);
    admin = TEST_UTIL.getHBaseAdmin();
    admin.createTable(desc);
    hTable = new HTable(conf, tableNameAsString);
    hTable.setAutoFlush(false, false);
  }

  @After
  public void tearDown() throws Exception {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    admin.close();
    hTable.close();
    fs.delete(TEST_UTIL.getDataTestDir(), true);
  }

  @Test
  public void testCompactionWithoutDelFiles() throws Exception {
    int count = 10;
    //create table and generate mob files
    createMobTableAndAddData(count, 1);

    assertEquals("Before compaction: mob rows", count, countMobRows(hTable));
    assertEquals("Before compaction: mob file count", count, countFiles(true));
    assertEquals("Before compaction: del file count", 0, countFiles(false));

    MobFileCompactor compactor =
      new PartitionedMobFileCompactor(conf, fs, desc.getTableName(), hcd);
    compactor.compact();

    assertEquals("After compaction: mob rows", count, countMobRows(hTable));
    assertEquals("After compaction: mob file count", 1, countFiles(true));
    assertEquals("After compaction: del file count", 0, countFiles(false));
  }

  @Test
  public void testCompactionWithDelFiles() throws Exception {
    int count = 8;
    //create table and generate mob files
    createMobTableAndAddData(count, 1);

    //get mob files
    assertEquals("Before compaction: mob file count", count, countFiles(true));
    assertEquals(count, countMobRows(hTable));

    // now let's delete one cell
    Delete delete = new Delete(Bytes.toBytes(row + 0));
    delete.deleteFamily(Bytes.toBytes(family));
    hTable.delete(delete);
    hTable.flushCommits();
    admin.flush(tableName);

    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(
        Bytes.toBytes(tableNameAsString));
    for(HRegion region : regions) {
      region.waitForFlushesAndCompactions();
      region.compactStores(true);
    }

    assertEquals("Before compaction: mob rows", count-1, countMobRows(hTable));
    assertEquals("Before compaction: mob file count", count, countFiles(true));
    assertEquals("Before compaction: del file count", 1, countFiles(false));

    // do the mob file compaction
    MobFileCompactor compactor =
      new PartitionedMobFileCompactor(conf, fs, desc.getTableName(), hcd);
    compactor.compact();

    assertEquals("After compaction: mob rows", count-1, countMobRows(hTable));
    assertEquals("After compaction: mob file count", 1, countFiles(true));
    assertEquals("After compaction: del file count", 0, countFiles(false));
  }

  @Test
  public void testCompactionWithDelFilesAndNotMergeAllFiles() throws Exception {
    int mergeSize = 5000;
    // change the mob compaction merge size
    conf.setLong(MobConstants.MOB_FILE_COMPACTION_MERGEABLE_THRESHOLD, mergeSize);

    int count = 8;
    // create table and generate mob files
    createMobTableAndAddData(count, 1);

    // get mob files
    assertEquals("Before compaction: mob file count", count, countFiles(true));
    assertEquals(count, countMobRows(hTable));

    int largeFilesCount = countLargeFiles(mergeSize);;

    // now let's delete one cell
    Delete delete = new Delete(Bytes.toBytes(row + 0));
    delete.deleteFamily(Bytes.toBytes(family));
    hTable.delete(delete);
    hTable.flushCommits();
    admin.flush(tableName);

    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(
        Bytes.toBytes(tableNameAsString));
    for(HRegion region : regions) {
      region.waitForFlushesAndCompactions();
      region.compactStores(true);
    }
    assertEquals("Before compaction: mob rows", count-1, countMobRows(hTable));
    assertEquals("Before compaction: mob file count", count, countFiles(true));
    assertEquals("Before compaction: del file count", 1, countFiles(false));

    // do the mob file compaction
    MobFileCompactor compactor =
      new PartitionedMobFileCompactor(conf, fs, desc.getTableName(), hcd);
    compactor.compact();

    assertEquals("After compaction: mob rows", count-1, countMobRows(hTable));
    // After the compaction, the files smaller than the mob compaction merge size
    // is merge to one file
    assertEquals("After compaction: mob file count", largeFilesCount + 1, countFiles(true));
    assertEquals("After compaction: del file count", 1, countFiles(false));

    // reset the conf the the default
    conf.setLong(MobConstants.MOB_FILE_COMPACTION_MERGEABLE_THRESHOLD,
        MobConstants.DEFAULT_MOB_FILE_COMPACTION_MERGEABLE_THRESHOLD);
  }

  @Test
  public void testCompactionWithDelFilesAndWithSmallCompactionBatchSize() throws Exception {
    conf.setInt(MobConstants.MOB_FILE_COMPACTION_BATCH_SIZE, 2);
    int count = 8;
    //create table and generate mob files
    createMobTableAndAddData(count, 1);

    //get mob files
    assertEquals("Before compaction: mob file count", count, countFiles(true));
    assertEquals(count, countMobRows(hTable));

    // now let's delete one cell
    Delete delete1 = new Delete(Bytes.toBytes(row + 0));
    delete1.deleteFamily(Bytes.toBytes(family));
    hTable.delete(delete1);
    hTable.flushCommits();
    admin.flush(tableName);

    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(
        Bytes.toBytes(tableNameAsString));
    for(HRegion region : regions) {
      region.waitForFlushesAndCompactions();
      region.compactStores(true);
    }

    assertEquals("Before compaction: mob rows", count-1, countMobRows(hTable));
    assertEquals("Before compaction: mob file count", count, countFiles(true));
    assertEquals("Before compaction: del file count", 1, countFiles(false));

    // do the mob file compaction
    MobFileCompactor compactor = new PartitionedMobFileCompactor(conf, fs, desc.getTableName(), hcd);
    compactor.compact();

    assertEquals("After compaction: mob rows", count-1, countMobRows(hTable));
    assertEquals("After compaction: mob file count", 4, countFiles(true));
    assertEquals("After compaction: del file count", 0, countFiles(false));

    conf.setInt(MobConstants.MOB_FILE_COMPACTION_BATCH_SIZE,
        MobConstants.DEFAULT_MOB_FILE_COMPACTION_BATCH_SIZE);
  }

  @Test
  public void testCompactionWithHFileLink() throws IOException, InterruptedException {
    int count = 4;
    // create table and generate mob files
    createMobTableAndAddData(count, 1);

    long tid = System.currentTimeMillis();
    byte[] snapshotName1 = Bytes.toBytes("snaptb-" + tid);
    // take a snapshot
    admin.snapshot(snapshotName1, tableName);

    // now let's delete one cell
    Delete delete = new Delete(Bytes.toBytes(row + 0));
    delete.deleteFamily(Bytes.toBytes(family));
    hTable.delete(delete);
    hTable.flushCommits();
    admin.flush(tableName);
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(
        Bytes.toBytes(tableNameAsString));
    for(HRegion region : regions) {
      region.waitForFlushesAndCompactions();
      region.compactStores(true);
    }

    assertEquals("Before compaction: mob rows", count-1, countMobRows(hTable));
    assertEquals("Before compaction: mob file count", count, countFiles(true));
    assertEquals("Before compaction: del file count", 1, countFiles(false));

    // do the mob file compaction
    MobFileCompactor compactor =
      new PartitionedMobFileCompactor(conf, fs, desc.getTableName(), hcd);
    compactor.compact();

    assertEquals("After first compaction: mob rows", count-1, countMobRows(hTable));
    assertEquals("After first compaction: mob file count", 1, countFiles(true));
    assertEquals("After first compaction: del file count", 0, countFiles(false));
    assertEquals("After first compaction: hfilelink count", 0, countHFileLinks());

    admin.disableTable(tableName);
    // Restore from snapshot, the hfilelink will exist in mob dir
    admin.restoreSnapshot(snapshotName1);
    admin.enableTable(tableName);

    assertEquals("After restoring snapshot: mob rows", count, countMobRows(hTable));
    assertEquals("After restoring snapshot: mob file count", count, countFiles(true));
    assertEquals("After restoring snapshot: del file count", 0, countFiles(false));
    assertEquals("After restoring snapshot: hfilelink count", 4, countHFileLinks());

    compactor.compact();

    assertEquals("After second compaction: mob rows", count, countMobRows(hTable));
    assertEquals("After second compaction: mob file count", 1, countFiles(true));
    assertEquals("After second compaction: del file count", 0, countFiles(false));
    assertEquals("After second compaction: hfilelink count", 0, countHFileLinks());
  }

  /**
   * Gets the number of rows in the given table.
   * @return the number of rows
   */
  private int countMobRows(final HTable table) throws IOException {
    Scan scan = new Scan();
    // Do not retrieve the mob data when scanning
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      count++;
    }
    results.close();
    return count;
  }

  /**
   * Gets the number of files in the mob path.
   * @param isMobFile gets number of the mob files or del files
   * @return the number of the files
   */
  private int countFiles(boolean isMobFile) throws IOException {
    Path mobDirPath = MobUtils.getMobFamilyPath(MobUtils.getMobRegionPath(conf,
      tableName), family);
    int count = 0;
    if (fs.exists(mobDirPath)) {
      FileStatus[] files = fs.listStatus(mobDirPath);
      for(FileStatus file : files) {
        if(isMobFile == true) {
          if(!StoreFileInfo.isDelFile(file.getPath())) {
            count++;
          }
        } else {
          if(StoreFileInfo.isDelFile(file.getPath())) {
            count++;
          }
        }
      }
    }
    return count;
  }

  /**
   * Gets the number of HFileLink in the mob path.
   * @return the number of the HFileLink
   */
  private int countHFileLinks() throws IOException {
    Path mobDirPath = MobUtils.getMobFamilyPath(MobUtils.getMobRegionPath(conf,
      tableName), family);
    int count = 0;
    if (fs.exists(mobDirPath)) {
      FileStatus[] files = fs.listStatus(mobDirPath);
      for(FileStatus file : files) {
        if(HFileLink.isHFileLink(file.getPath())) {
          count++;
        }
      }
    }
    return count;
  }

  /**
   * Gets the number of files.
   * @param size the size of the file
   * @return the number of files large than the size
   */
  private int countLargeFiles(int size) throws IOException {
    Path mobDirPath = MobUtils.getMobFamilyPath(MobUtils.getMobRegionPath(
        TEST_UTIL.getConfiguration(), tableName), family);
    int count = 0;
    if (fs.exists(mobDirPath)) {
      FileStatus[] files = fs.listStatus(mobDirPath);
      for(FileStatus file : files) {
        // ignore the del files in the mob path
        if((!StoreFileInfo.isDelFile(file.getPath())) && (file.getLen() > size)) {
          count++;
        }
      }
    }
    return count;
  }

  /**
   * Creates a mob table and inserts some data in the table.
   * @param count the mob file number
   * @param cellsPerFlush the number of cells per flush
   */
  private void createMobTableAndAddData(int count, int cellsPerFlush)
      throws IOException, InterruptedException {
    if (count <= 0 || cellsPerFlush <= 0) {
      throw new IllegalArgumentException();
    }
    int index = 0;
    for (int i = 0; i < count; i++) {
      byte[] mobVal = makeDummyData(100*(i+1));
      Put put = new Put(Bytes.toBytes(row + i));
      put.setDurability(Durability.SKIP_WAL);
      put.add(Bytes.toBytes(family), Bytes.toBytes(column), mobVal);
      hTable.put(put);
      if (++index == cellsPerFlush) {
        hTable.flushCommits();
        admin.flush(tableName);
        index = 0;
      }
    }
  }

  /**
   * Creates the dummy data with a specific size.
   * @param the size of data
   * @return the dummy data
   */
  private byte[] makeDummyData(int size) {
    byte[] dummyData = new byte[size];
    new Random().nextBytes(dummyData);
    return dummyData;
  }
}
