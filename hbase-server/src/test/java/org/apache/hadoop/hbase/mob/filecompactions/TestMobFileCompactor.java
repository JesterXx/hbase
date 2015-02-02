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
import org.apache.hadoop.hbase.Cell;
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
  private static HColumnDescriptor hcd1;
  private static HColumnDescriptor hcd2;
  private static FileSystem fs;
  private final static String row = "row_";
  private final static String family1 = "family1";
  private final static String family2 = "family2";
  private final static String qf1 = "qualifier1";
  private final static String qf2 = "qualifier2";

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
    hcd1 = new HColumnDescriptor(family1);
    hcd1.setMobEnabled(true);
    hcd1.setMobThreshold(0L);
    hcd1.setMaxVersions(4);
    hcd2 = new HColumnDescriptor(family2);
    hcd2.setMobEnabled(true);
    hcd2.setMobThreshold(0L);
    hcd2.setMaxVersions(4);
    desc = new HTableDescriptor(tableName);
    desc.addFamily(hcd1);
    desc.addFamily(hcd2);
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

    assertEquals("Before compaction: mob rows count", count, countMobRows(hTable));
    assertEquals("Before compaction: mob file count", count, countFiles(true, family1));
    assertEquals("Before compaction: del file count", 0, countFiles(false, family1));

    MobFileCompactor compactor =
      new PartitionedMobFileCompactor(conf, fs, tableName, hcd1);
    compactor.compact();

    assertEquals("After compaction: mob rows count", count, countMobRows(hTable));
    assertEquals("After compaction: mob file count", 1, countFiles(true, family1));
    assertEquals("After compaction: del file count", 0, countFiles(false, family1));
  }

  @Test
  public void testCompactionWithDelFiles() throws Exception {
    int count = 6;
    //create table and generate mob files
    createMobTableAndAddData(count, 1);

    assertEquals("Before deleting: mob rows count", count, countMobRows(hTable));
    assertEquals("Before deleting: mob cells count", 3*count, countMobCells(hTable));
    assertEquals("Before deleting: family1 mob file count", count, countFiles(true, family1));
    assertEquals("Before deleting: family2 mob file count", count, countFiles(true, family2));

    // now let's delete a family
    Delete delete1 = new Delete(Bytes.toBytes(row + 0));
    delete1.deleteFamily(Bytes.toBytes(family1));
    hTable.delete(delete1);
    // delete one row
    Delete delete2 = new Delete(Bytes.toBytes(row + 1));
    hTable.delete(delete2);
    // delete one cell
    Delete delete3 = new Delete(Bytes.toBytes(row + 2));
    delete3.deleteColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1));
    hTable.delete(delete3);
    hTable.flushCommits();
    admin.flush(tableName);

    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(
        Bytes.toBytes(tableNameAsString));
    for(HRegion region : regions) {
      region.waitForFlushesAndCompactions();
      region.compactStores(true);
    }

    assertEquals("Before compaction: mob rows count", count-1, countMobRows(hTable));
    assertEquals("Before compaction: mob cells count", 3*count-6, countMobCells(hTable));
    assertEquals("Before compaction: family1 mob file count", count, countFiles(true, family1));
    assertEquals("Before compaction: family2 file count", count, countFiles(true, family2));
    assertEquals("Before compaction: family1 del file count", 1, countFiles(false, family1));
    assertEquals("Before compaction: family2 del file count", 1, countFiles(false, family2));

    // do the mob file compaction
    MobFileCompactor compactor =
      new PartitionedMobFileCompactor(conf, fs, tableName, hcd1);
    compactor.compact();

    assertEquals("After compaction: mob rows count", count-1, countMobRows(hTable));
    assertEquals("After compaction: mob cells count", 3*count-6, countMobCells(hTable));
    assertEquals("After compaction: family1 mob file count", 1, countFiles(true, family1));
    assertEquals("After compaction: family2 mob file count", count, countFiles(true, family2));
    assertEquals("After compaction: family1 del file count", 0, countFiles(false, family1));
    assertEquals("After compaction: family2 del file count", 1, countFiles(false, family2));
  }

  @Test
  public void testCompactionWithDelFilesAndNotMergeAllFiles() throws Exception {
    int mergeSize = 5000;
    // change the mob compaction merge size
    conf.setLong(MobConstants.MOB_FILE_COMPACTION_MERGEABLE_THRESHOLD, mergeSize);

    int count = 8;
    // create table and generate mob files
    createMobTableAndAddData(count, 1);

    assertEquals("Before deleting: mob rows count", count, countMobRows(hTable));
    assertEquals("Before deleting: mob cells count", 3*count, countMobCells(hTable));
    assertEquals("Before deleting: mob file count", count, countFiles(true, family1));

    int largeFilesCount = countLargeFiles(mergeSize, family1);;

    // now let's delete a family
    Delete delete1 = new Delete(Bytes.toBytes(row + 0));
    delete1.deleteFamily(Bytes.toBytes(family1));
    hTable.delete(delete1);
    // delete one row
    Delete delete2 = new Delete(Bytes.toBytes(row + 1));
    hTable.delete(delete2);
    // delete one cell
    Delete delete3 = new Delete(Bytes.toBytes(row + 2));
    delete3.deleteColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1));
    hTable.delete(delete3);
    hTable.flushCommits();
    admin.flush(tableName);

    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(
        Bytes.toBytes(tableNameAsString));
    for(HRegion region : regions) {
      region.waitForFlushesAndCompactions();
      region.compactStores(true);
    }
    assertEquals("Before compaction: mob rows count", count-1, countMobRows(hTable));
    assertEquals("Before compaction: mob cells count", 3*count-6, countMobCells(hTable));
    assertEquals("Before compaction: family1 mob file count", count, countFiles(true, family1));
    assertEquals("Before compaction: family2 mob file count", count, countFiles(true, family2));
    assertEquals("Before compaction: family1 del file count", 1, countFiles(false, family1));
    assertEquals("Before compaction: family2 del file count", 1, countFiles(false, family2));

    // do the mob file compaction
    MobFileCompactor compactor =
      new PartitionedMobFileCompactor(conf, fs, tableName, hcd1);
    compactor.compact();

    assertEquals("After compaction: mob rows count", count-1, countMobRows(hTable));
    assertEquals("After compaction: mob cells count", 3*count-6, countMobCells(hTable));
    // After the compaction, the files smaller than the mob compaction merge size
    // is merge to one file
    assertEquals("After compaction: family1 mob file count", largeFilesCount + 1,
        countFiles(true, family1));
    assertEquals("After compaction: family2 mob file count", count, countFiles(true, family2));
    assertEquals("After compaction: family1 del file count", 1, countFiles(false, family1));
    assertEquals("After compaction: family2 del file count", 1, countFiles(false, family2));

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

    assertEquals("Before deleting: mob row count", count, countMobRows(hTable));
    assertEquals("Before deleting: family1 mob file count", count, countFiles(true, family1));
    assertEquals("Before deleting: family2 mob file count", count, countFiles(true, family2));

    // now let's delete a family
    Delete delete1 = new Delete(Bytes.toBytes(row + 0));
    delete1.deleteFamily(Bytes.toBytes(family1));
    hTable.delete(delete1);
    // delete one row
    Delete delete2 = new Delete(Bytes.toBytes(row + 1));
    hTable.delete(delete2);
    // delete one cell
    Delete delete3 = new Delete(Bytes.toBytes(row + 2));
    delete3.deleteColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1));
    hTable.delete(delete3);
    hTable.flushCommits();
    admin.flush(tableName);

    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(
        Bytes.toBytes(tableNameAsString));
    for(HRegion region : regions) {
      region.waitForFlushesAndCompactions();
      region.compactStores(true);
    }

    assertEquals("Before compaction: mob rows count", count-1, countMobRows(hTable));
    assertEquals("Before compaction: mob cells count", 3*count-6, countMobCells(hTable));
    assertEquals("Before compaction: family1 mob file count", count, countFiles(true, family1));
    assertEquals("Before compaction: family2 mob file count", count, countFiles(true, family2));
    assertEquals("Before compaction: family1 del file count", 1, countFiles(false, family1));
    assertEquals("Before compaction: family2 del file count", 1, countFiles(false, family2));

    // do the mob file compaction
    MobFileCompactor compactor = new PartitionedMobFileCompactor(conf, fs, tableName, hcd1);
    compactor.compact();

    assertEquals("After compaction: mob rows count", count-1, countMobRows(hTable));
    assertEquals("After compaction: mob cells count", 3*count-6, countMobCells(hTable));
    assertEquals("After compaction: family1 mob file count", 4, countFiles(true, family1));
    assertEquals("After compaction: family2 mob file count", count, countFiles(true, family2));
    assertEquals("After compaction: family1 del file count", 0, countFiles(false, family1));
    assertEquals("After compaction: family2 del file count", 1, countFiles(false, family2));

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

    // now let's delete a family
    Delete delete1 = new Delete(Bytes.toBytes(row + 0));
    delete1.deleteFamily(Bytes.toBytes(family1));
    hTable.delete(delete1);
    // delete one row
    Delete delete2 = new Delete(Bytes.toBytes(row + 1));
    hTable.delete(delete2);
    // delete one cell
    Delete delete3 = new Delete(Bytes.toBytes(row + 2));
    delete3.deleteColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1));
    hTable.delete(delete3);
    hTable.flushCommits();
    admin.flush(tableName);
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(
        Bytes.toBytes(tableNameAsString));
    for(HRegion region : regions) {
      region.waitForFlushesAndCompactions();
      region.compactStores(true);
    }

    assertEquals("Before compaction: mob rows count", count-1, countMobRows(hTable));
    assertEquals("Before compaction: mob cells count", 3*count-6, countMobCells(hTable));
    assertEquals("Before compaction: family1 mob file count", count, countFiles(true, family1));
    assertEquals("Before compaction: family2 mob file count", count, countFiles(true, family2));
    assertEquals("Before compaction: family1 del file count", 1, countFiles(false, family1));
    assertEquals("Before compaction: family2 del file count", 1, countFiles(false, family2));

    // do the mob file compaction
    MobFileCompactor compactor =
      new PartitionedMobFileCompactor(conf, fs, tableName, hcd1);
    compactor.compact();

    assertEquals("After first compaction: mob rows count", count-1, countMobRows(hTable));
    assertEquals("After first compaction: mob cells count", 3*count-6, countMobCells(hTable));
    assertEquals("After first compaction: family1 mob file count", 1, countFiles(true, family1));
    assertEquals("After first compaction: family2 mob file count", count, countFiles(true, family2));
    assertEquals("After first compaction: family1 del file count", 0, countFiles(false, family1));
    assertEquals("After first compaction: family2 del file count", 1, countFiles(false, family2));
    assertEquals("After first compaction: family1 hfilelink count", 0, countHFileLinks(family1));
    assertEquals("After first compaction: family2 hfilelink count", 0, countHFileLinks(family2));

    admin.disableTable(tableName);
    // Restore from snapshot, the hfilelink will exist in mob dir
    admin.restoreSnapshot(snapshotName1);
    admin.enableTable(tableName);

    assertEquals("After restoring snapshot: mob rows count", count, countMobRows(hTable));
    assertEquals("After restoring snapshot: mob cells count", 3*count, countMobCells(hTable));
    assertEquals("After restoring snapshot: family1 mob file count", count, countFiles(true, family1));
    assertEquals("After restoring snapshot: family2 mob file count", count, countFiles(true, family2));
    assertEquals("After restoring snapshot: family1 del file count", 0, countFiles(false, family1));
    assertEquals("After restoring snapshot: family2 del file count", 0, countFiles(false, family2));
    assertEquals("After restoring snapshot: family1 hfilelink count", 4, countHFileLinks(family1));
    assertEquals("After restoring snapshot: family2 hfilelink count", 0, countHFileLinks(family2));

    compactor.compact();

    assertEquals("After second compaction: mob rows count", count, countMobRows(hTable));
    assertEquals("After second compaction: mob cells count", 3*count, countMobCells(hTable));
    assertEquals("After second compaction: family1 mob file count", 1, countFiles(true, family1));
    assertEquals("After second compaction: family2 mob file count", count, countFiles(true, family2));
    assertEquals("After second compaction: family1 del file count", 0, countFiles(false, family1));
    assertEquals("After second compaction: family2 del file count", 0, countFiles(false, family2));
    assertEquals("After second compaction: family1 hfilelink count", 0, countHFileLinks(family1));
    assertEquals("After second compaction: family2 hfilelink count", 0, countHFileLinks(family2));
  }

  /**
   * Gets the number of rows in the given table.
   * @param table to get the  scanner
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
   * Gets the number of cells in the given table.
   * @param table to get the  scanner
   * @return the number of cells
   */
  private int countMobCells(final HTable table) throws IOException {
    Scan scan = new Scan();
    // Do not retrieve the mob data when scanning
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      for(Cell cell : res.listCells()) {
        count++;
      }
    }
    results.close();
    return count;
  }

  /**
   * Gets the number of files in the mob path.
   * @param isMobFile gets number of the mob files or del files
   * @param familyName the family name
   * @return the number of the files
   */
  private int countFiles(boolean isMobFile, String familyName) throws IOException {
    Path mobDirPath = MobUtils.getMobFamilyPath(MobUtils.getMobRegionPath(conf,
      tableName), familyName);
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
   * @param familyName the family name
   * @return the number of the HFileLink
   */
  private int countHFileLinks(String familyName) throws IOException {
    Path mobDirPath = MobUtils.getMobFamilyPath(MobUtils.getMobRegionPath(conf,
      tableName), familyName);
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
   * @param familyName the family name
   * @return the number of files large than the size
   */
  private int countLargeFiles(int size, String familyName) throws IOException {
    Path mobDirPath = MobUtils.getMobFamilyPath(MobUtils.getMobRegionPath(
        TEST_UTIL.getConfiguration(), tableName), familyName);
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
      put.add(Bytes.toBytes(family1), Bytes.toBytes(qf1), mobVal);
      put.add(Bytes.toBytes(family1), Bytes.toBytes(qf2), mobVal);
      put.add(Bytes.toBytes(family2), Bytes.toBytes(qf1), mobVal);
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
