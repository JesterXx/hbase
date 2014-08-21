/**
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
package org.apache.hadoop.hbase.snapshot;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.DefaultMobStoreFlusher;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.DefaultStoreFlusher;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionServerSnapshotManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test clone/restore snapshots from the client
 *
 * TODO This is essentially a clone of TestRestoreSnapshotFromClient.  This is worth refactoring
 * this because there will be a few more flavors of snapshots that need to run these tests.
 */
@Category(LargeTests.class)
public class TestMobRestoreFlushSnapshotFromClient {
  final Log LOG = LogFactory.getLog(getClass());

  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private final byte[] FAMILY = Bytes.toBytes("cf");

  private byte[] snapshotName0;
  private byte[] snapshotName1;
  private byte[] snapshotName2;
  private int snapshot0Rows;
  private int snapshot1Rows;
  private TableName tableName;
  private Admin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.getConfiguration().setBoolean("hbase.online.schema.update.enable", true);
    UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    UTIL.getConfiguration().setBoolean(
        "hbase.master.enabletable.roundrobin", true);

    // Enable snapshot
    UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    UTIL.getConfiguration().setLong(RegionServerSnapshotManager.SNAPSHOT_TIMEOUT_MILLIS_KEY,
      RegionServerSnapshotManager.SNAPSHOT_TIMEOUT_MILLIS_DEFAULT * 2);
    
    UTIL.getConfiguration().setClass(DefaultStoreEngine.DEFAULT_STORE_FLUSHER_CLASS_KEY,
        DefaultMobStoreFlusher.class, DefaultStoreFlusher.class);

    UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  /**
   * Initialize the tests with a table filled with some data
   * and two snapshots (snapshotName0, snapshotName1) of different states.
   * The tableName, snapshotNames and the number of rows in the snapshot are initialized.
   */
  @Before
  public void setup() throws Exception {
    this.admin = UTIL.getHBaseAdmin();

    long tid = System.currentTimeMillis();
    tableName = TableName.valueOf("testtb-" + tid);
    snapshotName0 = Bytes.toBytes("snaptb0-" + tid);
    snapshotName1 = Bytes.toBytes("snaptb1-" + tid);
    snapshotName2 = Bytes.toBytes("snaptb2-" + tid);

    // create Table
    MobSnapshotTestingUtils.createMobTable(UTIL, tableName, 1, FAMILY);

    HTable table = new HTable(UTIL.getConfiguration(), tableName);
    SnapshotTestingUtils.loadData(UTIL, table, 500, FAMILY);
    snapshot0Rows = MobSnapshotTestingUtils.countMobRows(table);
    LOG.info("=== before snapshot with 500 rows");
    logFSTree();

    // take a snapshot
    admin.snapshot(Bytes.toString(snapshotName0), tableName,
        SnapshotDescription.Type.FLUSH);

    LOG.info("=== after snapshot with 500 rows");
    logFSTree();

    // insert more data
    SnapshotTestingUtils.loadData(UTIL, table, 500, FAMILY);
    snapshot1Rows = MobSnapshotTestingUtils.countMobRows(table);
    LOG.info("=== before snapshot with 1000 rows");
    logFSTree();

    // take a snapshot of the updated table
    admin.snapshot(Bytes.toString(snapshotName1), tableName,
        SnapshotDescription.Type.FLUSH);
    LOG.info("=== after snapshot with 1000 rows");
    logFSTree();
    table.close();
  }

  @After
  public void tearDown() throws Exception {
    SnapshotTestingUtils.deleteAllSnapshots(UTIL.getHBaseAdmin());
    SnapshotTestingUtils.deleteArchiveDirectory(UTIL);
  }

  @Test
  public void testTakeFlushSnapshot() throws IOException {
    // taking happens in setup.
  }

  @Test
  public void testRestoreSnapshot() throws IOException {
    MobSnapshotTestingUtils.verifyMobRowCount(UTIL, tableName, snapshot1Rows);

    // Restore from snapshot-0
    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName0);
    logFSTree();
    admin.enableTable(tableName);
    LOG.info("=== after restore with 500 row snapshot");
    logFSTree();
    MobSnapshotTestingUtils.verifyMobRowCount(UTIL, tableName, snapshot0Rows);

    // Restore from snapshot-1
    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName1);
    admin.enableTable(tableName);
    MobSnapshotTestingUtils.verifyMobRowCount(UTIL, tableName, snapshot1Rows);
  }

  // ==========================================================================
  //  Helpers
  // ==========================================================================
  private void logFSTree() throws IOException {
    MasterFileSystem mfs = UTIL.getMiniHBaseCluster().getMaster().getMasterFileSystem();
    FSUtils.logFileSystemState(mfs.getFileSystem(), mfs.getRootDir(), LOG);
  }
}
