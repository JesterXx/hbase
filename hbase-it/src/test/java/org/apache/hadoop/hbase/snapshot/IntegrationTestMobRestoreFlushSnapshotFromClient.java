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
package org.apache.hadoop.hbase.snapshot;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.IntegrationTestBase;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.IntegrationTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionServerSnapshotManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

/**
 * Test clone/restore snapshots from the client
 *
 * TODO This is essentially a clone of TestRestoreSnapshotFromClient.  This is worth refactoring
 * this because there will be a few more flavors of snapshots that need to run these tests.
 */
@Category(IntegrationTests.class)
public class IntegrationTestMobRestoreFlushSnapshotFromClient extends IntegrationTestBase {

  final Log LOG = LogFactory.getLog(getClass());

  private final byte[] FAMILY = Bytes.toBytes("cf");

  private byte[] snapshotName0;
  private byte[] snapshotName1;
  private byte[] snapshotName2;
  private int snapshot0Rows;
  private int snapshot1Rows;
  private TableName tableName;
  private Admin admin;

  /**
   * Initialize the tests with a table filled with some data
   * and two snapshots (snapshotName0, snapshotName1) of different states.
   * The tableName, snapshotNames and the number of rows in the snapshot are initialized.
   */
  public void setUpTest() throws Exception {

    this.admin = util.getHBaseAdmin();

    long tid = System.currentTimeMillis();
    tableName = TableName.valueOf("testtb-" + tid);
    snapshotName0 = Bytes.toBytes("snaptb0-" + tid);
    snapshotName1 = Bytes.toBytes("snaptb1-" + tid);
    snapshotName2 = Bytes.toBytes("snaptb2-" + tid);

    // create Table
    MobSnapshotTestingUtils.createMobTable(util, tableName, 1, FAMILY);

    HTable table = new HTable(util.getConfiguration(), tableName);
    MobSnapshotTestingUtils.loadMobData(util, table, 100, FAMILY);
    snapshot0Rows = MobSnapshotTestingUtils.countMobRows(table);
    LOG.info("=== before snapshot with 100 rows");

    // take a snapshot
    admin.snapshot(Bytes.toString(snapshotName0), tableName,
        SnapshotDescription.Type.FLUSH);

    LOG.info("=== after snapshot with 100 rows");

    // insert more data
    MobSnapshotTestingUtils.loadMobData(util, table, 100, FAMILY);
    snapshot1Rows = MobSnapshotTestingUtils.countMobRows(table);
    LOG.info("=== before snapshot with 200 rows");

    // take a snapshot of the updated table
    admin.snapshot(Bytes.toString(snapshotName1), tableName,
        SnapshotDescription.Type.FLUSH);
    LOG.info("=== after snapshot with 200 rows");
    table.close();
  }

  public void cleanUpTest() throws Exception {
    if (admin.tableExists(tableName)) {
      util.deleteTable(tableName);
    }
    if (util.isDistributedCluster()) {
      util.getHBaseClusterInterface().restoreInitialStatus();
    } else {
      SnapshotTestingUtils.deleteAllSnapshots(admin);
      SnapshotTestingUtils.deleteArchiveDirectory(util);
    }
  }

  protected int getNumReplicas() {
    return 3;
  }

  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(getConf());
    setUpBaseConf(util.getConfiguration());
    util.initializeCluster(1);
  }

  public static void setUpBaseConf(Configuration conf) {
    conf.setBoolean("hbase.online.schema.update.enable", true);
    conf.setInt("hbase.regionserver.msginterval", 100);
    conf.setInt("hbase.client.pause", 250);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    conf.setBoolean(
        "hbase.master.enabletable.roundrobin", true);

    // Enable snapshot
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    conf.setLong(RegionServerSnapshotManager.SNAPSHOT_TIMEOUT_MILLIS_KEY,
      RegionServerSnapshotManager.SNAPSHOT_TIMEOUT_MILLIS_DEFAULT * 2);

    conf.setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 0);
  }

  @Test
  public void testAll() throws Exception {
    testCloneNonExistentSnapshot();
    testRestoreSnapshotOfCloned();
    testRestoreSnapshot();
    testTakeFlushSnapshot();
    testCloneSnapshot();
  }

  public void testTakeFlushSnapshot() throws Exception {
    setUpTest();
    // taking happens in setup.
    cleanUpTest();
  }

  public void testRestoreSnapshot() throws Exception {
    setUpTest();
    MobSnapshotTestingUtils.verifyMobRowCount(util, tableName, snapshot1Rows);

    // Restore from snapshot-0
    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName0);
    admin.enableTable(tableName);
    LOG.info("=== after restore with 500 row snapshot");
    MobSnapshotTestingUtils.verifyMobRowCount(util, tableName, snapshot0Rows);

    // Restore from snapshot-1
    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName1);
    admin.enableTable(tableName);
    MobSnapshotTestingUtils.verifyMobRowCount(util, tableName, snapshot1Rows);
    cleanUpTest();
  }

  public void testCloneNonExistentSnapshot() throws Exception {
    setUpTest();
    String snapshotName = "random-snapshot-" + System.currentTimeMillis();
    TableName tableName = TableName.valueOf("random-table-" + System.currentTimeMillis());
    try {
      admin.cloneSnapshot(snapshotName, tableName);
      fail("Expected SnapshotDoesNotExistException, got succeeded cloneSnapshot()");
    } catch (SnapshotDoesNotExistException e) {
      // expected
    }
    cleanUpTest();
  }

  public void testCloneSnapshot() throws Exception {
    setUpTest();
    TableName clonedTableName = TableName.valueOf("clonedtb-" + System.currentTimeMillis());
    testCloneSnapshot(clonedTableName, snapshotName0, snapshot0Rows);
    testCloneSnapshot(clonedTableName, snapshotName1, snapshot1Rows);
    cleanUpTest();
  }

  private void testCloneSnapshot(final TableName tableName, final byte[] snapshotName,
      int snapshotRows) throws IOException, InterruptedException {
    // create a new table from snapshot
    admin.cloneSnapshot(snapshotName, tableName);
    MobSnapshotTestingUtils.verifyMobRowCount(util, tableName, snapshotRows);

    util.deleteTable(tableName);
  }

  public void testRestoreSnapshotOfCloned() throws Exception {
    setUpTest();
    TableName clonedTableName = TableName.valueOf("clonedtb-" + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName0, clonedTableName);
    MobSnapshotTestingUtils.verifyMobRowCount(util, clonedTableName, snapshot0Rows);
    admin.snapshot(Bytes.toString(snapshotName2), clonedTableName, SnapshotDescription.Type.FLUSH);
    util.deleteTable(clonedTableName);

    admin.cloneSnapshot(snapshotName2, clonedTableName);
    MobSnapshotTestingUtils.verifyMobRowCount(util, clonedTableName, snapshot0Rows);
    util.deleteTable(clonedTableName);
    cleanUpTest();
  }

  @Override
  public int runTestFromCommandLine() throws Exception {
    testAll();
    return 0;
  }

  @Override
  public TableName getTablename() {
    return tableName;
  }

  @Override
  protected Set<String> getColumnFamilies() {
    return Sets.newHashSet(Bytes.toString(FAMILY));
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int status =  ToolRunner.run(conf, new IntegrationTestMobRestoreFlushSnapshotFromClient(), args);
    System.exit(status);
  }
}