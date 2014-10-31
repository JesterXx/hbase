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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.IntegrationTestBase;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.IntegrationTests;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

/**
 * Test clone snapshots from the client
 */
@Category(IntegrationTests.class)
public class IntegrationTestMobCloneSnapshotFromClient extends IntegrationTestBase {

  private static final byte[] FAMILY = Bytes.toBytes("cf");
  private byte[] emptySnapshot;
  private byte[] snapshotName0;
  private byte[] snapshotName1;
  private byte[] snapshotName2;
  private int snapshot0Rows;
  private int snapshot1Rows;
  private TableName tableName;
  private Admin admin;

  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(getConf());
    setUpBaseConf(util.getConfiguration());
    util.initializeCluster(1);
  }

  public static void setUpBaseConf(Configuration conf) {
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    conf.setBoolean("hbase.online.schema.update.enable", true);
    conf.setInt("hbase.hstore.compactionThreshold", 10);
    conf.setInt("hbase.regionserver.msginterval", 100);
    conf.setInt("hbase.client.pause", 250);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    conf.setBoolean(
        "hbase.master.enabletable.roundrobin", true);
    conf.setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 0);
  }

  /**
   * Initialize the tests with a table filled with some data
   * and two snapshots (snapshotName0, snapshotName1) of different states.
   * The tableName, snapshotNames and the number of rows in the snapshot are initialized.
   */
  public void setUpTest() throws Exception {

    this.admin = util.getHBaseAdmin();
    long tid = System.currentTimeMillis();
    tableName = TableName.valueOf("testtb-" + tid);
    emptySnapshot = Bytes.toBytes("emptySnaptb-" + tid);
    snapshotName0 = Bytes.toBytes("snaptb0-" + tid);
    snapshotName1 = Bytes.toBytes("snaptb1-" + tid);
    snapshotName2 = Bytes.toBytes("snaptb2-" + tid);

    // create Table and disable it
    MobSnapshotTestingUtils.createMobTable(util, tableName, getNumReplicas(), FAMILY);
    admin.disableTable(tableName);

    // take an empty snapshot
    admin.snapshot(emptySnapshot, tableName);

    HTable table = new HTable(util.getConfiguration(), tableName);
    try {
      // enable table and insert data
      admin.enableTable(tableName);
      MobSnapshotTestingUtils.loadMobData(util, table, 100, FAMILY);
      snapshot0Rows = MobSnapshotTestingUtils.countMobRows(table);
      admin.disableTable(tableName);

      // take a snapshot
      admin.snapshot(snapshotName0, tableName);

      // enable table and insert more data
      admin.enableTable(tableName);
      MobSnapshotTestingUtils.loadMobData(util, table, 100, FAMILY);
      snapshot1Rows = MobSnapshotTestingUtils.countMobRows(table);
      admin.disableTable(tableName);

      // take a snapshot of the updated table
      admin.snapshot(snapshotName1, tableName);

      // re-enable table
      admin.enableTable(tableName);
    } finally {
      table.close();
    }
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

  @Test
  public void testAll() throws Exception {
    testCloneNonExistentSnapshot();
    testCloneOnMissingNamespace();
    testCloneSnapshot();
    testCloneSnapshotCrossNamespace();
    testCloneLinksAfterDelete();
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

  public void testCloneOnMissingNamespace() throws Exception {
    setUpTest();
    TableName clonedTableName = TableName.valueOf("unknownNS:clonetb");
    try {
      admin.cloneSnapshot(snapshotName1, clonedTableName);
      fail("Expected NamespaceNotFoundException, got succeeded cloneSnapshot()");
    } catch (NamespaceNotFoundException e) {
      // expected
    }
    cleanUpTest();
  }

  public void testCloneSnapshot() throws Exception {
    setUpTest();
    TableName clonedTableName = TableName.valueOf("clonedtb-" + System.currentTimeMillis());
    testCloneSnapshot(clonedTableName, snapshotName0, snapshot0Rows);
    testCloneSnapshot(clonedTableName, snapshotName1, snapshot1Rows);
    testCloneSnapshot(clonedTableName, emptySnapshot, 0);
    cleanUpTest();
  }

  private void testCloneSnapshot(final TableName tableName, final byte[] snapshotName,
      int snapshotRows) throws IOException, InterruptedException {
    // create a new table from snapshot
    admin.cloneSnapshot(snapshotName, tableName);
    MobSnapshotTestingUtils.verifyMobRowCount(util, tableName, snapshotRows);

    verifyReplicasCameOnline(tableName);
    util.deleteTable(tableName);
  }

  protected void verifyReplicasCameOnline(TableName tableName) throws IOException {
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());
  }

  public void testCloneSnapshotCrossNamespace() throws Exception {
    setUpTest();
    String nsName = "testCloneSnapshotCrossNamespace" + System.currentTimeMillis();
    admin.createNamespace(NamespaceDescriptor.create(nsName).build());
    TableName clonedTableName =
        TableName.valueOf(nsName, "clonedtb-" + System.currentTimeMillis());
    testCloneSnapshot(clonedTableName, snapshotName0, snapshot0Rows);
    testCloneSnapshot(clonedTableName, snapshotName1, snapshot1Rows);
    testCloneSnapshot(clonedTableName, emptySnapshot, 0);
    admin.deleteNamespace(nsName);
    cleanUpTest();
  }

  /**
   * Verify that tables created from the snapshot are still alive after source table deletion.
   * @throws Exception
   */
  public void testCloneLinksAfterDelete() throws Exception {
    setUpTest();
    // Clone a table from the first snapshot
    TableName clonedTableName = TableName.valueOf("clonedtb1-" + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName0, clonedTableName);
    MobSnapshotTestingUtils.verifyMobRowCount(util, clonedTableName, snapshot0Rows);

    // Take a snapshot of this cloned table.
    admin.disableTable(clonedTableName);
    admin.snapshot(snapshotName2, clonedTableName);

    // Clone the snapshot of the cloned table
    TableName clonedTableName2 = TableName.valueOf("clonedtb2-" + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName2, clonedTableName2);
    MobSnapshotTestingUtils.verifyMobRowCount(util, clonedTableName2, snapshot0Rows);
    admin.disableTable(clonedTableName2);

    // Remove the original table
    util.deleteTable(tableName);
    waitCleanerRun();

    // Verify the first cloned table
    admin.enableTable(clonedTableName);
    MobSnapshotTestingUtils.verifyMobRowCount(util, clonedTableName, snapshot0Rows);

    // Verify the second cloned table
    admin.enableTable(clonedTableName2);
    MobSnapshotTestingUtils.verifyMobRowCount(util, clonedTableName2, snapshot0Rows);
    admin.disableTable(clonedTableName2);

    // Delete the first cloned table
    util.deleteTable(clonedTableName);
    waitCleanerRun();

    // Verify the second cloned table
    admin.enableTable(clonedTableName2);
    MobSnapshotTestingUtils.verifyMobRowCount(util, clonedTableName2, snapshot0Rows);

    // Clone a new table from cloned
    TableName clonedTableName3 = TableName.valueOf("clonedtb3-" + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName2, clonedTableName3);
    MobSnapshotTestingUtils.verifyMobRowCount(util, clonedTableName3, snapshot0Rows);

    // Delete the cloned tables
    util.deleteTable(clonedTableName2);
    util.deleteTable(clonedTableName3);
    admin.deleteSnapshot(snapshotName2);
    cleanUpTest();
  }

  private void waitCleanerRun() throws InterruptedException {
    if (!util.isDistributedCluster()) {
      util.getMiniHBaseCluster().getMaster().getHFileCleaner().choreForTesting();
    }
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
    int status =  ToolRunner.run(conf, new IntegrationTestMobCloneSnapshotFromClient(), args);
    System.exit(status);
  }
}