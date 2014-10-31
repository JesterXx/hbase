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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.IntegrationTestBase;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.IntegrationTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

/**
 * Test restore snapshots from the client
 */
@Category(IntegrationTests.class)
public class IntegrationTestMobRestoreSnapshotFromClient extends IntegrationTestBase {

  final Log LOG = LogFactory.getLog(getClass());
  private static final byte[] FAMILY = Bytes.toBytes("cf");
  private byte[] emptySnapshot;
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
    tableName =
        TableName.valueOf("testtb-" + tid);
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
    // enable table and insert data
    admin.enableTable(tableName);
    MobSnapshotTestingUtils.loadMobData(util, table, 500, FAMILY);
    snapshot0Rows = MobSnapshotTestingUtils.countMobRows(table);
    admin.disableTable(tableName);

    // take a snapshot
    admin.snapshot(snapshotName0, tableName);

    // enable table and insert more data
    admin.enableTable(tableName);
    MobSnapshotTestingUtils.loadMobData(util, table, 500, FAMILY);
    snapshot1Rows = MobSnapshotTestingUtils.countMobRows(table);
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

  @Test
  public void testAll() throws Exception {
    testRestoreSnapshot();
    testCloneAndRestoreSnapshot();
    testCorruptedSnapshot();
    testRestoreSchemaChange();
    testCloneSnapshotOfCloned();
  }

  public void testRestoreSnapshot() throws Exception {
    setUpTest();
    MobSnapshotTestingUtils.verifyMobRowCount(util, tableName, snapshot1Rows);
    admin.disableTable(tableName);
    admin.snapshot(snapshotName1, tableName);
    // Restore from snapshot-0
    admin.restoreSnapshot(snapshotName0);
    admin.enableTable(tableName);
    MobSnapshotTestingUtils.verifyMobRowCount(util, tableName, snapshot0Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());

    // Restore from emptySnapshot
    admin.disableTable(tableName);
    admin.restoreSnapshot(emptySnapshot);
    admin.enableTable(tableName);
    MobSnapshotTestingUtils.verifyMobRowCount(util, tableName, 0);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());

    // Restore from snapshot-1
    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName1);
    admin.enableTable(tableName);
    MobSnapshotTestingUtils.verifyMobRowCount(util, tableName, snapshot1Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());

    // Restore from snapshot-1
    util.deleteTable(tableName);
    admin.restoreSnapshot(snapshotName1);
    MobSnapshotTestingUtils.verifyMobRowCount(util, tableName, snapshot1Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());
    cleanUpTest();
  }

  public void testRestoreSchemaChange() throws Exception {
    setUpTest();
    byte[] TEST_FAMILY2 = Bytes.toBytes("cf2");

    HTable table = new HTable(util.getConfiguration(), tableName);

    // Add one column family and put some data in it
    admin.disableTable(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAMILY2);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(0L);
    admin.addColumn(tableName, hcd);
    admin.enableTable(tableName);
    assertEquals(2, table.getTableDescriptor().getFamilies().size());
    HTableDescriptor htd = admin.getTableDescriptor(tableName);
    assertEquals(2, htd.getFamilies().size());
    MobSnapshotTestingUtils.loadMobData(util, table, 100, TEST_FAMILY2);
    long snapshot2Rows = snapshot1Rows + 100;
    assertEquals(snapshot2Rows, MobSnapshotTestingUtils.countMobRows(table));
    assertEquals(100, MobSnapshotTestingUtils.countMobRows(table, TEST_FAMILY2));

    // Take a snapshot
    admin.disableTable(tableName);
    admin.snapshot(snapshotName2, tableName);

    // Restore the snapshot (without the cf)
    admin.restoreSnapshot(snapshotName0);
    admin.enableTable(tableName);
    assertEquals(1, table.getTableDescriptor().getFamilies().size());
    try {
      MobSnapshotTestingUtils.countMobRows(table, TEST_FAMILY2);
      fail("family '" + Bytes.toString(TEST_FAMILY2) + "' should not exists");
    } catch (NoSuchColumnFamilyException e) {
      // expected
    }
    assertEquals(snapshot0Rows, MobSnapshotTestingUtils.countMobRows(table));
    htd = admin.getTableDescriptor(tableName);
    assertEquals(1, htd.getFamilies().size());

    // Restore back the snapshot (with the cf)
    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName2);
    admin.enableTable(tableName);
    htd = admin.getTableDescriptor(tableName);
    assertEquals(2, htd.getFamilies().size());
    assertEquals(2, table.getTableDescriptor().getFamilies().size());
    assertEquals(100, MobSnapshotTestingUtils.countMobRows(table, TEST_FAMILY2));
    assertEquals(snapshot2Rows, MobSnapshotTestingUtils.countMobRows(table));
    table.close();
    cleanUpTest();
  }

  public void testCloneSnapshotOfCloned() throws Exception {
    setUpTest();
    TableName clonedTableName =
        TableName.valueOf("clonedtb-" + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName0, clonedTableName);
    MobSnapshotTestingUtils.verifyMobRowCount(util, clonedTableName, snapshot0Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(clonedTableName, admin, getNumReplicas());
    admin.disableTable(clonedTableName);
    admin.snapshot(snapshotName2, clonedTableName);
    util.deleteTable(clonedTableName);
    waitCleanerRun();

    admin.cloneSnapshot(snapshotName2, clonedTableName);
    MobSnapshotTestingUtils.verifyMobRowCount(util, clonedTableName, snapshot0Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(clonedTableName, admin, getNumReplicas());
    util.deleteTable(clonedTableName);
    cleanUpTest();
  }

  public void testCloneAndRestoreSnapshot() throws Exception {
    setUpTest();
    util.deleteTable(tableName);
    waitCleanerRun();

    admin.cloneSnapshot(snapshotName0, tableName);
    MobSnapshotTestingUtils.verifyMobRowCount(util, tableName, snapshot0Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());
    waitCleanerRun();

    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName0);
    admin.enableTable(tableName);
    MobSnapshotTestingUtils.verifyMobRowCount(util, tableName, snapshot0Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());
    cleanUpTest();
  }

  public void testCorruptedSnapshot() throws Exception {
    setUpTest();
    corruptSnapshot(util, Bytes.toString(snapshotName0));
    TableName cloneName = TableName.valueOf("corruptedClone-" + System.currentTimeMillis());
    try {
      admin.cloneSnapshot(snapshotName0, cloneName);
      fail("Expected CorruptedSnapshotException, got succeeded cloneSnapshot()");
    } catch (CorruptedSnapshotException e) {
      // Got the expected corruption exception.
      // check for no references of the cloned table.
      assertFalse(admin.tableExists(cloneName));
    } catch (Exception e) {
      fail("Expected CorruptedSnapshotException got: " + e);
    }
    cleanUpTest();
  }

  private void waitCleanerRun() throws InterruptedException {
    if (!util.isDistributedCluster()) {
      util.getMiniHBaseCluster().getMaster().getHFileCleaner().choreForTesting();
    }
  }

  /**
   * Corrupt the specified snapshot by deleting some files.
   *
   * @param util {@link HBaseTestingUtility}
   * @param snapshotName name of the snapshot to corrupt
   * @return array of the corrupted HFiles
   * @throws IOException on unexecpted error reading the FS
   */
  public static ArrayList corruptSnapshot(final HBaseTestingUtility util, final String snapshotName)
      throws IOException {
    Path rootDir = new Path(util.getConfiguration().get(HConstants.HBASE_DIR));
    final FileSystem fs =rootDir.getFileSystem(util.getConfiguration());
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName,
                                                                       rootDir);
    SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    final TableName table = TableName.valueOf(snapshotDesc.getTable());

    final ArrayList corruptedFiles = new ArrayList();
    final Configuration conf = util.getConfiguration();
    SnapshotReferenceUtil.visitTableStoreFiles(conf, fs, snapshotDir, snapshotDesc,
        new SnapshotReferenceUtil.StoreFileVisitor() {
      @Override
      public void storeFile(final HRegionInfo regionInfo, final String family,
            final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
        String region = regionInfo.getEncodedName();
        String hfile = storeFile.getName();
        HFileLink link = HFileLink.create(conf, table, region, family, hfile);
        if (corruptedFiles.size() % 2 == 0) {
          fs.delete(link.getAvailablePath(fs), true);
          corruptedFiles.add(hfile);
        }
      }
    });

    assertTrue(corruptedFiles.size() > 0);
    return corruptedFiles;
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
    int status =  ToolRunner.run(conf, new IntegrationTestMobRestoreSnapshotFromClient(), args);
    System.exit(status);
  }
}