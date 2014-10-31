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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.IntegrationTestBase;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.IntegrationTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotFileInfo;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

/**
 * Test Export Snapshot Tool
 */
@Category(IntegrationTests.class)
public class IntegrationTestMobExportSnapshot extends IntegrationTestBase {

  private final Log LOG = LogFactory.getLog(getClass());

  private final static byte[] FAMILY = Bytes.toBytes("cf");
  private byte[] emptySnapshotName;
  private byte[] snapshotName;
  private int tableNumFiles;
  private TableName tableName;
  private Admin admin;

  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(getConf());
    setUpBaseConf(util.getConfiguration());
    util.initializeCluster(1);

    if (!util.isDistributedCluster()) {
      // also need MR when running without a real cluster
      util.startMiniMapReduceCluster();
    }
  }

  @Override
  public void cleanUpCluster() throws Exception {
    if (!util.isDistributedCluster()) {
      util.shutdownMiniMapReduceCluster();
    }
    super.cleanUpCluster();
  }

  public static void setUpBaseConf(Configuration conf) {
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    conf.setInt("hbase.regionserver.msginterval", 100);
    conf.setInt("hbase.client.pause", 250);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    conf.setBoolean("hbase.master.enabletable.roundrobin", true);
    conf.setInt("mapreduce.map.maxattempts", 10);
    conf.setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 0);
  }

  /**
   * Create a table and take a snapshot of the table used by the export test.
   */
  public void setUpTest() throws Exception {

    this.admin = util.getHBaseAdmin();
    long tid = System.currentTimeMillis();
    tableName = TableName.valueOf("testtb-" + tid);
    snapshotName = Bytes.toBytes("snaptb0-" + tid);
    emptySnapshotName = Bytes.toBytes("emptySnaptb0-" + tid);

    // create Table
    MobSnapshotTestingUtils.createMobTable(util, tableName, 1, FAMILY);

    // Take an empty snapshot
    admin.snapshot(emptySnapshotName, tableName);

    // Add some rows
    MobSnapshotTestingUtils.loadMobData(util, tableName, 10, FAMILY);
    tableNumFiles = admin.getTableRegions(tableName).size();

    // take a snapshot
    admin.snapshot(snapshotName, tableName);
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

  @Test
  public void testAll() throws Exception {
    testBalanceSplit();
    testExportWithTargetName();
    testExportFileSystemState();
    testConsecutiveExports();
    testEmptyExportFileSystemState();
    testExportFileSystemStateWithSkipTmp();
    testExportRetry();
  }

  /**
   * Verfy the result of getBalanceSplits() method.
   * The result are groups of files, used as input list for the "export" mappers.
   * All the groups should have similar amount of data.
   *
   * The input list is a pair of file path and length.
   * The getBalanceSplits() function sort it by length,
   * and assign to each group a file, going back and forth through the groups.
   */
  public void testBalanceSplit() throws Exception {
    setUpTest();
    // Create a list of files
    List<Pair<SnapshotFileInfo, Long>> files = new ArrayList<Pair<SnapshotFileInfo, Long>>();
    for (long i = 0; i <= 20; i++) {
      SnapshotFileInfo fileInfo = SnapshotFileInfo.newBuilder()
        .setType(SnapshotFileInfo.Type.HFILE)
        .setHfile("file-" + i)
        .build();
      files.add(new Pair<SnapshotFileInfo, Long>(fileInfo, i));
    }

    // Create 5 groups (total size 210)
    //    group 0: 20, 11, 10,  1 (total size: 42)
    //    group 1: 19, 12,  9,  2 (total size: 42)
    //    group 2: 18, 13,  8,  3 (total size: 42)
    //    group 3: 17, 12,  7,  4 (total size: 42)
    //    group 4: 16, 11,  6,  5 (total size: 42)
    List<List<Pair<SnapshotFileInfo, Long>>> splits = ExportSnapshot.getBalancedSplits(files, 5);
    assertEquals(5, splits.size());

    String[] split0 = new String[] {"file-20", "file-11", "file-10", "file-1", "file-0"};
    verifyBalanceSplit(splits.get(0), split0, 42);
    String[] split1 = new String[] {"file-19", "file-12", "file-9",  "file-2"};
    verifyBalanceSplit(splits.get(1), split1, 42);
    String[] split2 = new String[] {"file-18", "file-13", "file-8",  "file-3"};
    verifyBalanceSplit(splits.get(2), split2, 42);
    String[] split3 = new String[] {"file-17", "file-14", "file-7",  "file-4"};
    verifyBalanceSplit(splits.get(3), split3, 42);
    String[] split4 = new String[] {"file-16", "file-15", "file-6",  "file-5"};
    verifyBalanceSplit(splits.get(4), split4, 42);
    cleanUpTest();
  }

  private void verifyBalanceSplit(final List<Pair<SnapshotFileInfo, Long>> split,
      final String[] expected, final long expectedSize) throws Exception {
    setUpTest();
    assertEquals(expected.length, split.size());
    long totalSize = 0;
    for (int i = 0; i < expected.length; ++i) {
      Pair<SnapshotFileInfo, Long> fileInfo = split.get(i);
      assertEquals(expected[i], fileInfo.getFirst().getHfile());
      totalSize += fileInfo.getSecond();
    }
    assertEquals(expectedSize, totalSize);
    cleanUpTest();
  }

  /**
   * Verify if exported snapshot and copied files matches the original one.
   */
  public void testExportFileSystemState() throws Exception {
    setUpTest();
    testExportFileSystemState(tableName, snapshotName, snapshotName, tableNumFiles);
    cleanUpTest();
  }

  public void testExportFileSystemStateWithSkipTmp() throws Exception {
    setUpTest();
    util.getConfiguration().setBoolean(ExportSnapshot.CONF_SKIP_TMP, true);
    testExportFileSystemState(tableName, snapshotName, snapshotName, tableNumFiles);
    cleanUpTest();
  }

  public void testEmptyExportFileSystemState() throws Exception {
    setUpTest();
    testExportFileSystemState(tableName, emptySnapshotName, emptySnapshotName, 0);
    cleanUpTest();
  }

  public void testConsecutiveExports() throws Exception {
    setUpTest();
    Path copyDir = getLocalDestinationDir();
    testExportFileSystemState(tableName, snapshotName, snapshotName, tableNumFiles, copyDir, false);
    testExportFileSystemState(tableName, snapshotName, snapshotName, tableNumFiles, copyDir, true);
    removeExportDir(copyDir);
    cleanUpTest();
  }

  public void testExportWithTargetName() throws Exception {
    setUpTest();
    final byte[] targetName = Bytes.toBytes("testExportWithTargetName");
    testExportFileSystemState(tableName, snapshotName, targetName, tableNumFiles);
    cleanUpTest();
  }

  private void testExportFileSystemState(final TableName tableName, final byte[] snapshotName,
      final byte[] targetName, int filesExpected) throws Exception {
    Path copyDir = getHdfsDestinationDir();
    testExportFileSystemState(tableName, snapshotName, targetName, filesExpected, copyDir, false);
    removeExportDir(copyDir);
  }

  /**
   * Test ExportSnapshot
   */
  private void testExportFileSystemState(final TableName tableName, final byte[] snapshotName,
      final byte[] targetName, int filesExpected, Path copyDir, boolean overwrite)
      throws Exception {
    URI hdfsUri = FileSystem.get(util.getConfiguration()).getUri();
    FileSystem fs = FileSystem.get(copyDir.toUri(), new Configuration());
    copyDir = copyDir.makeQualified(fs);
    List<String> opts = new ArrayList<String>();
    opts.add("-snapshot");
    opts.add(Bytes.toString(snapshotName));
    opts.add("-copy-to");
    opts.add(copyDir.toString());
    if (targetName != snapshotName) {
      opts.add("-target");
      opts.add(Bytes.toString(targetName));
    }
    if (overwrite) opts.add("-overwrite");

    // Export Snapshot
    int res = ExportSnapshot.innerMain(util.getConfiguration(),
        opts.toArray(new String[opts.size()]));
    assertEquals(0, res);

    // Verify File-System state
    FileStatus[] rootFiles = fs.listStatus(copyDir);
    assertEquals(filesExpected > 0 ? 2 : 1, rootFiles.length);
    for (FileStatus fileStatus: rootFiles) {
      String name = fileStatus.getPath().getName();
      assertTrue(fileStatus.isDirectory());
      assertTrue(name.equals(HConstants.SNAPSHOT_DIR_NAME) ||
                 name.equals(HConstants.HFILE_ARCHIVE_DIRECTORY));
    }
    Set<String> snapshotFiles = verifySnapshot(fs, copyDir, tableName, Bytes.toString(targetName));
    assertEquals(filesExpected, snapshotFiles.size());
  }

  /**
   * Check that ExportSnapshot will succede if something fails but the retry succede.
   */
  public void testExportRetry() throws Exception {
    setUpTest();
    assertEquals(0, runExportAndInjectFailures(snapshotName, true));
    cleanUpTest();
  }

  /*
   * Execute the ExportSnapshot job injecting failures
   */
  private int runExportAndInjectFailures(final byte[] snapshotName, boolean retry)
      throws Exception {
    Path copyDir = getLocalDestinationDir();
    URI hdfsUri = FileSystem.get(util.getConfiguration()).getUri();
    FileSystem fs = FileSystem.get(copyDir.toUri(), new Configuration());
    copyDir = copyDir.makeQualified(fs);

    Configuration conf = new Configuration(util.getConfiguration());
    conf.setBoolean(ExportSnapshot.CONF_TEST_FAILURE, true);
    conf.setBoolean(ExportSnapshot.CONF_TEST_RETRY, retry);

    // Export Snapshot
    Path sourceDir;
    if (util.isDistributedCluster()) {
      sourceDir = new Path(conf.get(HConstants.HBASE_DIR));
    } else {
      sourceDir = util.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    }
    int res = ExportSnapshot.innerMain(conf, new String[] {
      "-snapshot", Bytes.toString(snapshotName),
      "-copy-from", sourceDir.toString(),
      "-copy-to", copyDir.toString()
    });
    return res;
  }

  /*
   * Verify if the files exists
   */
  private Set<String> verifySnapshot(final FileSystem fs, final Path rootDir,
      final TableName tableName, final String snapshotName) throws IOException {
    final Path exportedSnapshot = new Path(rootDir,
      new Path(HConstants.SNAPSHOT_DIR_NAME, snapshotName));
    final Set<String> snapshotFiles = new HashSet<String>();
    final Path exportedArchive = new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY);
    SnapshotReferenceUtil.visitReferencedFiles(util.getConfiguration(), fs, exportedSnapshot,
          new SnapshotReferenceUtil.SnapshotVisitor() {
        @Override
        public void storeFile(final HRegionInfo regionInfo, final String family,
            final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
          if(MobUtils.isMobRegionInfo(regionInfo))
            return;
          String hfile = storeFile.getName();
          snapshotFiles.add(hfile);
          if (storeFile.hasReference()) {
            // Nothing to do here, we have already the reference embedded
          } else {
            verifyNonEmptyFile(new Path(exportedArchive,
              new Path(FSUtils.getTableDir(new Path("./"), tableName),
                  new Path(regionInfo.getEncodedName(), new Path(family, hfile)))));
          }
        }

        @Override
        public void logFile (final String server, final String logfile)
            throws IOException {
          snapshotFiles.add(logfile);
          verifyNonEmptyFile(new Path(exportedSnapshot, new Path(server, logfile)));
        }

        private void verifyNonEmptyFile(final Path path) throws IOException {
          assertTrue(path + " should exists", fs.exists(path));
          assertTrue(path + " should not be empty", fs.getFileStatus(path).getLen() > 0);
        }
    });

    // Verify Snapshot description
    SnapshotDescription desc = SnapshotDescriptionUtils.readSnapshotInfo(fs, exportedSnapshot);
    assertTrue(desc.getName().equals(snapshotName));
    assertTrue(desc.getTable().equals(tableName.getNameAsString()));
    return snapshotFiles;
  }

  private Path getHdfsDestinationDir() {
    Path rootDir;
    if (util.isDistributedCluster()) {
      rootDir = new Path(conf.get(HConstants.HBASE_DIR));
    } else {
      rootDir = util.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    }
    Path path = new Path(new Path(rootDir, "export-test"), "export-" + System.currentTimeMillis());
    LOG.info("HDFS export destination path: " + path);
    return path;
  }

  private Path getLocalDestinationDir() {
    Path path = util.getDataTestDir("local-export-" + System.currentTimeMillis());
    LOG.info("Local export destination path: " + path);
    return path;
  }

  private void removeExportDir(final Path path) throws IOException {
    FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
    fs.delete(path, true);
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
    int status =  ToolRunner.run(conf, new IntegrationTestMobExportSnapshot(), args);
    System.exit(status);
  }
}