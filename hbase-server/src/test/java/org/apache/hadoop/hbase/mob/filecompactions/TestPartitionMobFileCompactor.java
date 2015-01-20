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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobFileName;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.mob.filecompactions.MobFileCompactionRequest.CompactionType;
import org.apache.hadoop.hbase.mob.filecompactions.PartitionMobFileCompactionRequest.CompactedPartition;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestPartitionMobFileCompactor {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static String family = "family";
  private final static String qf = "qf";
  private HColumnDescriptor hcd = new HColumnDescriptor(family);
  protected static final char FIRST_CHAR = 'a';
  protected static final char LAST_CHAR = 'z';
  private Configuration conf = TEST_UTIL.getConfiguration();
  private CacheConfig cacheConf = new CacheConfig(conf);
  private byte[] startKey1;
  private byte[] startKey2;
  private Random random = new Random();
  private FileSystem fs;
  private List<String> expectedStartKeys = new ArrayList<>();
  private List<FileStatus> delFiles = new ArrayList<>();
  private List<FileStatus> allFiles = new ArrayList<>();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
    TEST_UTIL.getConfiguration().setInt("hfile.format.version", 3);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public void init(String tableName) throws Exception {
    fs = FileSystem.get(conf);
    Path testDir = FSUtils.getRootDir(conf);
    Path mobTestDir = new Path(testDir, MobConstants.MOB_DIR_NAME);
    Path basePath = new Path(new Path(mobTestDir, tableName), family);

    String mobSuffix = UUID.randomUUID().toString().replaceAll("-", "");
    startKey1 = Bytes.toBytes(random.nextInt());
    startKey2 = Bytes.toBytes(random.nextInt());
    MobFileName mobFileName1 = MobFileName.create(startKey1, MobUtils.formatDate(new Date()),
        mobSuffix);
    MobFileName mobFileName2 = MobFileName.create(startKey2, MobUtils.formatDate(new Date()),
        mobSuffix);
    expectedStartKeys.add(mobFileName1.getStartKey());
    expectedStartKeys.add(mobFileName2.getStartKey());
    // create two mob files.
    createStoreFile(basePath, mobFileName1, family, qf);
    createStoreFile(basePath, mobFileName2, family, qf);

    String delSuffix = UUID.randomUUID().toString().replaceAll("-", "") + "_del";
    MobFileName delFileName1 = MobFileName.create(startKey1, MobUtils.formatDate(new Date()),
        delSuffix);
    MobFileName delFileName2 = MobFileName.create(startKey2, MobUtils.formatDate(new Date()),
        delSuffix);
    // create two del files
    createStoreFile(basePath, delFileName1, family, qf);
    createStoreFile(basePath, delFileName2, family, qf);

    for (FileStatus file : fs.listStatus(basePath)) {
      allFiles.add(file);
      if (file.getPath().getName().endsWith("_del")) {
        delFiles.add(file);
      }
    }
  }

  @Test
  public void testCompactionSelect() throws Exception {
    String tableName = "testCompactionSelect";
    init(tableName);
    PartitionMobFileCompactor compactor = new PartitionMobFileCompactor(conf, fs,
        TableName.valueOf(tableName), hcd) {
      @Override
      public List<Path> compact(List<FileStatus> files) throws IOException {
        if (files == null || files.isEmpty()) {
          return null;
        }
        PartitionMobFileCompactionRequest request = select(files);
        // assert the compaction type is all files
         Assert.assertTrue((request.type).equals(CompactionType.ALL_FILES));
        // assert get the right partitions
        compareCompactedPartitions(request.compactedPartitions);
        // assert get the right del files
        compareDelFiles(request.delFiles);
        return null;
      }
    };
    compactor.compact(allFiles);
  }

  @Test
  public void testCompactDelFiles() throws Exception {
    String tableName = "testCompactDelFiles";
    init(tableName);
    // set the max del file count
    TEST_UTIL.getConfiguration().setInt(MobConstants.MOB_DELFILE_MAX_COUNT, 1);
    PartitionMobFileCompactor compactor = new PartitionMobFileCompactor(conf, fs,
        TableName.valueOf(tableName), hcd) {
      @Override
      protected List<Path> performCompact(PartitionMobFileCompactionRequest request)
          throws IOException {
        List<Path> delFilePaths = new ArrayList<Path>();
        for (FileStatus delFile : request.delFiles) {
          delFilePaths.add(delFile.getPath());
        }
        List<Path> newDelPaths = compactDelFiles(request, delFilePaths);
        // assert the del files are merged.
        Assert.assertTrue(newDelPaths.size() == 1);
        return null;
      }
    };
    compactor.compact(allFiles);
  }

  /**
   * compare the compacted partitions.
   * @param partitions the collection of CompactedPartitions
   */
  private void compareCompactedPartitions(Collection<CompactedPartition> partitions) {
    List<String> actualKeys = new ArrayList<>();
    for (CompactedPartition partition : partitions) {
      actualKeys.add(partition.getPartitionId().getStartKey());
    }
    Collections.sort(expectedStartKeys);
    Collections.sort(actualKeys);
    Assert.assertEquals(expectedStartKeys.size(), actualKeys.size());
    for (int i = 0; i < expectedStartKeys.size(); i++) {
      Assert.assertEquals(expectedStartKeys.get(i), actualKeys.get(i));
    }
  }

  /**
   * compare the del files.
   * @param allDelFiles all the del files
   */
  private void compareDelFiles(Collection<FileStatus> allDelFiles) {
    int i = 0;
    for (FileStatus file : allDelFiles) {
      Assert.assertEquals(delFiles.get(i), file);
      i++;
    }
  }

  /**
   * create store file
   * @param basePath the path to create file
   * @mobFileName the mob file name
   * @family the family name
   * @qualifier the column qualifier
   */
  private void createStoreFile(Path basePath, MobFileName mobFileName, String family, String qualifier)
      throws IOException {
    HFileContext meta = new HFileContextBuilder().withBlockSize(8 * 1024).build();
    StoreFile.Writer mobFileWriter = new StoreFile.WriterBuilder(conf, cacheConf, fs)
        .withFileContext(meta).withFilePath(new Path(basePath, mobFileName.getFileName())).build();
    writeStoreFile(mobFileWriter, Bytes.toBytes(family), Bytes.toBytes(qualifier));
  }

  /**
   * write data to store file
   * @param writer the storefile writer
   * @family the family name
   * @qualifier the column qualifier
   */
  private static void writeStoreFile(final StoreFile.Writer writer, byte[] family, byte[] qualifier)
      throws IOException {
    long now = System.currentTimeMillis();
    try {
      for (char d = FIRST_CHAR; d <= LAST_CHAR; d++) {
        for (char e = FIRST_CHAR; e <= LAST_CHAR; e++) {
          byte[] b = new byte[] { (byte) d, (byte) e };
          writer.append(new KeyValue(b, family, qualifier, now, b));
        }
      }
    } finally {
      writer.close();
    }
  }
}
