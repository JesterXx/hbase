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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMobFileStore extends HBaseTestCase {
  public static final Log LOG = LogFactory.getLog(TestMobFileStore.class);

  private MobFileStore mobFileStore;
  private Path mobFilePath;
  private FileSystem fs;
  private KeyValue seekKey1;
  private KeyValue seekKey2;
  private KeyValue seekKey3;
  private Date currentDate = new Date();

  private final byte[] TABLE = Bytes.toBytes("table");
  private final byte[] FAMILY = Bytes.toBytes("family");

  private final byte[] ROW = Bytes.toBytes("row");
  private final byte[] ROW2 = Bytes.toBytes("row2");
  private final byte[] VALUE = Bytes.toBytes("value");
  private final byte[] VALUE2 = Bytes.toBytes("value2");
  private final byte[] QF1 = Bytes.toBytes("qf1");
  private final byte[] QF2 = Bytes.toBytes("qf2");
  private final byte[] QF3 = Bytes.toBytes("qf3");

  private void init(String methodName) throws IOException {
    init(methodName, HBaseConfiguration.create());
  }

  private void init(String methodName, Configuration conf) throws IOException {
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
    hcd.setMaxVersions(4);
    hcd.setValue(MobConstants.IS_MOB, "true");
    init(methodName, conf, hcd);
  }

  private void init(String methodName, Configuration conf, HColumnDescriptor hcd)
      throws IOException {
    // Setting up a Store
    Path basedir = this.testDir;
    fs = FileSystem.get(conf);

    HTableDescriptor htd = new HTableDescriptor(TABLE);
    htd.addFamily(hcd);
    Path homePath = new Path(basedir, Bytes.toString(TABLE) + Path.SEPARATOR
        + Bytes.toString(FAMILY));
    fs.mkdirs(homePath);
    mobFileStore = MobFileStore.create(conf, fs, TableName.valueOf(TABLE), hcd);

    KeyValue key1 = new KeyValue(ROW, FAMILY, QF1, 1, VALUE);
    KeyValue key2 = new KeyValue(ROW, FAMILY, QF2, 1, VALUE);
    KeyValue key3 = new KeyValue(ROW2, FAMILY, QF3, 1, VALUE2);
    KeyValue[] keys = new KeyValue[] { key1, key2, key3 };
    int maxKeyCount = keys.length;
    HRegionInfo regionStartKey = new HRegionInfo();
    StoreFile.Writer mobWriter = mobFileStore.createWriterInTmp(currentDate,
        maxKeyCount, hcd.getCompactionCompression(), regionStartKey.getStartKey());
    mobFilePath = mobWriter.getPath();

    mobWriter.append(key1);
    mobWriter.append(key2);
    mobWriter.append(key3);
    mobWriter.close();
    String targetPathName = MobUtils.formatDate(currentDate);

    long valueLength1 = key1.getValueLength();
    long valueLength2 = key2.getValueLength();
    long valueLength3 = key3.getValueLength();
    byte[] referenceValue =
            Bytes.toBytes(targetPathName + Path.SEPARATOR
                + mobFilePath.getName());
    byte[] newReferenceValue1 = Bytes.add(Bytes.toBytes(valueLength1), referenceValue);
    byte[] newReferenceValue2 = Bytes.add(Bytes.toBytes(valueLength2), referenceValue);
    byte[] newReferenceValue3 = Bytes.add(Bytes.toBytes(valueLength3), referenceValue);
    seekKey1 = new KeyValue(ROW, FAMILY, QF1, Long.MAX_VALUE, newReferenceValue1);
    seekKey2 = new KeyValue(ROW, FAMILY, QF2, Long.MAX_VALUE, newReferenceValue2);
    seekKey3 = new KeyValue(ROW2, FAMILY, QF3, Long.MAX_VALUE, newReferenceValue3);
  }

  @Test
  public void testCommitFile() throws Exception {
    init(getName());
    String targetPathName = MobUtils.formatDate(new Date());
    Path targetPath = new Path(mobFileStore.getPath(), (targetPathName
        + Path.SEPARATOR + mobFilePath.getName()));
    fs.delete(targetPath);
    assertFalse(fs.exists(targetPath));
    //commit file
    mobFileStore.commitFile(mobFilePath, targetPath);
    assertTrue(fs.exists(targetPath));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testResolve() throws Exception {
    init(getName());
    String targetPathName = MobUtils.formatDate(currentDate);
    Path targetPath = new Path(mobFileStore.getPath(), targetPathName);
    mobFileStore.commitFile(mobFilePath, targetPath);
    //resolve
    Cell resultKey1 = mobFileStore.resolve(seekKey1, false);
    Cell resultKey2 = mobFileStore.resolve(seekKey2, false);
    Cell resultKey3 = mobFileStore.resolve(seekKey3, false);
    //compare
    assertEquals(VALUE, resultKey1.getValue());
    assertEquals(VALUE, resultKey2.getValue());
    assertEquals(VALUE2, resultKey3.getValue());
  }
}
