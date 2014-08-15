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
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestMobStoreScanner {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static String TN = "testMobStoreScanner";
  private final static byte [] row1 = Bytes.toBytes("row1");
  private final static byte [] row2 = Bytes.toBytes("row2");
  private final static byte [] family = Bytes.toBytes("family");
  private final static byte [] qf1 = Bytes.toBytes("qualifier1");
  private final static byte [] qf2 = Bytes.toBytes("qualifier2");
  protected final byte[] qf3 = Bytes.toBytes("qualifier3");
  protected final byte[] qf4 = Bytes.toBytes("qualifier4");
  protected final byte[] qf5 = Bytes.toBytes("qualifier5");
  protected final byte[] qf6 = Bytes.toBytes("qualifier6");
  private final static byte [] value1 = Bytes.toBytes("value1");
  private final static byte [] value2 = Bytes.toBytes("value2");
  private final static byte [] value3 = Bytes.toBytes("value3");
  private final static byte [] value4 = Bytes.toBytes("value4");
  private final static byte [] value5 = Bytes.toBytes("value5");
  private final static byte [] value6 = Bytes.toBytes("value6");
  private static HTable table;
  private static HBaseAdmin admin;
  private static HColumnDescriptor hcd;
  private static HTableDescriptor desc;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
    TEST_UTIL.getConfiguration().setClass(
        DefaultStoreEngine.DEFAULT_STORE_FLUSHER_CLASS_KEY,
        DefaultMobStoreFlusher.class, StoreFlusher.class);

    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public void setUp() throws Exception {
    desc = new HTableDescriptor(TableName.valueOf(TN));
    hcd = new HColumnDescriptor(family);
    hcd.setValue(MobConstants.IS_MOB, "true");
    hcd.setMaxVersions(4);
    desc.addFamily(hcd);
    admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    admin.createTable(desc);
    table = new HTable(TEST_UTIL.getConfiguration(), TN);
  }

  public void tearDown() throws Exception {
    admin.disableTable(TN);
    admin.deleteTable(TN);
    admin.close();
  }

  /**
   * set the scan attribute
   *
   * @param reversed if true, scan will be backward order
   * @param mobScanRaw if true, scan will get the mob reference
   * @return this
   */
  public void setScan(Scan scan, boolean reversed, boolean mobScanRaw) {
      scan.setReversed(reversed);
      scan.addColumn(family, qf2);
      scan.setMaxVersions(4);
      if(mobScanRaw) {
        scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
      }
  }

  @Test
  public void testMobStoreScanner() throws Exception {
	  testGetFromFiles(false);
	  testGetReferences(false);
	  testGetFromMemStore(false);
  }

  @Test
  public void testReversedMobStoreScanner() throws Exception {
	  testGetFromFiles(true);
	  testGetReferences(true);
	  testGetFromMemStore(true);
  }

  public void testGetFromFiles(boolean reversed) throws Exception {
    try {
      setUp();
      long ts1 = System.currentTimeMillis();
      long ts2 = ts1 + 1;
      long ts3 = ts1 + 2;

      Put put1 = new Put(row1);
      put1.add(family, qf1, ts3, value1);
      put1.add(family, qf2, ts2, value2);
      put1.add(family, qf3, ts1, value3);
      table.put(put1);

      Put put2 = new Put(row2);
      put2.add(family, qf4, ts3, value4);
      put2.add(family, qf5, ts2, value5);
      put2.add(family, qf6, ts1, value6);
      table.put(put2);

      table.flushCommits();
      admin.flush(TN);

      Scan scan = new Scan();
      setScan(scan, reversed, false);
      ResultScanner scanner = table.getScanner(scan);

      Result result = scanner.next();
      int size = 0;
      while (result != null) {
        size++;
        List<Cell> cells = result.getColumnCells(family, qf2);
        Assert.assertEquals(1, cells.size());
        Assert.assertEquals(Bytes.toString(value2),
            Bytes.toString(CellUtil.cloneValue(cells.get(0))));
        result = scanner.next();
      }
      scanner.close();
      Assert.assertEquals(1, size);
      tearDown();
    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void testGetFromMemStore(boolean reversed) throws Exception {
    try {
      setUp();
      long ts1 = System.currentTimeMillis();
      long ts2 = ts1 + 1;
      long ts3 = ts1 + 2;

      Put put1 = new Put(row1);
      put1.add(family, qf1, ts3, value1);
      put1.add(family, qf2, ts2, value2);
      put1.add(family, qf3, ts1, value3);
      table.put(put1);

      Put put2 = new Put(row2);
      put2.add(family, qf4, ts3, value4);
      put2.add(family, qf5, ts2, value5);
      put2.add(family, qf6, ts1, value6);
      table.put(put2);

      Scan scan = new Scan();
      setScan(scan, reversed, false);
      ResultScanner scanner = table.getScanner(scan);

      Result result = scanner.next();
      int size = 0;
      while (result != null) {
        size++;
        List<Cell> cells = result.getColumnCells(family, qf2);
        Assert.assertEquals(1, cells.size());
        Assert.assertEquals(Bytes.toString(value2),
            Bytes.toString(CellUtil.cloneValue(cells.get(0))));
        result = scanner.next();
      }
      scanner.close();
      Assert.assertEquals(1, size);
      tearDown();
    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void testGetReferences(boolean reversed) throws Exception {
    try {
      setUp();
      long ts1 = System.currentTimeMillis();
      long ts2 = ts1 + 1;
      long ts3 = ts1 + 2;

      Put put1 = new Put(row1);
      put1.add(family, qf1, ts3, value1);
      put1.add(family, qf2, ts2, value2);
      put1.add(family, qf3, ts1, value3);
      table.put(put1);

      Put put2 = new Put(row2);
      put2.add(family, qf4, ts3, value4);
      put2.add(family, qf5, ts2, value5);
      put2.add(family, qf6, ts1, value6);
      table.put(put2);

      table.flushCommits();
      admin.flush(TN);

      Scan scan = new Scan();
      setScan(scan, reversed, true);

      ResultScanner scanner = table.getScanner(scan);
      Result result = scanner.next();
      int size = 0;
      while (result != null) {
        size++;
        List<Cell> cells = result.getColumnCells(family, qf2);
        Assert.assertEquals(1, cells.size());
        Assert.assertEquals(Bytes.toString(row1),
            Bytes.toString(CellUtil.cloneRow(cells.get(0))));
        Assert.assertEquals(Bytes.toString(family),
            Bytes.toString(CellUtil.cloneFamily(cells.get(0))));
        Assert.assertFalse(Bytes.toString(value2).equals(
            Bytes.toString(CellUtil.cloneValue(cells.get(0)))));
        byte[] referenceValue = CellUtil.cloneValue(cells.get(0));
        String fileName = Bytes.toString(referenceValue, 8, referenceValue.length-8);
        Path mobFamilyPath;
        mobFamilyPath = new Path(MobUtils.getMobRegionPath(TEST_UTIL.getConfiguration(),
            TableName.valueOf(TN)), hcd.getNameAsString());
        Path targetPath = new Path(mobFamilyPath, fileName);
        FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
        Assert.assertTrue(fs.exists(targetPath));

        result = scanner.next();
      }
      scanner.close();
      Assert.assertEquals(1, size);
      tearDown();
    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
