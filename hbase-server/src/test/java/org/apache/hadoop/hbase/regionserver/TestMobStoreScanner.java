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
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
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
  
  @SuppressWarnings("deprecation")
  @Before
  public void setUp() throws Exception {
    desc = new HTableDescriptor(TN);
    hcd = new HColumnDescriptor(family);
    hcd.setValue(MobConstants.IS_MOB, "true");
    hcd.setMaxVersions(4);
    desc.addFamily(hcd);
    
    admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    admin.createTable(desc);
    table = new HTable(TEST_UTIL.getConfiguration(), TN);
  }
  
  @After
  public void tearDown() throws Exception {
    admin.disableTable(TN);
    admin.deleteTable(TN);
    admin.close();
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void testMobStoreScannerGetFromFiles() throws InterruptedException {
    try {
      
      long ts1 = 1; // System.currentTimeMillis();
      long ts2 = ts1 + 1;
      long ts3 = ts1 + 2;
      
      KeyValue kv13 = new KeyValue(row1, family, qf1, ts3, KeyValue.Type.Put, value1);
      KeyValue kv12 = new KeyValue(row1, family, qf2, ts2, KeyValue.Type.Put, value2);
      KeyValue kv11 = new KeyValue(row1, family, qf3, ts1, KeyValue.Type.Put, value3);

      KeyValue kv23 = new KeyValue(row2, family, qf4, ts3, KeyValue.Type.Put, value4);
      KeyValue kv22 = new KeyValue(row2, family, qf5, ts2, KeyValue.Type.Put, value5);
      KeyValue kv21 = new KeyValue(row2, family, qf6, ts1, KeyValue.Type.Put, value6);
      
      Put put1 = new Put(row1);
      put1.add(kv13);
      put1.add(kv12);
      put1.add(kv11);
      table.put(put1);
      
      Put put2 = new Put(row2);
      put2.add(kv23);
      put2.add(kv22);
      put2.add(kv21);
      table.put(put2);

      table.flushCommits();
      admin.flush(TN);
      
      Scan scan = new Scan();
      scan.addColumn(family, qf2);
      scan.setMaxVersions(4);
      ResultScanner scanner = table.getScanner(scan);

      Result result = scanner.next();
      int size = 0;
      while (result != null) {
        size++;
        List<Cell> cells = result.getColumnCells(family, qf2);
        Assert.assertEquals(1, cells.size());
        Assert.assertEquals(Bytes.toString(value2), Bytes.toString(cells.get(0).getValue()));
        result = scanner.next();
      }
      scanner.close();
      Assert.assertEquals(1, size);
    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void testMobStoreScannerGetFromMemStore() throws InterruptedException {
    try {
      
      long ts1 = 1; // System.currentTimeMillis();
      long ts2 = ts1 + 1;
      long ts3 = ts1 + 2;
      
      KeyValue kv13 = new KeyValue(row1, family, qf1, ts3, KeyValue.Type.Put, value1);
      KeyValue kv12 = new KeyValue(row1, family, qf2, ts2, KeyValue.Type.Put, value2);
      KeyValue kv11 = new KeyValue(row1, family, qf3, ts1, KeyValue.Type.Put, value3);

      KeyValue kv23 = new KeyValue(row2, family, qf4, ts3, KeyValue.Type.Put, value4);
      KeyValue kv22 = new KeyValue(row2, family, qf5, ts2, KeyValue.Type.Put, value5);
      KeyValue kv21 = new KeyValue(row2, family, qf6, ts1, KeyValue.Type.Put, value6);
      
      Put put1 = new Put(row1);
      put1.add(kv13);
      put1.add(kv12);
      put1.add(kv11);
      table.put(put1);
      
      Put put2 = new Put(row2);
      put2.add(kv23);
      put2.add(kv22);
      put2.add(kv21);
      table.put(put2);
      
      Scan scan = new Scan();
      scan.addColumn(family, qf2);
      scan.setMaxVersions(4);
      ResultScanner scanner = table.getScanner(scan);

      Result result = scanner.next();
      int size = 0;
      while (result != null) {
        size++;
        List<Cell> cells = result.getColumnCells(family, qf2);
        Assert.assertEquals(1, cells.size());
        Assert.assertEquals(Bytes.toString(value2), Bytes.toString(cells.get(0).getValue()));
        result = scanner.next();
      }
      scanner.close();
      Assert.assertEquals(1, size);
    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void testMobStoreScannerGetReferences() throws InterruptedException {
    try {
      
      long ts1 = 1; // System.currentTimeMillis();
      long ts2 = ts1 + 1;
      long ts3 = ts1 + 2;
      
      KeyValue kv13 = new KeyValue(row1, family, qf1, ts3, KeyValue.Type.Put, value1);
      KeyValue kv12 = new KeyValue(row1, family, qf2, ts2, KeyValue.Type.Put, value2);
      KeyValue kv11 = new KeyValue(row1, family, qf3, ts1, KeyValue.Type.Put, value3);

      KeyValue kv23 = new KeyValue(row2, family, qf4, ts3, KeyValue.Type.Put, value4);
      KeyValue kv22 = new KeyValue(row2, family, qf5, ts2, KeyValue.Type.Put, value5);
      KeyValue kv21 = new KeyValue(row2, family, qf6, ts1, KeyValue.Type.Put, value6);
      
      Put put1 = new Put(row1);
      put1.add(kv13);
      put1.add(kv12);
      put1.add(kv11);
      table.put(put1);
      
      Put put2 = new Put(row2);
      put2.add(kv23);
      put2.add(kv22);
      put2.add(kv21);
      table.put(put2);
      
      table.flushCommits();
      admin.flush(TN);
      
      Scan scan = new Scan();
      scan.addColumn(family, qf2);
      scan.setMaxVersions(4);
      //set the scan attribute
      scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
      ResultScanner scanner = table.getScanner(scan);

      Result result = scanner.next();
      int size = 0;
      while (result != null) {
        size++;
        List<Cell> cells = result.getColumnCells(family, qf2);
        Assert.assertEquals(1, cells.size());
        Assert.assertEquals(Bytes.toString(row1), Bytes.toString(cells.get(0).getRow()));
        Assert.assertEquals(Bytes.toString(family), Bytes.toString(cells.get(0).getFamily()));
        Assert.assertFalse(Bytes.toString(value2).equals(Bytes.toString(cells.get(0).getValue())));
        
        byte[] referenceValue = cells.get(0).getValue();
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
    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
