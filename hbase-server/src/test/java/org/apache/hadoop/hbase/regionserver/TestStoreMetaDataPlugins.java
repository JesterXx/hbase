/*
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.hbase.regionserver.StoreFile.MetaWriter;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Class that test StoreFile meta plug in
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestStoreMetaDataPlugins {

  public static final byte[] KEY = Bytes.toBytes("KEY");
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster;
  @Rule
  public final TestName TEST_NAME = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    cluster = TEST_UTIL.startMiniCluster(1, 1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }


  @Test
  public void testLoadFlushWithMaxTTL() throws Exception {
    Table table = null;
    try {
      TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
      byte[] fam = Bytes.toBytes("info");
      // column names
      byte[] qual = Bytes.toBytes("qual");
      byte[] row1 = Bytes.toBytes("rowb");
      byte[] row2 = Bytes.toBytes("rowc");

      HTableDescriptor desc = new HTableDescriptor(tableName);
      HColumnDescriptor colDesc = new HColumnDescriptor(fam);
      colDesc.setConfiguration(StoreFile.HBASE_HFILE_PLUGINS_KEY,
        TestPlugin.class.getName());
      desc.addFamily(colDesc);
      Admin admin = TEST_UTIL.getHBaseAdmin();
      admin.createTable(desc);
      byte[] value = Bytes.toBytes("value");
      table = TEST_UTIL.getConnection().getTable(tableName);
      Put put = new Put(row1);
      put.addColumn(fam, qual, value);
      table.put(put);

      put = new Put(row2);
      put.addColumn(fam, qual, value);
      table.put(put);

      admin.flush(tableName);

      List<HRegion> regions = cluster.getRegions(tableName);
      assertTrue(regions.size() == 1);
      HRegion region = regions.get(0);
      List<Store> stores = region.getStores();
      assertTrue(stores.size() == 1);
      Store store = stores.get(0);
      Collection<StoreFile> files = store.getStorefiles();
      assertTrue(files.size() == 1);
      StoreFile file = files.iterator().next();
      int numCells = Bytes.toInt(file.getMetadataValue(KEY));
      assertEquals(numCells, 2);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

}

class TestPlugin extends StoreFile.Plugin
{
  MetaWriter metaWriter;
  @Override
  public MetaWriter getMetaWriter() {
    if(metaWriter == null){
      metaWriter = new TestMetaWriter();
    }
    return metaWriter;
  }

}

class TestMetaWriter extends StoreFile.MetaWriter
{

  int cellCount = 0;
  public TestMetaWriter(){}
  @Override
  public void add(Cell cell) {
    cellCount++;
  }

  @Override
  public void appendMetadata(Writer writer)
    throws IOException
  {
    writer.appendFileInfo(TestStoreMetaDataPlugins.KEY, Bytes.toBytes(cellCount));
  }

}