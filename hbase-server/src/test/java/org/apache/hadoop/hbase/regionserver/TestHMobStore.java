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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;

@Category(MediumTests.class)
public class TestHMobStore {
  public static final Log LOG = LogFactory.getLog(TestHMobStore.class);
  @Rule public TestName name = new TestName();

  HMobStore store;
  byte [] table = Bytes.toBytes("table");
  byte [] family = Bytes.toBytes("family");

  byte [] row = Bytes.toBytes("row");
  byte [] row2 = Bytes.toBytes("row2");
  byte [] qf1 = Bytes.toBytes("qf1");
  byte [] qf2 = Bytes.toBytes("qf2");
  byte [] qf3 = Bytes.toBytes("qf3");
  byte [] qf4 = Bytes.toBytes("qf4");
  byte [] qf5 = Bytes.toBytes("qf5");
  byte [] qf6 = Bytes.toBytes("qf6");

  byte[] value = Bytes.toBytes("value");
  
  HColumnDescriptor hcd;
  FileSystem fs;

  NavigableSet<byte[]> qualifiers =
    new ConcurrentSkipListSet<byte[]>(Bytes.BYTES_COMPARATOR);

  List<Cell> expected = new ArrayList<Cell>();
  List<Cell> results = new ArrayList<Cell>();

  long id = System.currentTimeMillis();
  Get get = new Get(row);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final String DIR = TEST_UTIL.getDataTestDir("TestHMobStore").toString();

  /**
   * Setup
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {
    qualifiers.add(qf1);
    qualifiers.add(qf3);
    qualifiers.add(qf5);

    Iterator<byte[]> iter = qualifiers.iterator();
    while(iter.hasNext()){
      byte [] next = iter.next();
      expected.add(new KeyValue(row, family, next, 1, value));
      get.addColumn(family, next);
      get.setMaxVersions(); // all versions.
    }
  }

  private void init(String methodName, Configuration conf)
  throws IOException {
    hcd = new HColumnDescriptor(family);
    hcd.setValue(MobConstants.IS_MOB, "true");
    hcd.setMaxVersions(4);
    init(methodName, conf, hcd);
  }

  private void init(String methodName, Configuration conf,
      HColumnDescriptor hcd) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(table));
    init(methodName, conf, htd, hcd);
  }

  @SuppressWarnings("deprecation")
  private void init(String methodName, Configuration conf, HTableDescriptor htd,
      HColumnDescriptor hcd) throws IOException {
    //Setting up a Store
    Path basedir = new Path(DIR+methodName);
    Path tableDir = FSUtils.getTableDir(basedir, htd.getTableName());
    String logName = "logs";
    Path logdir = new Path(basedir, logName);
    FileSystem fs = FileSystem.get(conf);
    fs.delete(logdir, true);

    htd.addFamily(hcd);
    HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);
    HLog hlog = HLogFactory.createHLog(fs, basedir, logName, conf);
    HRegion region = new HRegion(tableDir, hlog, fs, conf, info, htd, null);
    store = new HMobStore(region, hcd, conf);
  }

  /**
   * Getting data from memstore
   * @throws IOException
   */
  @Test
  public void testGetFromMemStore() throws IOException {
    final Configuration conf = HBaseConfiguration.create();
    conf.setClass(DefaultStoreEngine.DEFAULT_STORE_FLUSHER_CLASS_KEY,
        DefaultMobStoreFlusher.class, StoreFlusher.class);
    init(name.getMethodName(), conf);

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, value));
    this.store.add(new KeyValue(row, family, qf2, 1, value));
    this.store.add(new KeyValue(row, family, qf3, 1, value));
    this.store.add(new KeyValue(row, family, qf4, 1, value));
    this.store.add(new KeyValue(row, family, qf5, 1, value));
    this.store.add(new KeyValue(row, family, qf6, 1, value));

    Scan scan = new Scan(get);
    InternalScanner scanner = (InternalScanner) store.getScanner(scan,
        scan.getFamilyMap().get(store.getFamily().getName()),
        0);

    List<Cell> results = new ArrayList<Cell>();
    scanner.next(results);
    Collections.sort(results, KeyValue.COMPARATOR);
    scanner.close();

    //Compare
    Assert.assertEquals(expected.size(), results.size());
    for(int i=0; i<results.size(); i++) {
      // Verify the values
      Assert.assertEquals(expected.get(i), results.get(i));
    }
  }

  /**
   * Getting MOB data from files
   * @throws IOException
   */
  @Test
  public void testGetFromFiles() throws IOException {
    final Configuration conf = TEST_UTIL.getConfiguration();
    conf.setClass(DefaultStoreEngine.DEFAULT_STORE_FLUSHER_CLASS_KEY,
        DefaultMobStoreFlusher.class, StoreFlusher.class);
    init(name.getMethodName(), conf);

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, value));
    this.store.add(new KeyValue(row, family, qf2, 1, value));
    //flush
    flush(1);

    //Add more data
    this.store.add(new KeyValue(row, family, qf3, 1, value));
    this.store.add(new KeyValue(row, family, qf4, 1, value));
    //flush
    flush(2);

    //Add more data
    this.store.add(new KeyValue(row, family, qf5, 1, value));
    this.store.add(new KeyValue(row, family, qf6, 1, value));
    //flush
    flush(3);

    Scan scan = new Scan(get);
    InternalScanner scanner = (InternalScanner) store.getScanner(scan,
        scan.getFamilyMap().get(store.getFamily().getName()),
        0);

    List<Cell> results = new ArrayList<Cell>();
    scanner.next(results);
    Collections.sort(results, KeyValue.COMPARATOR);
    scanner.close();

    //Compare
    Assert.assertEquals(expected.size(), results.size());
    for(int i=0; i<results.size(); i++) {
      Assert.assertEquals(expected.get(i), results.get(i));
    }
  }

  /**
   * Getting the reference data from files
   * @throws IOException
   */
  @Test
  public void testGetReferencesFromFiles() throws IOException {
    final Configuration conf = HBaseConfiguration.create();
    conf.setClass(DefaultStoreEngine.DEFAULT_STORE_FLUSHER_CLASS_KEY,
        DefaultMobStoreFlusher.class, StoreFlusher.class);
    init(name.getMethodName(), conf);

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, value));
    this.store.add(new KeyValue(row, family, qf2, 1, value));
    //flush
    flush(1);

    //Add more data
    this.store.add(new KeyValue(row, family, qf3, 1, value));
    this.store.add(new KeyValue(row, family, qf4, 1, value));
    //flush
    flush(2);

    //Add more data
    this.store.add(new KeyValue(row, family, qf5, 1, value));
    this.store.add(new KeyValue(row, family, qf6, 1, value));
    //flush
    flush(3);

    Scan scan = new Scan(get);
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    InternalScanner scanner = (InternalScanner) store.getScanner(scan,
      scan.getFamilyMap().get(store.getFamily().getName()),
      0);

    List<Cell> results = new ArrayList<Cell>();
    scanner.next(results);
    Collections.sort(results, KeyValue.COMPARATOR);
    scanner.close();

    //Compare
    Assert.assertEquals(expected.size(), results.size());
    for(int i=0; i<results.size(); i++) {
      Cell cell = results.get(i);
      Assert.assertTrue(MobUtils.isMobReferenceCell(cell));
    }
  }

  /**
   * Getting data from memstore and files
   * @throws IOException
   */
  @Test
  public void testGetFromMemStoreAndFiles() throws IOException {

    final Configuration conf = HBaseConfiguration.create();
    conf.setClass(DefaultStoreEngine.DEFAULT_STORE_FLUSHER_CLASS_KEY,
        DefaultMobStoreFlusher.class, StoreFlusher.class);
    init(name.getMethodName(), conf);

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, value));
    this.store.add(new KeyValue(row, family, qf2, 1, value));
    //flush
    flush(1);

    //Add more data
    this.store.add(new KeyValue(row, family, qf3, 1, value));
    this.store.add(new KeyValue(row, family, qf4, 1, value));
    //flush
    flush(2);

    //Add more data
    this.store.add(new KeyValue(row, family, qf5, 1, value));
    this.store.add(new KeyValue(row, family, qf6, 1, value));

    Scan scan = new Scan(get);
    InternalScanner scanner = (InternalScanner) store.getScanner(scan,
        scan.getFamilyMap().get(store.getFamily().getName()),
        0);

    List<Cell> results = new ArrayList<Cell>();
    scanner.next(results);
    Collections.sort(results, KeyValue.COMPARATOR);
    scanner.close();

    System.out.println(expected);
    System.out.println(results);
    //Compare
    Assert.assertEquals(expected.size(), results.size());
    for(int i=0; i<results.size(); i++) {
      Assert.assertEquals(expected.get(i), results.get(i));
    }
  }

  /**
   * Getting data from memstore and files
   * @throws IOException
   */
  @Test
  public void testMobCellSizeThreshold() throws IOException {

    final Configuration conf = HBaseConfiguration.create();
    conf.setClass(DefaultStoreEngine.DEFAULT_STORE_FLUSHER_CLASS_KEY,
        DefaultMobStoreFlusher.class, StoreFlusher.class);
    
    HColumnDescriptor hcd;
    hcd = new HColumnDescriptor(family);
    hcd.setValue(MobConstants.IS_MOB, "true");
    hcd.setValue(MobConstants.MOB_THRESHOLD, "100");
    hcd.setMaxVersions(4);
    init(name.getMethodName(), conf, hcd);

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, value));
    this.store.add(new KeyValue(row, family, qf2, 1, value));
    //flush
    flush(1);

    //Add more data
    this.store.add(new KeyValue(row, family, qf3, 1, value));
    this.store.add(new KeyValue(row, family, qf4, 1, value));
    //flush
    flush(2);

    //Add more data
    this.store.add(new KeyValue(row, family, qf5, 1, value));
    this.store.add(new KeyValue(row, family, qf6, 1, value));
    //flush
    flush(3);

    Scan scan = new Scan(get);
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    InternalScanner scanner = (InternalScanner) store.getScanner(scan,
      scan.getFamilyMap().get(store.getFamily().getName()),
      0);

    List<Cell> results = new ArrayList<Cell>();
    scanner.next(results);
    Collections.sort(results, KeyValue.COMPARATOR);
    scanner.close();

    //Compare
    Assert.assertEquals(expected.size(), results.size());
    for(int i=0; i<results.size(); i++) {
      Cell cell = results.get(i);
      //this is not mob reference cell.
      Assert.assertFalse(MobUtils.isMobReferenceCell(cell));
      Assert.assertEquals(expected.get(i), results.get(i));
      Assert.assertEquals(100, MobUtils.getMobThreshold(store.getFamily()));
    }
  }

  /**
   * Flush the memstore
   * @param storeFilesSize
   * @throws IOException
   */
  private void flush(int storeFilesSize) throws IOException{
    this.store.snapshot();
    flushStore(store, id++);
    Assert.assertEquals(storeFilesSize, this.store.getStorefiles().size());
    Assert.assertEquals(0, ((DefaultMemStore)this.store.memstore).kvset.size());
  }

  /**
   * Flush the memstore
   * @param store
   * @param id
   * @throws IOException
   */
  private static void flushStore(HMobStore store, long id) throws IOException {
    StoreFlushContext storeFlushCtx = store.createFlushContext(id);
    storeFlushCtx.prepare();
    storeFlushCtx.flushCache(Mockito.mock(MonitoredTask.class));
    storeFlushCtx.commit(Mockito.mock(MonitoredTask.class));
  }
}
