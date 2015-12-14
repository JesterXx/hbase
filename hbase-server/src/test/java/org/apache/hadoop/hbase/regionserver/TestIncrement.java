/**
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class TestIncrement {
  private static final Log LOG = LogFactory.getLog(TestIncrement.class);
  @Rule
  public TestName name = new TestName();
  private static HBaseTestingUtility TEST_UTIL;

  @Before
  public void setUp() throws Exception {
    TEST_UTIL = HBaseTestingUtility.createLocalHTU();
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.cleanupTestDir();
  }

  /**
   * Increments a single cell a bunch of times.
   */
  private static class SingleCellIncrementer extends Thread {
    private final int count;
    private final HRegion region;
    private final Increment increment;

    SingleCellIncrementer(final int i, final int count, final HRegion region,
      final Increment increment) {
      super("" + i);
      setDaemon(true);
      this.count = count;
      this.region = region;
      this.increment = increment;
    }

    @Override
    public void run() {
      for (int i = 0; i < this.count; i++) {
        try {
          this.region.increment(this.increment);
          // LOG.info(getName() + " " + i);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    Increment getIncrement() {
      return this.increment;
    }
  }

  @Test
  public void testContendedSingleCellIncrementer() throws IOException, InterruptedException {
    String methodName = this.name.getMethodName();
    TableName tableName = TableName.valueOf(methodName);
    byte[] incrementBytes = Bytes.toBytes("increment");
    Increment increment = new Increment(incrementBytes);
    increment.addColumn(incrementBytes, incrementBytes, 1);
    Configuration conf = TEST_UTIL.getConfiguration();
    final WAL wal = new FSHLog(FileSystem.get(conf), TEST_UTIL.getDataTestDir(), TEST_UTIL
      .getDataTestDir().toString(), conf);
    final HRegion region = (HRegion) TEST_UTIL.createLocalHRegion(tableName,
      HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, false, Durability.SKIP_WAL, wal,
      incrementBytes);
    try {
      final int threadCount = 100;
      final int incrementCount = 25000;
      SingleCellIncrementer[] threads = new SingleCellIncrementer[threadCount];
      for (int i = 0; i < threads.length; i++) {
        threads[i] = new SingleCellIncrementer(i, incrementCount, region, increment);
      }
      for (int i = 0; i < threads.length; i++) {
        threads[i].start();
      }
      for (int i = 0; i < threads.length; i++) {
        threads[i].join();
      }
      increment = new Increment(incrementBytes);
      Result r = region.get(new Get(increment.getRow()));
      long total = CellUtil.getValueAsLong(r.listCells().get(0));
      assertEquals(incrementCount * incrementCount + 1, total);
    } finally {
      region.close();
      wal.close();
    }
  }

  private void assertEquals(int i, long total) {
    // TODO Auto-generated method stub

  }
}