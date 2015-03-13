/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.LockTimeoutException;
import org.apache.hadoop.hbase.master.ZKLockManager.ZKLock;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests the zk lock manager
 */
@Category({ MasterTests.class, LargeTests.class })
public class TestZKLockManager {

  private static final TableName TABLE_NAME = TableName.valueOf("TestZKLocks");

  private static final byte[] FAMILY = Bytes.toBytes("f1");

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Before
  public void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
    TEST_UTIL.startMiniCluster(1);
  }

  @After
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testLockTimeoutException() throws Exception {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    final String lockName = MobUtils.getLockName(TABLE_NAME, Bytes.toString(FAMILY));
    final ZKLockManager zkLockManager = master.getZKLockManager();
    final CountDownLatch lockCounter = new CountDownLatch(1);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        ZKLock writeLock = zkLockManager.writeLock(lockName, "write lock");
        writeLock.acquire();
        lockCounter.countDown();
        return null;
      }
    });
    lockCounter.await();
    ZKLock readLock = zkLockManager.readLock(lockName, "read lock");
    boolean hasError = false;
    try {
      readLock.acquire(1000);
    } catch (LockTimeoutException e) {
      hasError = true;
    }
    assertTrue(hasError);
    executor.shutdownNow();
  }

  @Test
  public void testMoreReadLocks() throws Exception {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    final String lockName = MobUtils.getLockName(TABLE_NAME, Bytes.toString(FAMILY));
    final ZKLockManager zkLockManager = master.getZKLockManager();
    final CountDownLatch lockCounter = new CountDownLatch(1);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        ZKLock readLock = zkLockManager.readLock(lockName, "read lock");
        readLock.acquire();
        lockCounter.countDown();
        return null;
      }
    });
    lockCounter.await();
    ZKLock readLock = zkLockManager.readLock(lockName, "read lock");
    // should not throw exception
    readLock.acquire(0);
    executor.shutdownNow();
  }

  @Test(timeout = 600000)
  public void testReapAllLocks() throws Exception {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    final ZKLockManager masterLockManager = master.getZKLockManager();
    ZKLockManager regionServerLockManager = regionServer.getZKLockManager();

    String locks[] = { "lock1", "lock2", "lock3", "lock4" };
    ExecutorService executor = Executors.newFixedThreadPool(6);

    final CountDownLatch writeLocksObtained = new CountDownLatch(4);
    final CountDownLatch writeLocksAttempted = new CountDownLatch(10);

    // 6 threads will be stuck waiting for the zk lock
    for (int i = 0; i < locks.length; i++) {
      final String lock = locks[i];
      for (int j = 0; j < i + 1; j++) { // i+1 write locks attempted for table[i]
        executor.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            writeLocksAttempted.countDown();
            masterLockManager.writeLock(lock, "testReapAllLocks").acquire();
            writeLocksObtained.countDown();
            return null;
          }
        });
      }
    }
    writeLocksObtained.await();
    writeLocksAttempted.await();
    // now reap all zk locks
    regionServerLockManager.reapWriteLocks();
    // should not throw exception
    regionServerLockManager.writeLock(locks[locks.length - 1], "write lock").acquire(0);
    executor.shutdownNow();
  }
}
