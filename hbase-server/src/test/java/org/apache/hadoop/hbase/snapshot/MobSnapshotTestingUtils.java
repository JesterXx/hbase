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
package org.apache.hadoop.hbase.snapshot;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.junit.Assert;

public class MobSnapshotTestingUtils {

  /**
   * Create the Mob Table.
   */
  public static void createMobTable(final HBaseTestingUtility util, final TableName tableName,
      int regionReplication, final byte[]... families) throws IOException, InterruptedException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.setRegionReplication(regionReplication);
    for (byte[] family: families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      hcd.setValue(MobConstants.IS_MOB, "true");
      htd.addFamily(hcd);
    }
    byte[][] splitKeys = SnapshotTestingUtils.getSplitKeys();
    util.getHBaseAdmin().createTable(htd, splitKeys);
    SnapshotTestingUtils.waitForTableToBeOnline(util, tableName);
    assertEquals((splitKeys.length + 1) * regionReplication,
        util.getHBaseAdmin().getTableRegions(tableName).size());
  }

  /**
   * Return the number of rows in the given table.
   */
  public static int countMobRows(final HTable table) throws IOException {
    Scan scan = new Scan();
    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (@SuppressWarnings("unused") Result res : results) {
      count++;
      List<Cell> cells = res.listCells();
      for(Cell cell : cells) {
        // Verify the value
        Assert.assertNotNull(CellUtil.cloneValue(cell));
      }
    }
    results.close();
    return count;
  }

  /**
   * Return the number of rows in the given table.
   */
  public static int countMobRows(final HTable table, final byte[]... families) throws IOException {
    Scan scan = new Scan();
    for (byte[] family: families) {
      scan.addFamily(family);
    }
    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (@SuppressWarnings("unused") Result res : results) {
      count++;
      List<Cell> cells = res.listCells();
      for(Cell cell : cells) {
        // Verify the value
        Assert.assertNotNull(CellUtil.cloneValue(cell));
      }
    }
    results.close();
    return count;
  }

  public static void verifyMobRowCount(final HBaseTestingUtility util, final TableName tableName,
      long expectedRows) throws IOException {
    HTable table = new HTable(util.getConfiguration(), tableName);
    try {
      assertEquals(expectedRows, countMobRows(table));
    } finally {
      table.close();
    }
  }
}
