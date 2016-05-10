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
import java.util.NavigableSet;

import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

@InterfaceAudience.Private
public class RollingStoreScanner extends StoreScanner {

  public RollingStoreScanner(Store store, ScanInfo scanInfo, Scan scan,
    final NavigableSet<byte[]> columns, long readPt) throws IOException {
    super(store, scanInfo, scan, columns, readPt);
  }

  @Override
  protected void resetKVHeap(List<? extends KeyValueScanner> scanners, CellComparator comparator)
    throws IOException {
    byte[] needRollingScanner = scan.getAttribute(Scan.NEED_ROLLING_SCAN);
    if (needRollingScanner != null && Bytes.toBoolean(needRollingScanner)) {
      heap = new RollingKeyValueHeap(scanners, comparator);
    } else {
      super.resetKVHeap(scanners, comparator);
    }
  }
}
