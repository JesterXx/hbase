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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mob.MobUtils;

/**
 * Scanner scans both the memstore and the MOB Store. Coalesce KeyValue stream into List<KeyValue>
 * for a single row.
 * 
 */
public class MobStoreScanner extends StoreScanner {

  private static final Log LOG = LogFactory.getLog(MobStoreScanner.class);
  private boolean cacheMobBlocks = false;

  protected MobStoreScanner(Store store, boolean cacheBlocks, Scan scan,
      final NavigableSet<byte[]> columns, long ttl, int minVersions, long readPt) {
    super(store, cacheBlocks, scan, columns, ttl, minVersions, readPt);
    cacheMobBlocks = MobUtils.isCacheMobBlocks(scan);
  }

  public MobStoreScanner(Store store, ScanInfo scanInfo, Scan scan,
      List<? extends KeyValueScanner> scanners, ScanType scanType, long smallestReadPoint,
      long earliestPutTs) throws IOException {
    super(store, scanInfo, scan, scanners, scanType, smallestReadPoint, earliestPutTs);
    cacheMobBlocks = MobUtils.isCacheMobBlocks(scan);
  }

  public MobStoreScanner(Store store, ScanInfo scanInfo, Scan scan,
      List<? extends KeyValueScanner> scanners, long smallestReadPoint, long earliestPutTs,
      byte[] dropDeletesFromRow, byte[] dropDeletesToRow) throws IOException {
    super(store, scanInfo, scan, scanners, smallestReadPoint, earliestPutTs, dropDeletesFromRow,
        dropDeletesToRow);
    cacheMobBlocks = MobUtils.isCacheMobBlocks(scan);
  }

  public MobStoreScanner(Store store, ScanInfo scanInfo, Scan scan,
      final NavigableSet<byte[]> columns, long readPt) throws IOException {
    super(store, scanInfo, scan, columns, readPt);
    cacheMobBlocks = MobUtils.isCacheMobBlocks(scan);
  }

  @Override
  public boolean next(List<Cell> outResult, int limit) throws IOException {
    boolean result = super.next(outResult, limit);
    if (!MobUtils.isRawMobScan(scan)) {
      // retrieve the mob data
      if (outResult.isEmpty()) {
        return result;
      }
      MobFileStore mobFileStore = MobFileStoreManager.current().getMobFileStore(
          store.getTableName().getNameAsString(), store.getColumnFamilyName());
      if (mobFileStore == null) {
        LOG.error("Failed to find the MobFileStore[tableName:"
            + store.getTableName().getNameAsString() + ",familyName:" + store.getColumnFamilyName()
            + "]!");
      }
      for (int i = 0; i < outResult.size(); i++) {
        Cell cell = outResult.get(i);
        KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
        if (MobUtils.isMobReferenceKeyValue(kv)) {
          if (mobFileStore != null) {
            outResult.set(i, mobFileStore.resolve(kv, cacheMobBlocks));
          } else {
            // replace the outResult with empty KeyValues
            KeyValue emptyKv = new KeyValue(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
                kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(),
                kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength(),
                kv.getTimestamp(), Type.codeToType(kv.getTypeByte()), HConstants.EMPTY_BYTE_ARRAY,
                kv.getValueOffset(), 0, kv.getTags());
            outResult.set(i, emptyKv);
          }
        }
      }
    }
    return result;
  }
}
