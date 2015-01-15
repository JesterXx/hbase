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
package org.apache.hadoop.hbase.replication.master;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.filecompactions.MobFileCompactor;
import org.apache.hadoop.hbase.mob.filecompactions.StripeMobFileCompactor;

public class MobFileCompactChore extends Chore{

  private static final Log LOG = LogFactory.getLog(MobFileCompactChore.class);
  private HMaster master;
  private TableLockManager tableLockManager;

  public MobFileCompactChore(HMaster master) {
    super(master.getServerName() + "-MobFileCompactChore", master.getConfiguration().getInt(
      MobConstants.MOB_COMPACTION_CHORE_PERIOD, MobConstants.DEFAULT_MOB_COMPACTION_CHORE_PERIOD),
      master);
    this.master = master;
    this.tableLockManager = master.getTableLockManager();
  }

  @Override
  protected void chore() {
    try {
      TableDescriptors htds = master.getTableDescriptors();
      Map<String, HTableDescriptor> map = htds.getAll();
      for (HTableDescriptor htd : map.values()) {
        for (HColumnDescriptor hcd : htd.getColumnFamilies()) {
          if (hcd.isMobEnabled()) {
            boolean tableLocked = false;
            TableLock lock = null;
            if (tableLockManager != null) {
              lock = tableLockManager.writeLock(htd.getTableName(), "Run MobFileCompactChore");
            }
            try {
              if (lock != null) {
                lock.acquire();
                tableLocked = true;
              } else {
                tableLocked = true;
              }
              MobFileCompactor compactor = new StripeMobFileCompactor(master.getConfiguration(),
                master.getFileSystem(), htd.getTableName(), hcd);
              compactor.compact();
            } catch (Exception e) {
              LOG.error("Fail to compact the mob files for the column " + hcd.getNameAsString()
                + " in the table " + htd.getNameAsString(), e);
            } finally {
              if (lock != null && tableLocked) {
                try {
                  lock.release();
                } catch (IOException e) {
                  LOG.error(
                    "Fail to release the write lock for the table " + htd.getNameAsString(), e);
                }
              }
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Fail to clean the expired mob files", e);
    }
  }
}
