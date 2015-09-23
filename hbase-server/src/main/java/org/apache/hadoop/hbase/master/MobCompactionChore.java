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
package org.apache.hadoop.hbase.master;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.TableState;

/**
 * The Class MobCompactChore for running compaction regularly to merge small mob files.
 */
@InterfaceAudience.Private
public class MobCompactionChore extends ScheduledChore {

  private static final Log LOG = LogFactory.getLog(MobCompactionChore.class);
  private HMaster master;

  public MobCompactionChore(HMaster master, int period) {
    // use the period as initial delay.
    super(master.getServerName() + "-MobCompactionChore", master, period, period, TimeUnit.SECONDS);
    this.master = master;
  }

  @Override
  protected void chore() {
    try {
      TableDescriptors htds = master.getTableDescriptors();
      Map<String, HTableDescriptor> map = htds.getAll();
      for (HTableDescriptor htd : map.values()) {
        if (!master.getTableStateManager().isTableState(htd.getTableName(),
          TableState.State.ENABLED)) {
          continue;
        }
        boolean reported = false;
        try {
          for (HColumnDescriptor hcd : htd.getColumnFamilies()) {
            if (!hcd.isMobEnabled()) {
              continue;
            }
            if (!reported) {
              master.reportMobCompactionStart(htd.getTableName());
              reported = true;
            }
            MasterMobCompactionManager compactionManager = master.getMasterMobCompactionManager();
            List<HColumnDescriptor> columns = new ArrayList<HColumnDescriptor>(1);
            columns.add(hcd);
            Future<Void> future = compactionManager.requestMobCompaction(htd.getTableName(),
              columns, false);
            // wait for the end of the mob compaction
            future.get();
          }
        } finally {
          if (reported) {
            master.reportMobCompactionEnd(htd.getTableName());
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to compact mob files", e);
    }
  }
}
