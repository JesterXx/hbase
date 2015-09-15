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
package org.apache.hadoop.hbase.mob.compactions;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.mob.compactions.MobCompactionRequest.CompactionType;
import org.apache.hadoop.hbase.mob.compactions.RegionServerMobCompactionManager.MobCompactionSubprocedurePool;
import org.apache.hadoop.hbase.procedure.ProcedureMember;
import org.apache.hadoop.hbase.procedure.Subprocedure;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;

public class MobCompactionSubprocedure extends Subprocedure {
  private static final Log LOG = LogFactory.getLog(MobCompactionSubprocedure.class);

  private final TableName tableName;
  private final String columnName;
  private final RegionServerServices rss;
  private final List<Region> regions;
  private final MobCompactionSubprocedurePool taskManager;
  private boolean allRegionsOnline;
  private boolean allFiles;

  public MobCompactionSubprocedure(ProcedureMember member,
    ForeignExceptionDispatcher errorListener, long wakeFrequency, long timeout,
    RegionServerServices rss, List<Region> regions, TableName tableName, String columnName,
    MobCompactionSubprocedurePool taskManager, boolean allRegionsOnline, boolean allFiles) {
    super(member, tableName.getNameAsString(), errorListener, wakeFrequency, timeout);
    this.tableName = tableName;
    this.columnName = columnName;
    this.rss = rss;
    this.regions = regions;
    this.taskManager = taskManager;
    this.allRegionsOnline = allRegionsOnline;
    this.allFiles = allFiles;
  }

  @Override
  public void acquireBarrier() throws ForeignException {
    if (regions.isEmpty()) {
      // No regions on this RS, we are basically done.
      return;
    }

    monitor.rethrowException();

    for (Region region : regions) {
      // submit one task per region for parallelize by region.
      taskManager.submitTask(new RegionMobCompactionTask(region));
      monitor.rethrowException();
    }

    // wait for everything to complete.
    boolean success = false;
    LOG.debug("Mob compaction tasks submitted for " + regions.size() + " regions");
    try {
      success = taskManager.waitForOutstandingTasks();
      LOG.info("Mob compaction tasks for region server " + rss.getServerName() + " are finished["
        + success + "]");
    } catch (InterruptedException e) {
      throw new ForeignException(getMemberName(), e);
    }
    // TODO add nodes to zookeeper if all the tasks are finished successfully
  }

  private class RegionMobCompactionTask implements Callable<Boolean> {
    Region region;

    RegionMobCompactionTask(Region region) {
      this.region = region;
    }

    @Override
    public Boolean call() throws Exception {
      LOG.debug("Starting region operation mob compaction on " + region);
      region.startRegionOperation();
      try {
        LOG.debug("Mob compaction of region " + region.toString() + " started...");
        return mobCompactRegion(region, allFiles);
      } finally {
        LOG.debug("Closing region operation mob compaction on " + region);
        region.closeRegionOperation();
      }
    }
  }

  private boolean mobCompactRegion(Region region, boolean allFiles) throws IOException {
    HColumnDescriptor column = region.getTableDesc().getFamily(Bytes.toBytes(columnName));
    PartitionedMobCompactor2 compactor = new PartitionedMobCompactor2(rss, region, tableName,
      column);
    compactor.compact(allFiles);
    return compactor.getPartitionedMobCompactionRequest().getCompactionType() ==
      CompactionType.ALL_FILES;
  }

  @Override
  public byte[] insideBarrier() throws ForeignException {
    // No-Op
    return new byte[0];
  }

  @Override
  public void cleanup(Exception e) {
    LOG.info(
      "Aborting all mob compaction subprocedure task threads for '" + tableName.getNameAsString()
        + "' due to error", e);
    try {
      taskManager.cancelTasks();
    } catch (InterruptedException e1) {
      Thread.currentThread().interrupt();
    }
  }

}
