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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.MobFileStore;
import org.apache.hadoop.hbase.regionserver.MobFileStoreManager;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;

public class MobStoreCompactionCoprocessor extends BaseRegionObserver {

  private static final Log LOG = LogFactory.getLog(MobStoreCompactionCoprocessor.class);

  // First next check time is the initialization time of this object
  // So the first compact will pass the check
  private long nextCheckTime = System.currentTimeMillis();

  @Override
  public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e, final Store store,
      final StoreFile resultFile, CompactionRequest request) throws IOException {
    if (request.isMajor() && MobUtils.isMobFamily(store.getFamily())) {
      // delete the expired mob files only when the compaction is a major one and the store is a
      // mob one.
      Configuration conf = e.getEnvironment().getConfiguration();

      long ttl = store.getFamily().getTimeToLive();

      // Get the interval, default is a day
      long deleteExpiredMobInterval = conf.getLong(
          MobConstants.MOB_DELETE_EXPIRED_MOBFILES_INTERVAL, 1000 * 60 * 60 * 24);
      if (store.getFamily().getValue(MobConstants.MOB_DELETE_EXPIRED_MOBFILES_INTERVAL) != null) {
        String majorCompactionIntervalString = store.getFamily().getValue(
            MobConstants.MOB_DELETE_EXPIRED_MOBFILES_INTERVAL);
        deleteExpiredMobInterval = Long.parseLong(majorCompactionIntervalString);
      }

      // Check the time to judge whether to clean the obsolete file or not
      long currentTime = System.currentTimeMillis();
      if (currentTime < nextCheckTime) {
        // Not the check time, return directly
        return;
      }

      if (ttl == HConstants.FOREVER) {
        // default is unlimited ttl.
        ttl = Long.MAX_VALUE;
      } else if (ttl == -1) {
        ttl = Long.MAX_VALUE;
      } else {
        // second -> ms adjust for user data
        ttl *= 1000;
      }

      if (conf.getBoolean(MobConstants.MOB_DELETE_EXPIRED_MOBFILES, true)
          && (ttl != Long.MAX_VALUE) && !(store.getFamily().getMinVersions() > 0)) {
        LOG.info("Cleaning the expired MOB files...");
        FileSystem fs = FileSystem.get(conf);

        MobFileStore mobFileStore = MobFileStoreManager.current().getMobFileStore(
            store.getTableName().getNameAsString(), store.getFamily().getNameAsString());

        MobUtils.cleanExpiredData(fs, mobFileStore);
      }
      // After the clean process, change the next check time
      nextCheckTime = currentTime + deleteExpiredMobInterval;
    }
  }
}
