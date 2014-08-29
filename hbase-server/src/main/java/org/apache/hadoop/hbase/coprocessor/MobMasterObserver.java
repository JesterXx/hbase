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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * The master observer used in MOB.
 * It monitors the operations in HMaster, and takes special actions for MOB.
 *
 */
@InterfaceAudience.Private
public class MobMasterObserver extends BaseMasterObserver {

  private static final Log LOG = LogFactory.getLog(MobMasterObserver.class);

  /**
   * Archive the MOB after the table is deleted.
   */
  @Override
  public void postDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
    MasterFileSystem mfs = ctx.getEnvironment().getMasterServices().getMasterFileSystem();
    FileSystem fs = mfs.getFileSystem();
    Path tableDir = FSUtils.getTableDir(
        MobUtils.getMobHome(ctx.getEnvironment().getConfiguration()), tableName);
    HRegionInfo regionInfo = MobUtils.getMobRegionInfo(tableName);
    Path regionDir = new Path(tableDir, regionInfo.getEncodedName());
    try {
      // Find if the mob files exist
      if (fs.exists(regionDir)) {
        // Archive them if they're there
        HFileArchiver.archiveRegion(fs, ctx.getEnvironment().getMasterServices()
            .getMasterFileSystem().getRootDir(), tableDir, regionDir);
      }
      if (fs.exists(tableDir)) {
        // Delete the mob directory
        fs.delete(tableDir, true);
      }
    } catch (IOException e) {
      LOG.warn("Failed to delete the MOB files", e);
    }
  }
}
