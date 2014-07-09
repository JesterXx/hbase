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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.HLog;

/**
 * A customized HRegion for the BMOB (Binary Medium Object).
 * 
 * The difference between HRegion and HMobRegion is, the HMobRegion creates different Stores
 * according to the type of column family.
 * <ul>
 * <li>If the column family is a Mob one, the method instantiateHStore creates the HMobStore.</li>
 * <li>Otherwise, uses the HRegion.instantiateHStore to create the HStore.</li>
 * </ul>
 * 
 */
public class HMobRegion extends HRegion {

  @Deprecated
  public HMobRegion(final Path tableDir, final HLog log, final FileSystem fs,
      final Configuration confParam, final HRegionInfo regionInfo, final HTableDescriptor htd,
      final RegionServerServices rsServices) {
    super(tableDir, log, fs, confParam, regionInfo, htd, rsServices);
  }

  public HMobRegion(final HRegionFileSystem fs, final HLog log, final Configuration confParam,
      final HTableDescriptor htd, final RegionServerServices rsServices) {
    super(fs, log, confParam, htd, rsServices);
  }

  @Override
  protected HStore instantiateHStore(final HColumnDescriptor family) throws IOException {
    if (MobUtils.isMobFamily(family)) {
      MobFileStoreManager.current().init(conf, this.getFilesystem());
      return new HMobStore(this, family, this.conf);
    }
    return super.instantiateHStore(family);
  }
}
