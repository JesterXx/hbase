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
package org.apache.hadoop.hbase.mob.compactions;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * A mob compactor to directly compact the mob files.
 */
@InterfaceAudience.Private
public abstract class MobCompactor2 {

  protected RegionServerServices rss;
  protected Region region;
  protected FileSystem fs;
  protected Configuration conf;
  protected TableName tableName;
  protected HColumnDescriptor column;

  protected Path mobTableDir;
  protected Path mobFamilyDir;

  public MobCompactor2(RegionServerServices rss, Region region, TableName tableName,
    HColumnDescriptor column) {
    this.rss = rss;
    this.region = region;
    this.conf = rss.getConfiguration();
    this.fs = rss.getFileSystem();
    this.tableName = tableName;
    this.column = column;
    mobTableDir = FSUtils.getTableDir(MobUtils.getMobHome(conf), tableName);
    mobFamilyDir = MobUtils.getMobFamilyPath(conf, tableName, column.getNameAsString());
  }

  /**
   * Compacts the mob files for the current column family.
   * @return The paths of new mob files generated in the compaction.
   * @throws IOException
   */
  public List<Path> compact() throws IOException {
    return compact(false);
  }

  /**
   * Compacts the mob files by compaction type for the current column family.
   * @param allFiles Whether add all mob files into the compaction.
   * @return The paths of new mob files generated in the compaction.
   * @throws IOException
   */
  public List<Path> compact(boolean allFiles) throws IOException {
    return compact(Arrays.asList(fs.listStatus(mobFamilyDir)), allFiles);
  }

  /**
   * Compacts the candidate mob files.
   * @param files The candidate mob files.
   * @param allFiles Whether add all mob files into the compaction.
   * @return The paths of new mob files generated in the compaction.
   * @throws IOException
   */
  public abstract List<Path> compact(List<FileStatus> files, boolean allFiles)
    throws IOException;
}
