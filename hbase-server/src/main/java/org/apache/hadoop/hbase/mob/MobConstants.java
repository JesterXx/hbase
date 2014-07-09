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
package org.apache.hadoop.hbase.mob;

import org.apache.hadoop.hbase.util.Bytes;

public class MobConstants {

  public static final String IS_MOB = "is_mob";

  public static final byte MOB_REFERENCE_TAG_TYPE = (byte) 211;
  public static final byte[] MOB_REFERENCE_TAG_VALUE = Bytes.toBytes("ref");

  public static final String MOB_SCAN_RAW = "hbase.mob.scan.raw";
  public static final String MOB_CACHE_BLOCKS = "hbase.mob.cache.blocks";

  public static final String MOB_FILE_CACHE_SIZE_KEY = "hbase.mob.file.cache.size";
  public static final int DEFAULT_MOB_FILE_CACHE_SIZE = 1000;

  public static final String MOB_ROOTDIR = "hbase.mob.rootdir";
  public static final String DEFAULT_MOB_ROOTDIR_NAME = "mob";

  public static final String MOB_COMPACTION_START_DATE = "hbase.mob.compaction.start.date";

  public static final String MOB_DELETE_EXPIRED_MOBFILES = "hbase.mob.delete.expired.mobfiles";
  public static final String MOB_DELETE_EXPIRED_MOBFILES_INTERVAL = 
      "hbase.mob.delete.expired.mobfiles.interval";

  public static final String MOB_COMPACTION_ZOOKEEPER = "hbase.mob.compaction.zookeeper";

  public static final String MOB_COMPACTION_INVALID_FILE_RATIO = 
      "hbase.mob.compaction.invalid.file.ratio";
  public static final String MOB_COMPACTION_SMALL_FILE_THRESHOLD = 
      "hbase.mob.compaction.small.file.threshold";

  public static final float DEFAULT_MOB_COMPACTION_INVALID_FILE_RATIO = 0.3f;
  public static final long DEFAULT_MOB_COMPACTION_SMALL_FILE_THRESHOLD = 64 * 1024 * 1024; // 64M

  public static final String MOB_COMPACTION_TEMP_DIR = "hbase.mob.compaction.temp.dir";
  public static final String DEFAULT_MOB_COMPACTION_TEMP_DIR = "/tmp/hadoop/mobcompaction";

  public static final String MOB_COMPACTION_MEMSTORE_FLUSH_SIZE = 
      "hbase.mob.compaction.memstore.flush.size";
  public static final long DEFAULT_MOB_COMPACTION_MEMSTORE_FLUSH_SIZE = 1024 * 1024 * 128L;
  public static final String MOB_COMPACTION_MEMSTORE_BLOCK_MULTIPLIER = 
      "hbase.mob.compaction.memstore.block.multiplier";

  public static final String MOB_COMPACTION_JOB_WORKING_DIR = 
      "hbase.mob.compaction.job.working.dir";
  public static final String MOB_COMPACTION_OUTPUT_TABLE_FACTORY = 
      "hbase.mob.compaction.output.table.factory";

  private MobConstants() {

  }
}
