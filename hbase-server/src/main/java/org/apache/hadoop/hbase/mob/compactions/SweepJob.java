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
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.mob.MobZookeeper;
import org.apache.hadoop.hbase.regionserver.MobFileStore;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.zookeeper.KeeperException;

public class SweepJob {

  private FileSystem fs;

  public SweepJob(FileSystem fs) {
    this.fs = fs;
  }

  private static final Log LOG = LogFactory.getLog(SweepJob.class);

  public void sweep(MobFileStore store, Configuration conf) throws IOException,
      ClassNotFoundException, InterruptedException, KeeperException {
    Configuration newConf = new Configuration(conf);
    ZKUtil.applyClusterKeyToConf(newConf, conf.get(MobConstants.MOB_COMPACTION_ZOOKEEPER));
    MobZookeeper zk = MobZookeeper.newInstance(newConf);
    if (!zk.lockStore(store.getTableName(), store.getFamilyName())) {
      LOG.warn("Can not lock the store " + store.getFamilyName()
          + ". The major compaction in Apache HBase may be in-progress. Please re-run the job.");
      return;
    }
    try {
      // 1. delete expired files by checking the column family.
      // 2. Mapper. output. <filename, KeyValue List>
      MobUtils.cleanExpiredData(fs, store);

      Scan scan = new Scan();
      // Do not retrieve the mob data when scanning
      scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
      scan.setFilter(new ReferenceOnlyFilter());
      scan.setCaching(10000);

      Job job = prepareTableJob(store, scan, SweepMapper.class, Text.class, KeyValue.class,
          SweepReducer.class, Text.class, Writable.class, TextOutputFormat.class, newConf);
      job.getConfiguration().set(TableInputFormat.SCAN_COLUMN_FAMILY, store.getFamilyName());

      /**
       * Record the compaction begin time
       */
      job.getConfiguration().set(MobConstants.MOB_COMPACTION_START_DATE,
          String.valueOf(new Date().getTime()));

      job.setPartitionerClass(MobFilePathHashPartitioner.class);
      job.waitForCompletion(true);
    } finally {
      zk.unlockStore(store.getTableName(), store.getFamilyName());
    }

  }

  protected Job prepareTableJob(MobFileStore store, Scan scan, Class<? extends TableMapper> mapper,
      Class<? extends Writable> mapOutputKey, Class<? extends Cell> mapOutputValue,
      Class<? extends Reducer> reducer, Class<? extends WritableComparable> reduceOutputKey,
      Class<? extends Writable> reduceOutputValue, Class<? extends OutputFormat> outputFormat,
      Configuration conf) throws IOException {

    Job job = Job.getInstance(conf);

    job.setJarByClass(mapper);
    TableMapReduceUtil.initTableMapperJob(store.getTableName(), scan, mapper, reduceOutputKey,
        reduceOutputValue, job);

    job.setInputFormatClass(TableInputFormat.class);
    job.setMapOutputKeyClass(mapOutputKey);
    job.setMapOutputValueClass(mapOutputValue);
    job.setReducerClass(reducer);
    job.setOutputFormatClass(outputFormat);
    String jobName = getCustomJobName(this.getClass().getSimpleName(), mapper, reducer,
        store.getTableName(), store.getFamilyName());
    // Add the Output directory of the FileOutputFormat
    Path outputPath = MobUtils.getCompactionOutputPath(conf.get(
        MobConstants.MOB_COMPACTION_TEMP_DIR, MobConstants.DEFAULT_MOB_COMPACTION_TEMP_DIR),
        jobName);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setJobName(jobName);
    // delete this output path
    fs.delete(outputPath, true);
    // delete the temp directory of the mob files in case the failure in the previous
    // execution.
    Path workingPath = MobUtils.getCompactionWorkingPath(conf.get(
        MobConstants.MOB_COMPACTION_TEMP_DIR, MobConstants.DEFAULT_MOB_COMPACTION_TEMP_DIR),
        jobName);
    job.getConfiguration().set(MobConstants.MOB_COMPACTION_JOB_WORKING_DIR, 
        workingPath.toString());
    fs.delete(workingPath, true);
    return job;
  }

  private static String getCustomJobName(String className, Class<? extends Mapper> mapper,
      Class<? extends Reducer> reducer, String tableName, String familyName) {
    StringBuilder name = new StringBuilder();
    name.append(className);
    name.append('-').append(mapper.getSimpleName());
    name.append('-').append(reducer.getSimpleName());
    name.append('-').append(tableName);
    name.append('-').append(familyName);
    return name.toString();
  }
}