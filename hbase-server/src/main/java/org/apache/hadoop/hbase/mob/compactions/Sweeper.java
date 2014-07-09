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
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.MobFileStore;
import org.apache.hadoop.hbase.regionserver.MobFileStoreManager;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;

import com.google.protobuf.ServiceException;

public class Sweeper extends Configured implements Tool {
  private HBaseAdmin admin;
  private Set<String> existingTables = new HashSet<String>();
  private FileSystem fs;

  public void init() throws IOException, ServiceException {
    Configuration conf = getConf();
    HBaseAdmin.checkHBaseAvailable(conf);
    this.admin = new HBaseAdmin(conf);
    this.fs = FileSystem.get(conf);
    MobFileStoreManager.current().init(conf, fs);
  }

  public void sweepFamily(String table, String family) throws IOException, InterruptedException,
      ClassNotFoundException, KeeperException {
    if (!existingTables.contains(table)) {
      if (!admin.tableExists(table)) {
        throw new IOException("Table " + table + " not exist");
      } else {
        existingTables.add(table);
      }
    }
    MobFileStore store = MobFileStoreManager.current().getMobFileStore(table, family);
    SweepJob job = new SweepJob(fs);
    job.sweep(store, getConf());
    close();
  }

  private void close() {
    if (admin != null) {
      try {
        admin.close();
      } catch (IOException e) {
        System.out.println("Fail to close the HBaseAdmin: " + e.getMessage());
      }
    }
  }

  public static void main(String[] args) throws Exception {

    System.out.print("Usage:\n" + "--------------------------\n" + Sweeper.class.getName()
        + "[tableName] [familyName]");

    Configuration conf = getDefaultConfiguration(null);
    ToolRunner.run(conf, new Sweeper(), args);
  }

  private static Configuration getDefaultConfiguration(Configuration conf) {
    return conf == null ? HBaseConfiguration.create() : HBaseConfiguration.create(conf);
  }

  public int run(String[] args) throws Exception {
    init();
    if (args.length >= 2) {
      String table = args[0];
      String family = args[1];
      sweepFamily(table, family);
    }
    return 0;
  }
}