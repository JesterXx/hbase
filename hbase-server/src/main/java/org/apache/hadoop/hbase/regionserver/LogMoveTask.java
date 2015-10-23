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
package org.apache.hadoop.hbase.regionserver;

import java.util.Collection;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class LogMoveTask implements Callable<Void> {

  static final Log LOG = LogFactory.getLog(LogMoveTask.class);
  private FileSystem fs;
  private Collection<Path> files;
  private String storagePolicy;

  public LogMoveTask(FileSystem fs, Collection<Path> files, String storagePolicy) {
    this.fs = fs;
    this.files = files;
    this.storagePolicy = storagePolicy;
  }

  @Override
  public Void call() throws Exception {
    LOG.info("start to move-log: " + files.size() + " files to " + storagePolicy);
    long start = EnvironmentEdgeManager.currentTime();
    DFSClient client = ((DistributedFileSystem) fs).getClient();
    try {
      for (Path file : files) {
        String path = Path.getPathWithoutSchemeAndAuthority(file).toString();
//        String path = file.toString();
        client.setStoragePolicy(path, storagePolicy);
        client.applyFilePolicy(path.toString());
      }  
    } catch(Exception e) {
      LOG.error("Failed to move logg",e);
      throw e;
    }
    long duration = EnvironmentEdgeManager.currentTime() - start;
    LOG.info("log move took " + duration);
    return null;
  }
}
