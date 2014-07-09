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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;

public class CachedMobFile extends MobFile implements Comparable<CachedMobFile> {

  private long accessTime;
  private MobFile file;
  private AtomicLong reference = new AtomicLong(0);

  public CachedMobFile(MobFile file) {
    super(null);
    this.file = file;
  }

  public void access(long time) {
    this.accessTime = time;
  }

  public int compareTo(CachedMobFile that) {
    if (this.accessTime == that.accessTime)
      return 0;
    return this.accessTime < that.accessTime ? 1 : -1;
  }

  @Override
  public synchronized void open() throws IOException {
    file.open();
    reference.incrementAndGet();
  }

  @Override
  public KeyValue readKeyValue(KeyValue search, boolean cacheMobBlocks) throws IOException {
    return file.readKeyValue(search, cacheMobBlocks);
  }

  @Override
  public String getName() {
    return file.getName();
  }

  @Override
  public synchronized void close() throws IOException {
    long refs = reference.decrementAndGet();
    if (refs == 0) {
      this.file.close();
    }
  }
  
  public long getReference() {
    return this.reference.get();
  }

  public static CachedMobFile create(FileSystem fs, Path path,
      Configuration conf, MobCacheConfig cacheConf) throws IOException {
    MobFile file = MobFile.create(fs, path, conf, cacheConf);
    return new CachedMobFile(file);
  }
}