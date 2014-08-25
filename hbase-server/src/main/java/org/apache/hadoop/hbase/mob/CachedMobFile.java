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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;

/**
 * Cached mob file.
 */
@InterfaceAudience.Private
public class CachedMobFile extends MobFile implements Comparable<CachedMobFile> {

  private long accessCount;
  private MobFile file;
  private AtomicLong referenceCount = new AtomicLong(0);

  public CachedMobFile(MobFile file) {
    this.file = file;
  }

  public static CachedMobFile create(FileSystem fs, Path path, Configuration conf,
      MobCacheConfig cacheConf) throws IOException {
    MobFile file = MobFile.create(fs, path, conf, cacheConf);
    return new CachedMobFile(file);
  }

  public void access(long accessCount) {
    this.accessCount = accessCount;
  }

  public int compareTo(CachedMobFile that) {
    if (this.accessCount == that.accessCount)
      return 0;
    return this.accessCount < that.accessCount ? 1 : -1;
  }

  /**
   * Opens the mob file if it's not opened yet and increases the reference.
   * It's not thread-safe. Use MobFileCache.openFile() instead.
   * The reader of the mob file is just opened when it's not opened no matter how many times
   * this open() method is invoked.
   * The reference is a counter that how many times this reader is referenced. When the
   * reference is 0, this reader is closed.
   */
  @Override
  public void open() throws IOException {
    file.open();
    referenceCount.incrementAndGet();
  }

  /**
   * Decreases the reference of the underlying reader for the mob file.
   * It's not thread-safe. Use MobFileCache.closeFile() instead.
   * This underlying reader isn't closed until the reference is 0.
   */
  @Override
  public void close() throws IOException {
    long refs = referenceCount.decrementAndGet();
    if (refs == 0) {
      this.file.close();
    }
  }

  /**
   * Reads a cell from the mob file.
   * @param search The cell need to be searched in the mob file.
   * @param cacheMobBlocks Whether should this scanner cache blocks.
   * @return The cell in the mob file.
   * @throws IOException
   */
  @Override
  public Cell readCell(Cell search, boolean cacheMobBlocks) throws IOException {
    return file.readCell(search, cacheMobBlocks);
  }

  /**
   * Gets the file name.
   * @return The file name.
   */
  @Override
  public String getFileName() {
    return file.getFileName();
  }

  /**
   * Internal use only. This is used by the sweeper.
   *
   * @return The store file scanner.
   * @throws IOException
   */
  @Override
  public StoreFileScanner getScanner() throws IOException {
    return file.getScanner();
  }

  /**
   * Gets the reference of the current mob file.
   * Internal usage, currently it's for testing.
   * @return The reference of the current mob file.
   */
  public long getReferenceCount() {
    return this.referenceCount.longValue();
  }
}