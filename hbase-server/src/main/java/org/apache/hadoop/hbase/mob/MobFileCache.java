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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.IdLock;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The cache for mob files.
 * This cache doesn't cache the mob file blocks. It only caches the references of mob files.
 */
@InterfaceAudience.Private
public class MobFileCache {

  private static final Log LOG = LogFactory.getLog(MobFileCache.class);

  /*
   * Eviction and statistics thread. Periodically run to print the statistics and
   * evict the lru cached mob files when the count of the cached files is larger
   * than the threshold.
   */
  static class EvictionThread extends Thread {
    MobFileCache lru;

    public EvictionThread(MobFileCache lru) {
      super("MobFileCache.EvictionThread");
      setDaemon(true);
      this.lru = lru;
    }

    @Override
    public void run() {
      lru.evict();
    }
  }

  private Map<String, CachedMobFile> map = null;
  /* Cache access count (sequential ID) */
  private final AtomicLong count;
  private final AtomicLong miss;

  private final ReentrantLock evictionLock = new ReentrantLock(true);

  private IdLock keyLock = new IdLock();

  private final ScheduledExecutorService scheduleThreadPool = Executors.newScheduledThreadPool(1,
      new ThreadFactoryBuilder().setNameFormat("MobFileCache #%d").setDaemon(true).build());
  private Configuration conf;

  private static final float DEFAULT_EVICT_REMAIN_RATIO = 0.5f;

  // the count of the cached references to mob files
  private int mobFileCacheSize;
  private boolean isCacheEnabled = false;
  private float evictRemainRatio;

  public MobFileCache(Configuration conf) {
    this.conf = conf;
    this.mobFileCacheSize = conf.getInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY,
        MobConstants.DEFAULT_MOB_FILE_CACHE_SIZE);
    isCacheEnabled = (mobFileCacheSize > 0);
    map = new ConcurrentHashMap<String, CachedMobFile>(mobFileCacheSize);
    this.count = new AtomicLong(0);
    this.miss = new AtomicLong(0);
    if (isCacheEnabled) {
      long period = conf.getInt(MobConstants.MOB_CACHE_EVICT_PERIOD, 3600); // in seconds
      evictRemainRatio = conf.getFloat(MobConstants.MOB_CACHE_EVICT_REMAIN_RATIO,
          DEFAULT_EVICT_REMAIN_RATIO);
      this.scheduleThreadPool.scheduleAtFixedRate(new EvictionThread(this), period, period,
          TimeUnit.SECONDS);
    }
    LOG.info("MobFileCache is initialized, and the cache size is " + mobFileCacheSize);
  }

  /**
   * Evicts the lru cached mob files when the count of the cached files is larger
   * than the threshold.
   */
  public void evict() {
    if (isCacheEnabled) {
      // Ensure only one eviction at a time
      printStatistics();
      if (!evictionLock.tryLock()) {
        return;
      }
      try {
        if (map.size() <= mobFileCacheSize) {
          return;
        }
        List<CachedMobFile> files = new ArrayList<CachedMobFile>(map.size());
        for (CachedMobFile file : map.values()) {
          files.add(file);
        }
        Collections.sort(files);
        int start = (int) (mobFileCacheSize * evictRemainRatio);
        for (int i = start; i < files.size(); i++) {
          String name = files.get(i).getName();
          CachedMobFile deletedFile = map.remove(name);
          if (null != deletedFile) {
            try {
              deletedFile.close();
            } catch (IOException e) {
              LOG.error(e.getMessage(), e);
            }
          }
        }
      } finally {
        evictionLock.unlock();
      }
    }
  }

  /**
   * Evicts the cached file by the name.
   * @param fileName The name of a cached file.
   */
  public void evictFile(String fileName) {
    if (isCacheEnabled) {
      IdLock.Entry lockEntry = null;
      try {
        lockEntry = keyLock.getLockEntry(fileName.hashCode());
        CachedMobFile deletedFile = map.remove(fileName);
        if (null != deletedFile) {
          deletedFile.close();
        }
      } catch (IOException e) {
        LOG.error("Fail to evict the file " + fileName, e);
      } finally {
        if (lockEntry != null) {
          keyLock.releaseLockEntry(lockEntry);
        }
      }
    }
  }

  /**
   * Opens a mob file.
   * @param fs The current file system.
   * @param path The file path.
   * @param cacheConf The current MobCacheConfig
   * @return A opened mob file.
   * @throws IOException
   */
  public MobFile openFile(FileSystem fs, Path path, MobCacheConfig cacheConf) throws IOException {
    if (!isCacheEnabled) {
      return MobFile.create(fs, path, conf, cacheConf);
    } else {
      String fileName = path.getName();
      CachedMobFile cached = map.get(fileName);
      IdLock.Entry lockEntry = keyLock.getLockEntry(fileName.hashCode());
      try {
        if (null == cached) {
          cached = map.get(fileName);
          if (null == cached) {
            if (map.size() > mobFileCacheSize) {
              evict();
            }
            cached = CachedMobFile.create(fs, path, conf, cacheConf);
            cached.open();
            map.put(fileName, cached);
          }
          miss.incrementAndGet();
        }
        cached.open();
        cached.access(count.incrementAndGet());
      } finally {
        keyLock.releaseLockEntry(lockEntry);
      }
      return cached;
    }
  }

  /**
   * Closes a mob file.
   * @param file The mob file that needs to be closed.
   */
  public void closeFile(MobFile file) {
    IdLock.Entry lockEntry = null;
    try {
      lockEntry = keyLock.getLockEntry(file.getName().hashCode());
      file.close();
    } catch (IOException e) {
      LOG.error("MobFileCache, Exception happen during close " + file.getName(), e);
    } finally {
      if (lockEntry != null) {
        keyLock.releaseLockEntry(lockEntry);
      }
    }
  }

  /**
   * Gets the count of cached mob files.
   * @return The count of the cached mob files.
   */
  public int getCacheSize() {
    return map == null ? 0 : map.size();
  }

  /**
   * Prints the statistics.
   */
  public void printStatistics() {
    long access = count.get();
    long missed = miss.get();
    LOG.info("MobFileCache Statistics, access: " + access + ", miss: " + missed + ", hit: "
        + (access - missed) + ", hit rate: "
        + ((access == 0) ? 0 : ((access - missed) * 100 / access)) + "%");

  }

}
