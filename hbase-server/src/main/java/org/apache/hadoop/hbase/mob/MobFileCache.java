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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.IdLock;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class MobFileCache {

  private static final Log LOG = LogFactory.getLog(MobFileCache.class);

  /*
   * Statistics thread. Periodically prints the cache statistics to the log.
   */
  static class EvictionThread extends Thread {
    MobFileCache lru;

    public EvictionThread(MobFileCache lru) {
      super("LobFileCache.StatisticsThread");
      setDaemon(true);
      this.lru = lru;
    }

    @Override
    public void run() {
      lru.evict();
    }
  }

  private Map<String, CachedMobFile> map = null;
  /** Cache access count (sequential ID) */
  private final AtomicLong count;
  private final AtomicLong miss;

  private final ReentrantLock evictionLock = new ReentrantLock(true);

  private IdLock keyLock = new IdLock();

  /** Statistics thread schedule pool (for heavy debugging, could remove) */
  private final ScheduledExecutorService scheduleThreadPool = Executors.newScheduledThreadPool(1,
      new ThreadFactoryBuilder().setNameFormat("LobFileCache #%d").setDaemon(true).build());
  private Configuration conf;

  /** Eviction thread */
  private static final int EVICTION_CHECK_PERIOD = 3600; // in seconds

  private int lobFileCacheSize;
  private boolean isCacheEnabled = false;

  public MobFileCache(Configuration conf) {
    this.conf = conf;
    this.lobFileCacheSize = Integer.parseInt(conf.get(MobConstants.MOB_FILE_CACHE_SIZE_KEY,
        String.valueOf(MobConstants.DEFAULT_MOB_FILE_CACHE_SIZE)));
    isCacheEnabled = (lobFileCacheSize > 0);
    map = new ConcurrentHashMap<String, CachedMobFile>(lobFileCacheSize);
    this.count = new AtomicLong(0);
    this.miss = new AtomicLong(0);
    if (isCacheEnabled) {
      this.scheduleThreadPool.scheduleAtFixedRate(new EvictionThread(this), EVICTION_CHECK_PERIOD,
          EVICTION_CHECK_PERIOD, TimeUnit.SECONDS);
    }
    LOG.info("LobFileCache is initialized, and the cache size is " + lobFileCacheSize);
  }

  public void evict() {
    if (isCacheEnabled) {
      evict(false);
    }
  }

  public void evict(boolean evictAll) {
    if (isCacheEnabled) {
      // Ensure only one eviction at a time
      printStatistics();
      if (!evictionLock.tryLock()) {
        return;
      }

      try {
        // Clear all the blob file cache
        if (evictAll) {
          for (String fileName : map.keySet()) {
            CachedMobFile file = map.remove(fileName);
            if (null != file) {
              try {
                file.close();
              } catch (IOException e) {
                LOG.error(e.getMessage(), e);
              }
            }
          }
        } else {
          if (map.size() <= lobFileCacheSize) {
            return;
          }
          List<CachedMobFile> files = new ArrayList<CachedMobFile>(map.size());
          for (CachedMobFile file : map.values()) {
            files.add(file);
          }

          Collections.sort(files);

          for (int i = lobFileCacheSize; i < files.size(); i++) {
            String name = files.get(i).getName();
            CachedMobFile file = map.remove(name);
            if (null != file) {
              try {
                file.close();
              } catch (IOException e) {
                LOG.error(e.getMessage(), e);
              }
            }
          }
        }
      } finally {
        evictionLock.unlock();
      }
    }
  }

  public MobFile open(FileSystem fs, Path path, MobCacheConfig cacheConf) throws IOException {
    if (!isCacheEnabled) {
      return MobFile.create(fs, path, conf, cacheConf);
    } else {
      String fileName = path.getName();
      CachedMobFile cached = map.get(fileName);
      if (null == cached) {
        IdLock.Entry lockEntry = keyLock.getLockEntry(fileName.hashCode());
        try {
          cached = map.get(fileName);
          if (null == cached) {
            if (map.size() > lobFileCacheSize) {
              evict(false);
            }
            cached = CachedMobFile.create(fs, path, conf, cacheConf);
            // open an extra time to keep a reference count in the map
            cached.open();
            map.put(fileName, cached);
          }
        } catch (IOException e) {
          LOG.error("BlobFileCache, Exception happen during open " + path.toString(), e);
          return null;
        } finally {
          miss.incrementAndGet();
          keyLock.releaseLockEntry(lockEntry);
        }
      }
      cached.open();
      cached.access(count.incrementAndGet());
      return cached;
    }
  }

  public int getCacheSize() {
    if (map != null) {
      return map.size();
    }
    return 0;
  }

  public void printStatistics() {
    long access = count.get();
    long missed = miss.get();
    LOG.info("BlobFileCache Statistics, access: " + access + ", miss: " + missed + ", hit: "
        + (access - missed) + ", hit rate: "
        + ((access == 0) ? 0 : ((access - missed) * 100 / access)) + "%");

  }

}
