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
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

/**
 * This is the StoreFlusher used by the MOB store.
 * 
 */
public class MobStoreFlusher extends StoreFlusher {

  private static final Log LOG = LogFactory.getLog(MobStoreFlusher.class);
  private final Object flushLock = new Object();
  private boolean isMob = false;
  private int mobCellSizeThreshold = 0;

  public MobStoreFlusher(Configuration conf, Store store) {
    super(conf, store);
    isMob = MobUtils.isMobFamily(store.getFamily());
    mobCellSizeThreshold = conf.getInt(MobConstants.MOB_CELL_SIZE_THRESHOLD,
        MobConstants.DEFAULT_MOB_CELL_SIZE_THRESHOLD);
  }

  @Override
  public List<Path> flushSnapshot(SortedSet<KeyValue> snapshot, long cacheFlushId,
      TimeRangeTracker snapshotTimeRangeTracker, AtomicLong flushedSize, MonitoredTask status)
      throws IOException {
    ArrayList<Path> result = new ArrayList<Path>();
    if (snapshot.size() == 0) {
      return result; // don't flush if there are no entries
    }

    // Use a store scanner to find which rows to flush.
    long smallestReadPoint = store.getSmallestReadPoint();
    InternalScanner scanner = createScanner(snapshot, smallestReadPoint);
    if (scanner == null) {
      return result; // NULL scanner returned from coprocessor hooks means skip normal processing
    }
    if (!isMob) {
      StoreFile.Writer writer;
      long flushed = 0;
      try {
        // TODO: We can fail in the below block before we complete adding this flush to
        // list of store files. Add cleanup of anything put on filesystem if we fail.
        synchronized (flushLock) {
          status.setStatus("Flushing " + store + ": creating writer");
          // Write the map out to the disk
          writer = store.createWriterInTmp(snapshot.size(), store.getFamily().getCompression(),
              false, true, true);
          writer.setTimeRangeTracker(snapshotTimeRangeTracker);
          try {
            flushed = performFlush(scanner, writer, smallestReadPoint);
          } finally {
            finalizeWriter(writer, cacheFlushId, status);
          }
        }
      } finally {
        flushedSize.set(flushed);
        scanner.close();
      }
      LOG.info("Flushed, sequenceid=" + cacheFlushId + ", memsize="
          + StringUtils.humanReadableInt(flushed) + ", hasBloomFilter=" + writer.hasGeneralBloom()
          + ", into tmp file " + writer.getPath());
      result.add(writer.getPath());
      return result;
    } else {
      StoreFile.Writer writer;
      MobFileStore mobFileStore = MobFileStoreManager.current().getMobFileStore(
          store.getTableName().getNameAsString(), store.getFamily().getNameAsString());
      if (null == mobFileStore) {
        // create it and cache it
        mobFileStore = MobFileStoreManager.current().createMobFileStore(
            store.getTableName().getNameAsString(), store.getFamily());
      }

      StoreFile.Writer mobFileWriter = null;
      long flushed = 0;
      try {
        synchronized (flushLock) {
          int compactionKVMax = conf.getInt(HConstants.COMPACTION_KV_MAX,
              HConstants.COMPACTION_KV_MAX_DEFAULT);
          status.setStatus("Flushing " + store + ": creating writer");
          // A. Write the map out to the disk
          writer = store.createWriterInTmp(snapshot.size(), store.getFamily().getCompression(),
              false, true, true);
          writer.setTimeRangeTracker(snapshotTimeRangeTracker);

          Iterator<KeyValue> iter = snapshot.iterator();
          int mobKVCount = 0;
          while (iter != null && iter.hasNext()) {
            KeyValue kv = iter.next();
            if (kv.getValueLength() >= mobCellSizeThreshold && !MobUtils.isMobReferenceKeyValue(kv)
                && kv.getTypeByte() == KeyValue.Type.Put.getCode()) {
              mobKVCount++;
            }
          }

          mobFileWriter = mobFileStore.createWriterInTmp(mobKVCount, store.getFamily()
              .getCompression(), store.getRegionInfo().getStartKey());
          // the path is {tableName}/{cfName}/{date}/mobFiles
          // the relative path is {date}/mobFiles
          String dateDir = MobUtils.formatDate(new Date());
          Path targetPath = new Path(mobFileStore.getHomePath(), dateDir);

          String relativePath = dateDir + Path.SEPARATOR + mobFileWriter.getPath().getName();

          byte[] referenceValue = Bytes.toBytes(relativePath);

          try {
            List<Cell> kvs = new ArrayList<Cell>();
            boolean hasMore;
            do {
              hasMore = scanner.next(kvs, compactionKVMax);
              if (!kvs.isEmpty()) {
                for (Cell c : kvs) {
                  // If we know that this KV is going to be included always, then let us
                  // set its memstoreTS to 0. This will help us save space when writing to
                  // disk.
                  KeyValue kv = KeyValueUtil.ensureKeyValue(c);
                  if (kv.getMvccVersion() <= smallestReadPoint) {
                    // let us not change the original KV. It could be in the memstore
                    // changing its memstoreTS could affect other threads/scanners.
                    kv = kv.shallowCopy();
                    kv.setMvccVersion(0);
                  }

                  if (kv.getValueLength() < mobCellSizeThreshold
                      || MobUtils.isMobReferenceKeyValue(kv)
                      || kv.getTypeByte() != KeyValue.Type.Put.getCode()) {
                    writer.append(kv);
                  } else {
                    // append the original keyValue in the mob file.
                    mobFileWriter.append(kv);

                    // append the tag to the KeyValue.
                    // The key is same, the value is the filename of the mob file
                    List<Tag> existingTags = kv.getTags();
                    if (existingTags.isEmpty()) {
                      existingTags = new ArrayList<Tag>();
                    }
                    Tag mobRefTag = new Tag(MobConstants.MOB_REFERENCE_TAG_TYPE,
                        MobConstants.MOB_REFERENCE_TAG_VALUE);
                    existingTags.add(mobRefTag);
                    KeyValue reference = new KeyValue(kv.getRowArray(), kv.getRowOffset(),
                        kv.getRowLength(), kv.getFamilyArray(), kv.getFamilyOffset(),
                        kv.getFamilyLength(), kv.getQualifierArray(), kv.getQualifierOffset(),
                        kv.getQualifierLength(), kv.getTimestamp(), KeyValue.Type.Put,
                        referenceValue, 0, referenceValue.length, existingTags);
                    writer.append(reference);
                  }

                  flushed += MemStore.heapSizeChange(kv, true);
                }
                kvs.clear();
              }
            } while (hasMore);
          } finally {
            // Write out the log sequence number that corresponds to this output
            // hfile. Also write current time in metadata as minFlushTime.
            // The hfile is current up to and including cacheFlushSeqNum.
            status.setStatus("Flushing " + store + ": appending metadata");
            writer.appendMetadata(cacheFlushId, false);
            mobFileWriter.appendMetadata(cacheFlushId, false);
            status.setStatus("Flushing " + store + ": closing flushed file");
            writer.close();
            mobFileWriter.close();

            // commit the mob file from temp folder to target folder.
            mobFileStore.commitFile(mobFileWriter.getPath(), targetPath);
          }
        }
      } finally {
        flushedSize.set(flushed);
        scanner.close();
      }
      LOG.info("Flushed, sequenceid=" + cacheFlushId + ", memsize="
          + StringUtils.humanReadableInt(flushed) + ", hasBloomFilter=" + writer.hasGeneralBloom()
          + ", into tmp file " + writer.getPath());
      result.add(writer.getPath());
      return result;
    }
  }

}
