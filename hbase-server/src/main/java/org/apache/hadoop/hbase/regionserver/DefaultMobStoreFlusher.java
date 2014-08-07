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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * An implementation of the StoreFlusher. It extends the DefaultStoreFlusher.
 * If the store is not a mob store, the flusher flushes the MemStore the same with
 * DefaultStoreFlusher, 
 * If the store is a mob store, the flusher flushes the MemStore into two places.
 * One is the store files of HBase, the other is the mob files.
 * <ol>
 * <li>Cells that are not PUT type or have the delete mark will be directly flushed to HBase.</li>
 * <li>If the size of a cell value is larger than or equal with a threshold, it'll be flushed
 * to a mob file, another cell with the path of this file will be flushed to HBase.</li>
 * <li>If the size of a cell value is smaller than a threshold, it'll be flushed to HBase
 * directly.</li>
 * </ol>
 * 
 */
public class DefaultMobStoreFlusher extends DefaultStoreFlusher {

  private static final Log LOG = LogFactory.getLog(DefaultMobStoreFlusher.class);
  private final Object flushLock = new Object();
  private boolean isMob = false;
  private long mobCellValueSizeThreshold = 0;
  private Path targetPath;
  private MobFileStore mobFileStore;
  private Object lock = new Object();

  public DefaultMobStoreFlusher(Configuration conf, Store store) {
    super(conf, store);
    isMob = MobUtils.isMobFamily(store.getFamily());
    mobCellValueSizeThreshold = MobUtils.getMobSizeThreshold(store.getFamily());
    this.targetPath = new Path(MobUtils.getMobRegionPath(conf, store.getTableName()),
        store.getColumnFamilyName());
  }

  /**
   * Flushes the snapshot of the MemStore. 
   * If this store is not a mob store, flush the cells in the snapshot to store files of HBase. 
   * If the store is a mob one, the flusher flushes the MemStore into two places.
   * One is the store files of HBase, the other is the mob files.
   * <ol>
   * <li>Cells that are not PUT type or have the delete mark will be directly flushed to HBase.</li>
   * <li>If the size of a cell value is larger than or equal with a threshold, it'll be
   * flushed to a mob file,
   * another cell with the path of this file will be flushed to HBase.</li>
   * <li>If the size of a cell value is smaller than a threshold, it'll be flushed to HBase
   * directly.</li>
   * </ol>
   */
  @Override
  public List<Path> flushSnapshot(MemStoreSnapshot snapshot, long cacheFlushId,
      MonitoredTask status) throws IOException {
    ArrayList<Path> result = new ArrayList<Path>();
    int cellsCount = snapshot.getCellsCount();
    if (cellsCount == 0) return result; // don't flush if there are no entries

    // Use a store scanner to find which rows to flush.
    long smallestReadPoint = store.getSmallestReadPoint();
    InternalScanner scanner = createScanner(snapshot.getScanner(), smallestReadPoint);
    if (scanner == null) {
      return result; // NULL scanner returned from coprocessor hooks means skip normal processing
    }
    StoreFile.Writer writer;
    try {
      // TODO: We can fail in the below block before we complete adding this flush to
      // list of store files. Add cleanup of anything put on filesystem if we fail.
      synchronized (flushLock) {
        status.setStatus("Flushing " + store + ": creating writer");
        // Write the map out to the disk
        writer = store.createWriterInTmp(cellsCount, store.getFamily().getCompression(),
            false, true, true);
        writer.setTimeRangeTracker(snapshot.getTimeRangeTracker());
        try {
          if (!isMob) {
            // It's not a mob store, flush the cells in a normal way
            performFlush(scanner, writer, smallestReadPoint);
          } else {
            mobFileStore = currentMobFileStore();
            StoreFile.Writer mobFileWriter = null;
            int compactionKVMax = conf.getInt(HConstants.COMPACTION_KV_MAX,
                HConstants.COMPACTION_KV_MAX_DEFAULT);
            long time = snapshot.getTimeRangeTracker().getMaximumTimestamp();
            mobFileWriter = mobFileStore.createWriterInTmp(new Date(time), cellsCount, store
                .getFamily().getCompression(), store.getRegionInfo().getStartKey());
            // the target path is {tableName}/.mob/{cfName}/mobFiles
            // the relative path is mobFiles
            String relativePath = mobFileWriter.getPath().getName();
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
                    if (kv.getValueLength() <= mobCellValueSizeThreshold
                        || MobUtils.isMobReferenceCell(kv)
                        || kv.getTypeByte() != KeyValue.Type.Put.getCode()) {
                      writer.append(kv);
                    } else {
                      // append the original keyValue in the mob file.
                      mobFileWriter.append(kv);

                      // append the tags to the KeyValue.
                      // The key is same, the value is the filename of the mob file
                      List<Tag> existingTags = Tag.asList(kv.getTagsArray(), kv.getTagsOffset(),
                          kv.getTagsLength());
                      if (existingTags.isEmpty()) {
                        existingTags = new ArrayList<Tag>();
                      }
                      Tag mobRefTag = new Tag(TagType.MOB_REFERENCE_TAG_TYPE,
                          HConstants.EMPTY_BYTE_ARRAY);
                      existingTags.add(mobRefTag);
                      long valueLength = kv.getValueLength();
                      byte[] newValue = Bytes.add(Bytes.toBytes(valueLength), referenceValue);
                      KeyValue reference = new KeyValue(kv.getRowArray(), kv.getRowOffset(),
                          kv.getRowLength(), kv.getFamilyArray(), kv.getFamilyOffset(),
                          kv.getFamilyLength(), kv.getQualifierArray(), kv.getQualifierOffset(),
                          kv.getQualifierLength(), kv.getTimestamp(), KeyValue.Type.Put,
                          newValue, 0, newValue.length, existingTags);
                      reference.setSequenceId(kv.getSequenceId());
                      writer.append(reference);
                    }
                  }
                  kvs.clear();
                }
              } while (hasMore);
            } finally {
              status.setStatus("Flushing mob file " + store + ": appending metadata");
              mobFileWriter.appendMetadata(cacheFlushId, false);
              status.setStatus("Flushing mob file " + store + ": closing flushed file");
              mobFileWriter.close();
            }

            // commit the mob file from temp folder to target folder.
            mobFileStore.commitFile(mobFileWriter.getPath(), targetPath);
          }
        } finally {
          finalizeWriter(writer, cacheFlushId, status);
        }
      }
    } finally {
      scanner.close();
    }
    LOG.info("Flushed, sequenceid=" + cacheFlushId + ", memsize="
        + snapshot.getSize() + ", hasBloomFilter=" + writer.hasGeneralBloom()
        + ", into tmp file " + writer.getPath());
    result.add(writer.getPath());
    return result;
  }

  /**
   * Gets the current MobFileStore.
   * 
   * @return The current MobFileStore.
   * @throws IOException
   */
  private MobFileStore currentMobFileStore() throws IOException {
    if (null == mobFileStore) {
      synchronized (lock) {
        if (null == mobFileStore) {
          if (!this.store.getFileSystem().exists(targetPath)) {
            this.store.getFileSystem().mkdirs(targetPath);
          }
          mobFileStore = MobFileStore.create(conf, this.store.getFileSystem(),
              this.store.getTableName(), this.store.getFamily());
        }
      }
    }
    return mobFileStore;
  }
}
