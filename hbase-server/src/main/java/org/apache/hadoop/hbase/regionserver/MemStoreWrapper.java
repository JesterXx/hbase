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
import java.util.List;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.compactions.SweepCounter;
import org.apache.hadoop.hbase.mob.compactions.SweepReducer.SweepPartitionId;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionBackedScanner;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MemStoreWrapper {

  private static final Log LOG = LogFactory.getLog(MemStoreWrapper.class);

  private MemStore memstore;
  private long blockingMemStoreSize;
  private SweepPartitionId partitionId;
  private Context context;
  private Configuration conf;
  private MobFileStore mobFileStore;
  private HTableInterface table;

  public MemStoreWrapper(Context context, HTableInterface table, MemStore memstore,
      MobFileStore mobFileStore) {
    this.memstore = memstore;
    this.context = context;
    this.mobFileStore = mobFileStore;
    this.table = table;
    this.conf = context.getConfiguration();
    long flushSize = this.conf.getLong(MobConstants.MOB_COMPACTION_MEMSTORE_FLUSH_SIZE,
        MobConstants.DEFAULT_MOB_COMPACTION_MEMSTORE_FLUSH_SIZE);
    this.blockingMemStoreSize = flushSize
        * this.conf.getLong(MobConstants.MOB_COMPACTION_MEMSTORE_BLOCK_MULTIPLIER, 2);
  }

  public void setPartitionId(SweepPartitionId partitionId) {
    this.partitionId = partitionId;
  }

  public void flushMemStoreIfNecessary() throws IOException {
    if (memstore.heapSize() >= blockingMemStoreSize) {
      flushMemStore();
    }
  }

  public void flushMemStore() throws IOException {
    memstore.snapshot();
    SortedSet<KeyValue> snapshot = memstore.getSnapshot();
    internalFlushCache(snapshot, Long.MAX_VALUE);
    memstore.clearSnapshot(snapshot);
  }

  private void internalFlushCache(final SortedSet<KeyValue> set, final long logCacheFlushId)
      throws IOException {
    if (set.size() == 0) {
      return;
    }
    // generate the temp files into a fixed path.
    String tempPathString = context.getConfiguration().get(
        MobConstants.MOB_COMPACTION_JOB_WORKING_DIR);
    StoreFile.Writer mobFileWriter = mobFileStore.createWriterInTmp(new Path(tempPathString),
        set.size(), mobFileStore.getColumnDescriptor().getCompactionCompression(),
        partitionId.getStartKey());

    Path targetPath = new Path(mobFileStore.getHomePath(), partitionId.getDate());

    String relativePath = partitionId.getDate() + Path.SEPARATOR
        + mobFileWriter.getPath().getName();
    LOG.info("Create temp files under " + mobFileWriter.getPath().toString());

    byte[] referenceValue = Bytes.toBytes(relativePath);
    int keyValueCount = 0;
    KeyValueScanner scanner = new CollectionBackedScanner(set, KeyValue.COMPARATOR);
    scanner.seek(KeyValue.createFirstOnRow(new byte[] {}));
    KeyValue kv = null;
    while (null != (kv = scanner.next())) {
      kv.setMvccVersion(0);
      mobFileWriter.append(kv);
      keyValueCount++;
    }
    scanner.close();
    // Write out the log sequence number that corresponds to this output
    // hfile. The hfile is current up to and including logCacheFlushId.
    mobFileWriter.appendMetadata(logCacheFlushId, false);
    mobFileWriter.close();

    mobFileStore.commitFile(mobFileWriter.getPath(), targetPath);
    context.getCounter(SweepCounter.FILE_AFTER_MERGE_OR_CLEAN).increment(1);
    // write reference
    scanner = new CollectionBackedScanner(set, KeyValue.COMPARATOR);
    scanner.seek(KeyValue.createFirstOnRow(new byte[] {}));
    kv = null;
    while (null != (kv = scanner.next())) {
      kv.setMvccVersion(0);
      List<Tag> existingTags = kv.getTags();
      if (existingTags.isEmpty()) {
        existingTags = new ArrayList<Tag>();
      }
      Tag mobRefTag = new Tag(MobConstants.MOB_REFERENCE_TAG_TYPE,
          MobConstants.MOB_REFERENCE_TAG_VALUE);
      existingTags.add(mobRefTag);

      KeyValue reference = new KeyValue(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(),
          kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(), kv.getBuffer(),
          kv.getQualifierOffset(), kv.getQualifierLength(), kv.getTimestamp(), KeyValue.Type.Put,
          referenceValue, 0, referenceValue.length, existingTags);

      Put put = new Put(reference.getRow());
      put.add(reference);
      table.put(put);
      context.getCounter(SweepCounter.RECORDS_UPDATED).increment(1);
    }
    if (keyValueCount > 0) {
      table.flushCommits();
    }
    scanner.close();
  }

  public void addToMemstore(KeyValue kv) {
    memstore.add(kv);
  }

}
