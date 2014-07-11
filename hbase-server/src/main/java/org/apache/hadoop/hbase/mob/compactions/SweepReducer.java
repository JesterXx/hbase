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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobFile;
import org.apache.hadoop.hbase.mob.MobFilePath;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.DefaultMemStore;
import org.apache.hadoop.hbase.regionserver.MemStoreWrapper;
import org.apache.hadoop.hbase.regionserver.MobFileStore;
import org.apache.hadoop.hbase.regionserver.MobFileStoreManager;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class SweepReducer extends Reducer<Text, KeyValue, Writable, Writable> {

  private static final Log LOG = LogFactory.getLog(SweepReducer.class);

  private MemStoreWrapper memstore;
  private Configuration conf;
  private FileSystem fs;

  private String tableName;
  private String family;
  private Path familyDir;
  private MobFileStore mobFileStore;
  private CacheConfig cacheConf;
  private HTableInterface table;
  private String compactionBeginString;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    this.conf = context.getConfiguration();
    this.fs = FileSystem.get(conf);
    this.tableName = conf.get(TableInputFormat.INPUT_TABLE);
    this.family = conf.get(TableInputFormat.SCAN_COLUMN_FAMILY);
    this.familyDir = new Path(MobUtils.getMobHome(conf), tableName + Path.SEPARATOR + family);

    MobFileStoreManager.current().init(conf, fs);
    this.mobFileStore = MobFileStoreManager.current().getMobFileStore(tableName, family);
    this.cacheConf = new CacheConfig(conf, mobFileStore.getColumnDescriptor());

    String htableFactoryClassName = conf.get(
        MobConstants.MOB_COMPACTION_OUTPUT_TABLE_FACTORY, HTableFactory.class.getName());
    HTableInterfaceFactory htableFactory = null;
    try {
      htableFactory = (HTableInterfaceFactory) Class.forName(htableFactoryClassName).newInstance();
    } catch (Exception e) {
      throw new IOException("Fail to instantiate the HTableInterfaceFactory", e);
    }
    this.table = htableFactory.createHTableInterface(this.conf, Bytes.toBytes(tableName));
    this.table.setAutoFlush(false, false);

    this.table.setWriteBufferSize(1 * 1024 * 1024); // 1MB
    memstore = new MemStoreWrapper(context, table, new DefaultMemStore(), mobFileStore);

    long compactionBegin = Long.parseLong(conf.get(MobConstants.MOB_COMPACTION_START_DATE, "0"));
    this.compactionBeginString = MobUtils.formatDate(new Date(compactionBegin));
  }

  private SweepPartition createPartition(SweepPartitionId id, Context context) throws IOException {

    String partitionDate = id.getDate();

    // Skip compacting the data which is generated after the compaction begin
    // Or the data is generated same day of compaction begin
    if (partitionDate.compareTo(compactionBeginString) >= 0) {
      return null;
    } else {
      return new SweepPartition(id, context);
    }
  }

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    try {
      SweepPartitionId id = null;
      SweepPartition partition = null;
      while (context.nextKey()) {
        Text key = context.getCurrentKey();
        id = SweepPartitionId.create(key.toString());
        if (null == partition || !id.equals(partition.getId())) {
          if (null != partition) {
            partition.close();
          }

          partition = createPartition(id, context);
        }
        if (partition != null) {
          partition.execute(key, context.getValues());
        }
      }
      if (null != partition) {
        partition.close();
      }
      context.write(new Text("Reducer task is done"), new Text(""));
    } finally {
      cleanup(context);
    }

  }

  public class SweepPartition {

    private SweepPartitionId id;
    private Context context;
    private boolean memstoreUpdated = false;
    private boolean mergeSmall = false;
    private Map<MobFilePath, MobFileStatus> fileInfos = new HashMap<MobFilePath, MobFileStatus>();
    private List<MobFilePath> toBeDeleted;

    public SweepPartition(SweepPartitionId id, Context context) throws IOException {
      this.id = id;
      this.context = context;
      this.toBeDeleted = new ArrayList<MobFilePath>();
      memstore.setPartitionId(id);
      init();
    }

    public SweepPartitionId getId() {
      return this.id;
    }

    private void init() throws IOException {
      Path partitionPath = new Path(familyDir, id.getDate());

      FileStatus[] fileStatus = listStatus(partitionPath, id.getStartKey());
      if (null == fileStatus) {
        return;
      }

      int smallFileCount = 0;
      float invalidFileRatio = conf.getFloat(MobConstants.MOB_COMPACTION_INVALID_FILE_RATIO,
          MobConstants.DEFAULT_MOB_COMPACTION_INVALID_FILE_RATIO);
      long smallFileThreshold = conf.getLong(MobConstants.MOB_COMPACTION_SMALL_FILE_THRESHOLD,
          MobConstants.DEFAULT_MOB_COMPACTION_SMALL_FILE_THRESHOLD);
      for (int i = 0; i < fileStatus.length; i++) {
        MobFileStatus info = new MobFileStatus(fileStatus[i]);
        info.setInvalidFileRatio(invalidFileRatio).setSmallFileThreshold(smallFileThreshold);
        if (info.needMerge()) {
          smallFileCount++;
        }
        fileInfos.put(info.getPath(), info);
      }
      if (smallFileCount >= 2) {
        this.mergeSmall = true;
      }
    }

    public void close() throws IOException {
      if (null == id) {
        return;
      }
      // delete files that have no reference
      Set<MobFilePath> filePaths = fileInfos.keySet();
      Iterator<MobFilePath> iter = filePaths.iterator();
      while (iter.hasNext()) {
        MobFilePath path = iter.next();
        MobFileStatus fileInfo = fileInfos.get(path);
        if (fileInfo.getReferenceCount() <= 0) {
          fs.delete(path.getAbsolutePath(familyDir), false);
          context.getCounter(SweepCounter.FILE_NO_REFERENCE).increment(1);
          context.getCounter(SweepCounter.FILE_TO_BE_MERGE_OR_CLEAN).increment(1);
        }
      }

      // flush remain key values into mob files
      if (memstoreUpdated) {
        memstore.flushMemStore();
      }

      // delete old files after compaction
      for (int i = 0; i < toBeDeleted.size(); i++) {
        Path path = toBeDeleted.get(i).getAbsolutePath(familyDir);
        LOG.info("[In Partition close] Delete the file " + path.getName() + " in partition close");
        context.getCounter(SweepCounter.FILE_TO_BE_MERGE_OR_CLEAN).increment(1);
        fs.delete(path, false);
      }
      fileInfos.clear();
    }

    public void execute(Text path, Iterable<KeyValue> values) throws IOException {
      if (null == values) {
        return;
      }
      MobFilePath mobFilePath = MobFilePath.create(path.toString());
      LOG.info("[In reducer] The file path: " + path.toString());
      MobFileStatus info = fileInfos.get(mobFilePath);
      if (null == info) {
        LOG.info("[In reducer] Cannot find the file, probably this record is obsolte");
        return;
      }
      Set<KeyValue> kvs = new HashSet<KeyValue>();
      Iterator<KeyValue> iter = values.iterator();
      while (iter.hasNext()) {
        KeyValue kv = iter.next();
        kvs.add(kv);
        info.addReference();
      }
      if (info.needClean() || (mergeSmall && info.needMerge())) {
        context.getCounter(SweepCounter.INPUT_FILE_COUNT).increment(1);
        MobFile file = MobFile.create(fs, mobFilePath.getAbsolutePath(familyDir), conf, cacheConf);
        StoreFileScanner scanner = null;
        try {
          scanner = file.getScanner();
          scanner.seek(KeyValueUtil.createFirstOnRow(new byte[] {}));

          Cell cell = null;
          while (null != (cell = scanner.next())) {
            KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
            KeyValue keyOnly = kv.createKeyOnly(false);
            if (kvs.contains(keyOnly)) {
              memstore.addToMemstore(kv);
              memstoreUpdated = true;
              memstore.flushMemStoreIfNecessary();
            }
          }
        } finally {
          if (scanner != null) {
            scanner.close();
          }
        }
        toBeDeleted.add(mobFilePath);
      }
    }

    private FileStatus[] listStatus(Path p, String prefix) throws IOException {
      return fs.listStatus(p, new PathPrefixFilter(prefix));
    }
  }

  public static class PathPrefixFilter implements PathFilter {

    private String prefix;

    public PathPrefixFilter(String prefix) {
      this.prefix = prefix;
    }

    public boolean accept(Path path) {
      return path.getName().startsWith(prefix);
    }

  }

  public static class SweepPartitionId {
    private String date;
    private String startKey;

    public SweepPartitionId(MobFilePath filePath) {
      this.date = filePath.getDate();
      this.startKey = filePath.getStartKey();
    }

    public SweepPartitionId(String date, String startKey) {
      this.date = date;
      this.startKey = startKey;
    }

    public static SweepPartitionId create(String key) {
      return new SweepPartitionId(MobFilePath.create(key));
    }

    @Override
    public boolean equals(Object anObject) {
      if (this == anObject) {
        return true;
      }
      if (anObject instanceof MobFilePath) {
        MobFilePath another = (MobFilePath) anObject;
        if (this.date.equals(another.getDate()) && this.startKey.equals(another.getStartKey())) {
          return true;
        }
      }
      return false;
    }

    public String getDate() {
      return this.date;
    }

    public String getStartKey() {
      return this.startKey;
    }

    public void setDate(String date) {
      this.date = date;
    }

    public void setStartKey(String startKey) {
      this.startKey = startKey;
    }
  }

  private static class MobFileStatus {
    private long count;
    private int referenceCount;
    private MobFilePath mobFilePath;
    private long length;

    private float invalidFileRatio = MobConstants.DEFAULT_MOB_COMPACTION_INVALID_FILE_RATIO;
    private long smallFileThreshold = MobConstants.DEFAULT_MOB_COMPACTION_SMALL_FILE_THRESHOLD;

    public MobFileStatus(FileStatus status) {
      Path path = status.getPath();
      String fileName = path.getName();
      String parentName = path.getParent().getName();

      this.length = status.getLen();

      this.mobFilePath = MobFilePath.create(parentName, fileName);
      this.count = mobFilePath.getRecordCount();
      referenceCount = 0;
    }

    public MobFileStatus setInvalidFileRatio(float invalidFileRatio) {
      this.invalidFileRatio = invalidFileRatio;
      return this;
    }

    public MobFileStatus setSmallFileThreshold(long smallFileThreshold) {
      this.smallFileThreshold = smallFileThreshold;
      return this;
    }

    public void addReference() {
      this.referenceCount++;
    }

    public int getReferenceCount() {
      return this.referenceCount;
    }

    public boolean needClean() {
      if (referenceCount >= count) {
        return false;
      }
      if (count - referenceCount > invalidFileRatio * count) {
        return true;
      }
      return false;
    }

    public boolean needMerge() {
      if (this.length < smallFileThreshold) {
        return true;
      }
      return false;
    }

    public MobFilePath getPath() {
      return mobFilePath;
    }
  }
}
