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
package org.apache.hadoop.hbase.mob.filecompactions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobFileName;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.mob.filecompactions.MobFileCompactionRequest.CompactionType;
import org.apache.hadoop.hbase.mob.filecompactions.PartitionedMobFileCompactionRequest.CompactionPartition;
import org.apache.hadoop.hbase.mob.filecompactions.PartitionedMobFileCompactionRequest.CompactionPartitionId;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.Writer;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * An implementation of {@link MobFileCompactor} that compacts the mob files in partitions.
 */
@InterfaceAudience.Private
public class PartitionedMobFileCompactor extends MobFileCompactor {

  private static final Log LOG = LogFactory.getLog(PartitionedMobFileCompactor.class);
  protected long mergeableSize;
  protected int delFileMaxCount;
  /** The number of files compacted in a batch */
  protected int compactionBatch;
  protected int compactionKVMax;

  private Path tempPath;
  private Path bulkloadPath;
  private CacheConfig compactionCacheConfig;
  private Tag tableNameTag;

  public PartitionedMobFileCompactor(Configuration conf, FileSystem fs, TableName tableName,
    HColumnDescriptor column) {
    super(conf, fs, tableName, column);
    mergeableSize = conf.getLong(MobConstants.MOB_FILE_COMPACTION_MERGEABLE_THRESHOLD,
      MobConstants.DEFAULT_MOB_FILE_COMPACTION_MERGEABLE_THRESHOLD);
    delFileMaxCount = conf.getInt(MobConstants.MOB_DELFILE_MAX_COUNT,
      MobConstants.DEFAULT_MOB_DELFILE_MAX_COUNT);
    // default is no limit
    compactionBatch = conf.getInt(MobConstants.MOB_FILE_COMPACTION_BATCH_SIZE,
      MobConstants.DEFAULT_MOB_FILE_COMPACTION_BATCH_SIZE);
    tempPath = new Path(MobUtils.getMobHome(conf), MobConstants.TEMP_DIR_NAME);
    bulkloadPath = new Path(tempPath, new Path(MobConstants.BULKLOAD_DIR_NAME,
      column.getNameAsString()));
    compactionKVMax = this.conf.getInt(HConstants.COMPACTION_KV_MAX,
      HConstants.COMPACTION_KV_MAX_DEFAULT);
    Configuration copyOfConf = new Configuration(conf);
    copyOfConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0f);
    compactionCacheConfig = new CacheConfig(copyOfConf);
    tableNameTag = new Tag(TagType.MOB_TABLE_NAME_TAG_TYPE, tableName.getName());
  }

  @Override
  public List<Path> compact(List<FileStatus> files) throws IOException {
    if (files == null || files.isEmpty()) {
      return null;
    }
    // find the files to compact.
    PartitionedMobFileCompactionRequest request = select(files);
    // compact the files.
    return performCompact(request);
  }

  /**
   * Selects the compacted mob/del files.
   * Iterates the candidates to find out all the del files and small mob files.
   * @param candidates All the candidates.
   * @return A compaction request.
   */
  protected PartitionedMobFileCompactionRequest select(List<FileStatus> candidates) {
    Collection<FileStatus> allDelFiles = new ArrayList<FileStatus>();
    Map<CompactionPartitionId, CompactionPartition> filesToCompact =
      new HashMap<CompactionPartitionId, CompactionPartition>();
    int smallFilesCount = 0;
    for (FileStatus file : candidates) {
      if (file.isFile()) {
        // group the del files and small files.
        if (StoreFileInfo.isDelFile(file.getPath())) {
          allDelFiles.add(file);
        } else if (file.getLen() < mergeableSize) {
          // add the small files to the merge pool
          MobFileName fileName = MobFileName.create(file.getPath().getName());
          CompactionPartitionId id = new CompactionPartitionId(fileName.getStartKey(), fileName.getDate());
          CompactionPartition compactionPartition = filesToCompact.get(id);
          if (compactionPartition == null) {
            compactionPartition = new CompactionPartition(id);
            compactionPartition.addFile(file);
            filesToCompact.put(id, compactionPartition);
          } else {
            compactionPartition.addFile(file);
          }
          smallFilesCount++;
        }
      }
    }
    PartitionedMobFileCompactionRequest request = new PartitionedMobFileCompactionRequest(
      filesToCompact.values(), allDelFiles);
    if (candidates.size() == (allDelFiles.size() + smallFilesCount)) {
      // all the files are selected
      request.setCompactionType(CompactionType.ALL_FILES);
    }
    return request;
  }

  /**
   * Performs the compaction on the selected files.
   * <ol>
   * <li>Compacts the del files.</li>
   * <li>Compacts the selected small mob files and all the del files.</li>
   * <li>If all the candidates are selected, delete the del files.</li>
   * </ol>
   * @param request The compaction request.
   * @return The paths of new mob files generated in the compaction.
   * @throws IOException
   */
  protected List<Path> performCompact(PartitionedMobFileCompactionRequest request) throws IOException {
    // merge the del files
    List<Path> delFilePaths = new ArrayList<Path>();
    for (FileStatus delFile : request.delFiles) {
      delFilePaths.add(delFile.getPath());
    }
    List<Path> newDelPaths = compactDelFiles(request, delFilePaths);
    List<StoreFile> delFiles = new ArrayList<StoreFile>();
    for (Path newDelPath : newDelPaths) {
      StoreFile sf = new StoreFile(fs, newDelPath, conf, compactionCacheConfig, BloomType.NONE);
      delFiles.add(sf);
    }
    // compact the mob files by partitions.
    List<Path> paths = compactMobFiles(request, delFiles);
    // archive the del files if all the mob files are selected.
    if (request.type == CompactionType.ALL_FILES && !newDelPaths.isEmpty()) {
      try {
        MobUtils.removeMobFiles(conf, fs, tableName, mobTableDir, column.getName(), delFiles);
      } catch (IOException e) {
        LOG.error("Failed to archive the del files " + delFiles, e);
      }
    }
    return paths;
  }

  /**
   * Compacts the selected small mob files and all the del files.
   * @param request The compaction request.
   * @param delFiles The del files.
   * @return The paths of new mob files after compactions.
   * @throws IOException
   */
  protected List<Path> compactMobFiles(PartitionedMobFileCompactionRequest request,
    List<StoreFile> delFiles) throws IOException {
    Collection<CompactionPartition> partitions = request.compactionPartitions;
    if (partitions == null || partitions.isEmpty()) {
      return Collections.emptyList();
    }
    List<Path> paths = new ArrayList<Path>();
    HTable table = new HTable(conf, tableName);
    try {
      // compact the mob files by partitions.
      for (CompactionPartition partition : partitions) {
        paths.addAll(compactMobFilePartition(request, partition, delFiles, table));
      }
    } finally {
      try {
        table.close();
      } catch (IOException e) {
        LOG.error("Failed to close the HTable", e);
      }
    }
    return paths;
  }

  /**
   * Compacts a partition of selected small mob files and all the del files.
   * @param request The compaction request.
   * @param partition A compaction partition.
   * @param delFiles The del files.
   * @param table The current table.
   * @return The paths of new mob files after compactions.
   * @throws IOException
   */
  private List<Path> compactMobFilePartition(PartitionedMobFileCompactionRequest request,
    CompactionPartition partition, List<StoreFile> delFiles, HTable table) throws IOException {
    List<Path> newFiles = new ArrayList<Path>();
    List<FileStatus> files = partition.listFiles();
    int offset = 0;
    while (offset < files.size()) {
      int batch = compactionBatch;
      if (files.size() - offset < compactionBatch) {
        batch = files.size() - offset;
      }
      if (batch == 1 && delFiles.isEmpty()) {
        // only one file left and no del files, do not compact it.
        offset++;
        continue;
      }
      // add the selected mob files and del files into filesToCompact
      List<StoreFile> filesToCompact = new ArrayList<StoreFile>();
      for (int i = offset; i < batch + offset; i++) {
        StoreFile sf = new StoreFile(fs, files.get(i).getPath(), conf, compactionCacheConfig,
          BloomType.NONE);
        filesToCompact.add(sf);
      }
      // Pair(maxSeqId, cellsCount)
      Pair<Long, Long> fileInfo = getFileInfo(filesToCompact);
      filesToCompact.addAll(delFiles);
      // open scanners to the selected mob files and del files.
      List scanners = StoreFileScanner.getScannersForStoreFiles(filesToCompact, false, true, false,
        null, HConstants.LATEST_TIMESTAMP);
      Scan scan = new Scan();
      scan.setMaxVersions(column.getMaxVersions());
      long ttl = HStore.determineTTLFromFamily(column);
      ScanInfo scanInfo = new ScanInfo(column, ttl, 0, KeyValue.COMPARATOR);
      StoreScanner scanner = new StoreScanner(scan, scanInfo, ScanType.COMPACT_DROP_DELETES, null,
        scanners, 0L, HConstants.LATEST_TIMESTAMP);
      // open writers for the mob files and new ref store files.
      Writer writer = null;
      Writer refFileWriter = null;
      Path filePath = null;
      Path refFilePath = null;
      long mobCells = 0;
      try {
        writer = MobUtils.createWriter(conf, fs, column, partition.getPartitionId().getDate(),
          tempPath, Long.MAX_VALUE, column.getCompactionCompression(), partition.getPartitionId()
            .getStartKey(), compactionCacheConfig);
        filePath = writer.getPath();
        byte[] fileName = Bytes.toBytes(filePath.getName());
        // create a temp file and open a writer for it in the bulkloadPath
        refFileWriter = MobUtils.createRefFileWriter(conf, fs, column, bulkloadPath, fileInfo
          .getSecond().longValue(), compactionCacheConfig);
        refFilePath = refFileWriter.getPath();
        List<Cell> cells = new ArrayList<Cell>();
        boolean hasMore = false;
        do {
          hasMore = scanner.next(cells, compactionKVMax);
          for (Cell cell : cells) {
            KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
            writer.append(kv);
            KeyValue reference = MobUtils.createMobRefKeyValue(kv, fileName, tableNameTag);
            refFileWriter.append(reference);
            mobCells++;
          }
          cells.clear();
        } while (hasMore);
      } finally {
        scanner.close();
        if (writer != null) {
          writer.appendMetadata(fileInfo.getFirst(), false, mobCells);
          try {
            writer.close();
          } catch (IOException e) {
            LOG.error("Failed to close the writer of the file " + filePath, e);
          }
        }
        if (refFileWriter != null) {
          refFileWriter.appendMetadata(fileInfo.getFirst(), false);
          refFileWriter.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
            Bytes.toBytes(request.selectionTime));
          try {
            refFileWriter.close();
          } catch (IOException e) {
            LOG.error("Failed to close the writer of the ref file " + refFilePath, e);
          }
        }
      }
      if (mobCells > 0) {
        // commit mob file
        MobUtils.commitFile(conf, fs, filePath, mobFamilyDir, compactionCacheConfig);
        // bulkload the ref file
        try {
          LoadIncrementalHFiles bulkload = new LoadIncrementalHFiles(conf);
          bulkload.doBulkLoad(bulkloadPath, table);
        } catch (Exception e) {
          // delete the committed mob file and bulkload files in bulkloadPath
          deletePath(new Path(mobFamilyDir, filePath.getName()));
          deletePath(bulkloadPath);
          throw new IOException(e);
        }
        // archive old mob files, but not include the del files.
        List<StoreFile> archivedFiles = filesToCompact.subList(0, batch);
        try {
          MobUtils
            .removeMobFiles(conf, fs, tableName, mobTableDir, column.getName(), archivedFiles);
        } catch (IOException e) {
          LOG.error("Failed to archive the files " + archivedFiles, e);
        }
        newFiles.add(new Path(mobFamilyDir, filePath.getName()));
      } else {
        // remove the new files
        // the mob file is empty, delete it instead of committing.
        deletePath(filePath);
        // the mob file is empty, delete it instead of committing.
        deletePath(refFilePath);
      }
      offset += batch;
    }
    return newFiles;
  }

  /**
   * Compacts the del files in batches which avoids opening too many files.
   * @param request The compaction request.
   * @param delFilePaths
   * @return The paths of new del files after merging.
   * @throws IOException
   */
  protected List<Path> compactDelFiles(PartitionedMobFileCompactionRequest request,
    List<Path> delFilePaths) throws IOException {
    if (delFilePaths.size() > delFileMaxCount) {
      // when there are more del files than the number that is allowed, merge it firstly.
      int index = 0;
      List<StoreFile> batchedDelFiles = new ArrayList<StoreFile>();
      List<Path> paths = new ArrayList<Path>();
      Iterator<Path> pathIterator = delFilePaths.iterator();
      boolean hasNext = pathIterator.hasNext();
      while (hasNext) {
        // compact the del files in batches.
        if (index < compactionBatch) {
          StoreFile delFile = new StoreFile(fs, pathIterator.next(), conf, compactionCacheConfig,
            BloomType.NONE);
          batchedDelFiles.add(delFile);
          index++;
        } else {
          // merge del store files in a batch.
          paths.add(mergeDelFiles(request, batchedDelFiles));
          batchedDelFiles.clear();
          index = 0;
        }
        hasNext = pathIterator.hasNext();
        if (!hasNext) {
          if (index > 0) {
            // when the number of del files does not reach the compactionBatch and no more del
            // files are left, directly merge them.
            paths.add(mergeDelFiles(request, batchedDelFiles));
            batchedDelFiles.clear();
          }
          // check whether the number of the remaining del files is not larger than the max count.
          if (paths.size() > delFileMaxCount) {
            // continue next round of merging.
            pathIterator = paths.iterator();
            hasNext = true;
            index = 0;
            paths = new ArrayList<>();
          }
        }
      }
      return paths;
    } else {
      return delFilePaths;
    }
  }

  /**
   * Merges the del file in a batch.
   * @param request The compaction request.
   * @param delFiles The del files.
   * @return The path of new del file after merging.
   * @throws IOException
   */
  private Path mergeDelFiles(PartitionedMobFileCompactionRequest request, List<StoreFile> delFiles)
    throws IOException {
    // create a scanner for the del files.
    List scanners = StoreFileScanner.getScannersForStoreFiles(delFiles, false, true, false, null,
      HConstants.LATEST_TIMESTAMP);
    Scan scan = new Scan();
    scan.setMaxVersions(column.getMaxVersions());
    long ttl = HStore.determineTTLFromFamily(column);
    ScanInfo scanInfo = new ScanInfo(column, ttl, 0, KeyValue.COMPARATOR);
    StoreScanner scanner = new StoreScanner(scan, scanInfo, ScanType.COMPACT_RETAIN_DELETES, null,
      scanners, 0L, HConstants.LATEST_TIMESTAMP);
    Writer writer = null;
    Path filePath = null;
    try {
      writer = MobUtils.createDelFileWriter(conf, fs, column,
        MobUtils.formatDate(new Date(request.selectionTime)), tempPath, Long.MAX_VALUE,
        column.getCompactionCompression(), HConstants.EMPTY_START_ROW, compactionCacheConfig);
      filePath = writer.getPath();
      List<Cell> cells = new ArrayList<Cell>();
      boolean hasMore = false;
      do {
        hasMore = scanner.next(cells, compactionKVMax);
        for (Cell cell : cells) {
          KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
          writer.append(kv);
        }
        cells.clear();
      } while (hasMore);
    } finally {
      scanner.close();
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          LOG.error("Failed to close the writer of the file " + filePath, e);
        }
      }
    }
    // commit the new del file
    Path path = MobUtils.commitFile(conf, fs, filePath, mobFamilyDir, compactionCacheConfig);
    // archive the old del files
    try {
      MobUtils.removeMobFiles(conf, fs, tableName, mobTableDir, column.getName(), delFiles);
    } catch (IOException e) {
      LOG.error("Failed to archive the old del files " + delFiles, e);
    }
    return path;
  }

  /**
   * Gets the max seqId and number of cells of the store files.
   * @param storeFiles The store files.
   * @return The pair of the max seqId and number of cells of the store files.
   * @throws IOException
   */
  private Pair<Long, Long> getFileInfo(List<StoreFile> storeFiles) throws IOException {
    long maxSeqId = 0;
    long maxKeyCount = 0;
    for (StoreFile sf : storeFiles) {
      // the readers will be closed later after the merge.
      maxSeqId = Math.max(maxSeqId, sf.getMaxSequenceId());
      byte[] count = sf.createReader().loadFileInfo().get(StoreFile.MOB_CELLS_COUNT);
      if (count != null) {
        maxKeyCount += Bytes.toLong(count);
      }
    }
    return new Pair<Long, Long>(Long.valueOf(maxSeqId), Long.valueOf(maxKeyCount));
  }

  /**
   * Deletes a file.
   * @param path The path of the file to be deleted.
   */
  private void deletePath(Path path) {
    try {
      if (path != null) {
        fs.delete(path, true);
      }
    } catch (IOException e) {
      LOG.error("Failed to delete the file " + path, e);
    }
  }
}
