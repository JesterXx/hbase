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
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.hbase.mob.filecompactions.MobFileCompactRequest.CompactionType;
import org.apache.hadoop.hbase.mob.filecompactions.StripeMobFileCompactRequest.CompactedStripe;
import org.apache.hadoop.hbase.mob.filecompactions.StripeMobFileCompactRequest.CompactedStripeId;
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
 *
 */
public class StripeMobFileCompactor extends MobFileCompactor {

  private static final Log LOG = LogFactory.getLog(StripeMobFileCompactor.class);
  protected long mergeableSize;
  protected int delFileMaxCount;
  protected int compactionBatch;
  protected int compactionKVMax;

  private Path tempPath;
  private Path bulkloadPath;
  private CacheConfig compactionCacheConfig;
  private Tag tableNameTag;

  public StripeMobFileCompactor(Configuration conf, FileSystem fs, TableName tableName,
    HColumnDescriptor column) {
    super(conf, fs, tableName, column);
    mergeableSize = conf.getLong(MobConstants.MOB_COMPACTION_MERGEABLE_SIZE,
      MobConstants.DEFAULT_MOB_COMPACTION_MERGEABLE_SIZE);
    delFileMaxCount = conf.getInt(MobConstants.MOB_DELFILE_MAX_COUNT,
      MobConstants.DEFAULT_MOB_DELFILE_MAX_COUNT);
    compactionBatch = conf.getInt(MobConstants.MOB_COMPACTION_BATCH_SIZE,
      MobConstants.DEFAULT_MOB_COMPACTION_BATCH_SIZE);
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
    StripeMobFileCompactRequest request = select(files);
    return performCompact(request);
  }

  /**
   * Selects the compacted mob/del files.
   * Iterates the candidates to find out all the del files and small mob files.
   * @param files All the candidates.
   * @return The compact request.
   */
  protected StripeMobFileCompactRequest select(List<FileStatus> files) {
    Collection<FileStatus> allDelFiles = new ArrayList<FileStatus>();
    Map<CompactedStripeId, CompactedStripe> filesToCompact =
      new HashMap<CompactedStripeId, CompactedStripe>();
    Iterator<FileStatus> ir = files.iterator();
    for (; ir.hasNext();) {
      FileStatus file = ir.next();
      if (file.isFile()) {
        if (StoreFileInfo.isDelFile(file.getPath())) {
          allDelFiles.add(file);
          ir.remove();
        } else if (file.getLen() < mergeableSize) {
          // add it to the merge pool
          MobFileName fileName = MobFileName.create(file.getPath().getName());
          CompactedStripeId id = new CompactedStripeId(fileName.getStartKey(), fileName.getDate());
          CompactedStripe compactedStripe = filesToCompact.get(id);
          if (compactedStripe == null) {
            compactedStripe = new CompactedStripe(id);
            compactedStripe.addFile(file);
            filesToCompact.put(id, compactedStripe);
          } else {
            compactedStripe.addFile(file);
          }
          ir.remove();
        } 
      } else {
        ir.remove();
      }
    }
    StripeMobFileCompactRequest request = new StripeMobFileCompactRequest(filesToCompact.values(),
      allDelFiles);
    if (files.isEmpty()) {
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
   * @param request
   * @return
   * @throws IOException
   */
  protected List<Path> performCompact(StripeMobFileCompactRequest request) throws IOException {
    // merge the del files
    List<Path> delFilePaths = new ArrayList<Path>();
    for (FileStatus delFile : request.allDelFiles) {
      delFilePaths.add(delFile.getPath());
    }
    List<Path> newDelPaths = compactDelFiles(request, delFilePaths);
    List<StoreFile> delFiles = new ArrayList<StoreFile>();
    for (Path newDelPath : newDelPaths) {
      StoreFile sf = new StoreFile(fs, newDelPath, conf, compactionCacheConfig, BloomType.NONE);
      delFiles.add(sf);
    }
    // compact the mob files by stripes.
    List<Path> paths = compactMobFiles(request, delFiles);
    // archive the del files if the all mob files are selected.
    if (request.type == CompactionType.ALL_FILES && !newDelPaths.isEmpty()) {
      try {
        MobUtils.removeMobFiles(conf, fs, tableName, mobTableDir, column.getName(), delFiles);
      } catch (IOException e) {
        LOG.error("Fail to archive the del files " + delFiles, e);
      }
    }
    return paths;
  }

  /**
   * Compacts the selected small mob files and all the del files.
   * @param request
   * @param delFiles
   * @return The paths of new mob files after compactions.
   * @throws IOException
   */
  protected List<Path> compactMobFiles(StripeMobFileCompactRequest request,
    List<StoreFile> delFiles) throws IOException {
    Collection<CompactedStripe> stripes = request.compactedStripes;
    if (stripes == null || stripes.isEmpty()) {
      return null;
    }
    List<Path> paths = new ArrayList<Path>();
    HTable table = new HTable(conf, tableName);
    try {
      for (CompactedStripe stripe : stripes) {
        paths.addAll(compactMobFileStripe(request, stripe, 0, delFiles, table));
      }
    } finally {
      try {
        table.close();
      } catch (IOException e) {
        LOG.error("Fail to close the HTable", e);
      }
    }
    return paths;
  }

  /**
   * Compacts a stripe of selected small mob files and all the del files.
   * @param request
   * @param stripe
   * @param offset
   * @param delFiles
   * @param table
   * @return The paths of new mob files after compactions.
   * @throws IOException
   */
  private List<Path> compactMobFileStripe(StripeMobFileCompactRequest request,
    CompactedStripe stripe, int offset, List<StoreFile> delFiles, HTable table)
      throws IOException {
    List<Path> newFiles = new ArrayList<Path>();
    List<FileStatus> files = stripe.listFiles();
    boolean isLastBatch = false;
    int batch = compactionBatch;
    if (files.size() - offset <= compactionBatch) {
      isLastBatch = true;
      batch = files.size() - offset;
    }
    // add the selected mob files and del files into filesToCompact
    List<StoreFile> filesToCompact = new ArrayList<StoreFile>();
    for (int i = offset; i < batch + offset; i++) {
      StoreFile sf = new StoreFile(fs, files.get(offset).getPath(), conf, compactionCacheConfig,
        BloomType.NONE);
      filesToCompact.add(sf);
    }
    Pair<Long, Long> fileInfo = getFileInfo(filesToCompact);
    filesToCompact.addAll(delFiles);
    List scanners = StoreFileScanner.getScannersForStoreFiles(filesToCompact, false, true, false,
      null, HConstants.LATEST_TIMESTAMP);
    Scan scan = new Scan();
    scan.setMaxVersions(column.getMaxVersions());
    long ttl = HStore.determineTTLFromFamily(column);
    ScanInfo scanInfo = new ScanInfo(column, ttl, 0, KeyValue.COMPARATOR);
    StoreScanner scanner = new StoreScanner(scan, scanInfo, ScanType.COMPACT_DROP_DELETES, null,
      scanners, 0L, HConstants.LATEST_TIMESTAMP);
    Writer writer = null;
    Writer refFileWriter = null;
    Path filePath = null;
    Path refFilePath = null;
    long mobCells = 0;
    try {
      writer = MobUtils.createWriter(conf, fs, column, stripe.getStripeId().getDate(), tempPath,
        Long.MAX_VALUE, column.getCompactionCompression(), stripe.getStripeId().getStartKey(),
        compactionCacheConfig);
      filePath = writer.getPath();
      byte[] fileName = Bytes.toBytes(filePath.getName());
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
          LOG.error("Fail to close the writer of the file " + filePath, e);
        }
      }
      if (refFileWriter != null) {
        refFileWriter.appendMetadata(fileInfo.getFirst(), false);
        refFileWriter.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
          Bytes.toBytes(request.selectionTime));
        try {
          refFileWriter.close();
        } catch (IOException e) {
          LOG.error("Fail to close the writer of the ref file " + refFilePath, e);
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
      // archive old files
      List<StoreFile> archivedFiles = filesToCompact.subList(offset, batch);
      try {
        MobUtils.removeMobFiles(conf, fs, tableName, mobTableDir, column.getName(), archivedFiles);
      } catch (IOException e) {
        LOG.error("Fail to archive the files " + archivedFiles, e);
      }
    } else {
      // remove the new files
      // the mob file is empty, delete it instead of committing.
      deletePath(filePath);
      // the mob file is empty, delete it instead of committing.
      deletePath(refFilePath);
    }
    // next round
    if (!isLastBatch) {
      newFiles.addAll(compactMobFileStripe(request, stripe, offset + compactionBatch, delFiles,
        table));
    }
    return newFiles;
  }

  /**
   * Compacts the del files in batches.
   * @param request
   * @param filePaths
   * @return
   * @throws IOException
   */
  protected List<Path> compactDelFiles(StripeMobFileCompactRequest request, List<Path> filePaths)
    throws IOException {
    if (filePaths.size() > delFileMaxCount) {
      int index = 0;
      List<StoreFile> sfs = new ArrayList<StoreFile>();
      List<Path> paths = new ArrayList<Path>();
      for (Path filePath : filePaths) {
        if (index < compactionBatch) {
          StoreFile sf = new StoreFile(fs, filePath, conf, compactionCacheConfig, BloomType.NONE);
          sfs.add(sf);
          index++;
        } else {
          // merge del store files.
          paths.add(mergeDelFiles(request, sfs));
          sfs.clear();
          index = 0;
        }
      }
      if (index > 0) {
        // merge del store files.
        paths.add(mergeDelFiles(request, sfs));
        sfs.clear();
      }
      return compactDelFiles(request, paths);
    } else {
      return filePaths;
    }
  }

  /**
   * Merges the del file in a batch.
   * @param request
   * @param storeFiles
   * @return The path of new del file after merging.
   * @throws IOException
   */
  private Path mergeDelFiles(StripeMobFileCompactRequest request, List<StoreFile> storeFiles)
    throws IOException {
    List scanners = StoreFileScanner.getScannersForStoreFiles(storeFiles, false, true, false, null,
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
          LOG.error("Fail to close the writer of the file " + filePath, e);
        }
      }
    }
    // commit the new del file
    Path path = MobUtils.commitFile(conf, fs, filePath, mobFamilyDir, compactionCacheConfig);
    // archive the old del files
    try {
      MobUtils.removeMobFiles(conf, fs, tableName, mobTableDir, column.getName(), storeFiles);
    } catch (IOException e) {
      LOG.error("Fail to archive the old del files " + storeFiles, e);
    }
    return path;
  }

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

  private void deletePath(Path path) {
    try {
      if (path != null) {
        fs.delete(path, true);
      }
    } catch (IOException e) {
      LOG.error("Fail to delete the file " + path, e);
    }
  }
}
