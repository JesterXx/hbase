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
import java.net.InetSocketAddress;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobFileName;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.mob.compactions.MobCompactionRequest.CompactionType;
import org.apache.hadoop.hbase.mob.compactions.PartitionedMobCompactionRequest.CompactionPartition;
import org.apache.hadoop.hbase.mob.compactions.PartitionedMobCompactionRequest.CompactionPartitionId;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

/**
 * An implementation of {@link MobCompactor} that compacts the mob files in partitions.
 */
@InterfaceAudience.Private
public class PartitionedMobCompactor extends MobCompactor {

  private static final Log LOG = LogFactory.getLog(PartitionedMobCompactor.class);
  protected long mergeableSize;
  protected int delFileMaxCount;
  /** The number of files compacted in a batch */
  protected int compactionBatchSize;
  protected int compactionKVMax;

  private Path tempPath;
  private Path bulkloadPath;
  private CacheConfig compactionCacheConfig;
  private Tag tableNameTag;
  private Encryption.Context cryptoContext = Encryption.Context.NONE;
  private PartitionedMobCompactionRequest request;
  private Map<String, byte[]> prefixAndKeys;
  private HRegionInfo regionInfo;

  public PartitionedMobCompactor(RegionServerServices rss, Region region, TableName tableName,
    HColumnDescriptor column, Map<String, byte[]> prefixAndKeys) throws IOException {
    super(rss, region, tableName, column);
    this.prefixAndKeys = prefixAndKeys;
    regionInfo = this.region.getRegionInfo();
    mergeableSize = conf.getLong(MobConstants.MOB_COMPACTION_MERGEABLE_THRESHOLD,
      MobConstants.DEFAULT_MOB_COMPACTION_MERGEABLE_THRESHOLD);
    delFileMaxCount = conf.getInt(MobConstants.MOB_DELFILE_MAX_COUNT,
      MobConstants.DEFAULT_MOB_DELFILE_MAX_COUNT);
    // default is 100
    compactionBatchSize = conf.getInt(MobConstants.MOB_COMPACTION_BATCH_SIZE,
      MobConstants.DEFAULT_MOB_COMPACTION_BATCH_SIZE);
    tempPath = new Path(MobUtils.getMobHome(conf), MobConstants.TEMP_DIR_NAME);
    bulkloadPath = new Path(tempPath, new Path(MobConstants.BULKLOAD_DIR_NAME, new Path(
      tableName.getNamespaceAsString(), tableName.getQualifierAsString())));
    compactionKVMax = this.conf.getInt(HConstants.COMPACTION_KV_MAX,
      HConstants.COMPACTION_KV_MAX_DEFAULT);
    boolean cacheDataOnRead = conf.getBoolean(CacheConfig.CACHE_DATA_ON_READ_KEY,
      CacheConfig.DEFAULT_CACHE_DATA_ON_READ);
    Configuration configration = conf;
    if (cacheDataOnRead) {
      Configuration copyOfConf = new Configuration(conf);
      copyOfConf.setBoolean(CacheConfig.CACHE_DATA_ON_READ_KEY, Boolean.FALSE);
      configration = copyOfConf;
    }
    compactionCacheConfig = new CacheConfig(configration);
    tableNameTag = new ArrayBackedTag(TagType.MOB_TABLE_NAME_TAG_TYPE, tableName.getName());
    cryptoContext = EncryptionUtil.createEncryptionContext(configration, column);
  }

  @Override
  public List<Path> compact(List<FileStatus> files, boolean allFiles) throws IOException {
    if (files == null || files.isEmpty()) {
      LOG.info("No candidate mob files");
      return null;
    }
    LOG.info("is allFiles: " + allFiles);
    // find the files to compact.
    request = select(files, allFiles);
    // compact the files.
    return performCompaction(request);
  }

  /**
   * Gets the compaction request.
   * @return The compaction request.
   */
  public PartitionedMobCompactionRequest getPartitionedMobCompactionRequest() {
    return this.request;
  }

  /**
   * Selects the compacted mob/del files.
   * Iterates the candidates to find out all the del files and small mob files.
   * @param candidates All the candidates.
   * @param allFiles Whether add all mob files into the compaction.
   * @return A compaction request.
   * @throws IOException
   */
  protected PartitionedMobCompactionRequest select(List<FileStatus> candidates,
    boolean allFiles) throws IOException {
    long selectionTime = EnvironmentEdgeManager.currentTime();
    Date expiredDate = new Date(selectionTime - column.getTimeToLive() * 1000);
    expiredDate = new Date(expiredDate.getYear(), expiredDate.getMonth(), expiredDate.getDate());
    Collection<FileStatus> allDelFiles = new ArrayList<FileStatus>();
    Map<CompactionPartitionId, CompactionPartition> filesToCompact =
      new HashMap<CompactionPartitionId, CompactionPartition>();
    int selectedFileCount = 0;
    int irrelevantFileCount = 0;
    int expiredFileCount = 0;
    for (FileStatus file : candidates) {
      if (!file.isFile()) {
        irrelevantFileCount++;
        continue;
      }
      // group the del files and small files.
      FileStatus linkedFile = file;
      HFileLink link = null;
      String fn = file.getPath().getName();
      if (HFileLink.isHFileLink(file.getPath())) {
        link = HFileLink.buildFromHFileLinkPattern(conf, file.getPath());
        fn = link.getOriginPath().getName(); 
      }
      if (!StoreFileInfo.isDelFile((fn))) {
        MobFileName fileName = MobFileName.create(fn);
        if (!isOwnByRegion(fileName.getStartKey())) {
          irrelevantFileCount++;
          continue;
        }
      }
      if (link != null) {
        linkedFile = MobUtils.getReferencedFileStatus(fs, link);
        if (linkedFile == null) {
          // If the linked file cannot be found, regard it as an irrelevantFileCount file
          irrelevantFileCount++;
          continue;
        }
      }
      if (StoreFileInfo.isDelFile(linkedFile.getPath())) {
        allDelFiles.add(file);
      } else {
        MobFileName fileName = MobFileName.create(linkedFile.getPath().getName());
        if (isExpiredMobFile(fileName, expiredDate)) {
          expiredFileCount++;
          continue;
        }
        if (allFiles || linkedFile.getLen() < mergeableSize) {
          // add all files if allFiles is true,
          // otherwise add the small files to the merge pool
          CompactionPartitionId id = new CompactionPartitionId(fileName.getStartKey(),
            fileName.getDate());
          CompactionPartition compactionPartition = filesToCompact.get(id);
          if (compactionPartition == null) {
            compactionPartition = new CompactionPartition(id);
            compactionPartition.addFile(file);
            filesToCompact.put(id, compactionPartition);
          } else {
            compactionPartition.addFile(file);
          }
          selectedFileCount++;
        }
      }
    }
    PartitionedMobCompactionRequest request = new PartitionedMobCompactionRequest(
      filesToCompact.values(), allDelFiles, selectionTime);
    if (candidates.size() == (allDelFiles.size() + selectedFileCount + irrelevantFileCount +
      expiredFileCount)) {
      // all the files are selected
      request.setCompactionType(CompactionType.ALL_FILES);
    }
    LOG.info("The compaction type is " + request.getCompactionType() + ", the request has "
      + allDelFiles.size() + " del files, " + selectedFileCount + " selected files, "
      + irrelevantFileCount + " irrelevant files, and " + expiredFileCount + " expired files");
    return request;
  }

  /**
   * Gets whether the start key is owned by the current region.
   * @param prefix The prefix of the mob file name.
   * @return True if the start key is owned by the current region.
   */
  private boolean isOwnByRegion(String prefix) {
    byte[] startKey = prefixAndKeys.get(prefix);
    return regionInfo.containsRow(startKey);
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
  protected List<Path> performCompaction(PartitionedMobCompactionRequest request)
    throws IOException {
    // merge the del files
    List<Path> delFilePaths = new ArrayList<Path>();
    for (FileStatus delFile : request.delFiles) {
      delFilePaths.add(delFile.getPath());
    }
    List<StoreFile> delFiles = new ArrayList<StoreFile>();
    List<Path> paths = null;
    try {
      for (Path delFilePath : delFilePaths) {
        StoreFile sf = new StoreFile(fs, delFilePath, conf, compactionCacheConfig, BloomType.NONE);
        sf.createReader();
        delFiles.add(sf);
      }
      LOG.info("After merging, there are " + delFiles.size() + " del files");
      // compact the mob files by partitions.
      paths = compactMobFiles(request, delFiles);
      LOG.info("After compaction, there are " + paths.size() + " mob files");
    } finally {
      closeStoreFileReaders(delFiles);
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
  protected List<Path> compactMobFiles(final PartitionedMobCompactionRequest request,
    final List<StoreFile> delFiles) throws IOException {
    Collection<CompactionPartition> partitions = request.compactionPartitions;
    if (partitions == null || partitions.isEmpty()) {
      LOG.info("No partitions of mob files");
      return Collections.emptyList();
    }
    List<Path> paths = new ArrayList<Path>();
    Connection c = rss.getConnection();
    final Table table = c.getTable(tableName);
    try {
      List<CompactionPartitionId> failedPartitions = new ArrayList<CompactionPartitionId>();
      for (final CompactionPartition partition : partitions) {
        LOG.info("Compacting mob files for partition " + partition.getPartitionId());
        try {
          paths.addAll(compactMobFilePartition(request, partition, delFiles, table));
        } catch (Exception e) {
          // just log the error
          LOG.error("Failed to compact the partition " + partition.getPartitionId(), e);
          failedPartitions.add(partition.getPartitionId());
        }
      }
      if (!failedPartitions.isEmpty()) {
        // if any partition fails in the compaction, directly throw an exception.
        throw new IOException("Failed to compact the partitions " + failedPartitions);
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
  private List<Path> compactMobFilePartition(PartitionedMobCompactionRequest request,
    CompactionPartition partition, List<StoreFile> delFiles, Table table) throws IOException {
    List<Path> newFiles = new ArrayList<Path>();
    List<FileStatus> files = partition.listFiles();
    int offset = 0;
    Path bulkloadPathOfPartition = new Path(bulkloadPath, partition.getPartitionId().toString());
    Path bulkloadColumnPath = new Path(bulkloadPathOfPartition, column.getNameAsString());
    while (offset < files.size()) {
      int batch = compactionBatchSize;
      if (files.size() - offset < compactionBatchSize) {
        batch = files.size() - offset;
      }
      if (batch == 1 && delFiles.isEmpty()) {
        // only one file left and no del files, do not compact it,
        // and directly add it to the new files.
        newFiles.add(files.get(offset).getPath());
        offset++;
        continue;
      }
      // clean the bulkload directory to avoid loading old files.
      fs.delete(bulkloadPathOfPartition, true);
      // add the selected mob files and del files into filesToCompact
      List<StoreFile> filesToCompact = new ArrayList<StoreFile>();
      for (int i = offset; i < batch + offset; i++) {
        StoreFile sf = new StoreFile(fs, files.get(i).getPath(), conf, compactionCacheConfig,
          BloomType.NONE);
        filesToCompact.add(sf);
      }
      filesToCompact.addAll(delFiles);
      // compact the mob files in a batch.
      compactMobFilesInBatch(request, partition, table, filesToCompact, batch,
        bulkloadPathOfPartition, bulkloadColumnPath, newFiles);
      // move to the next batch.
      offset += batch;
    }
    LOG.info("Compaction is finished. The number of mob files is changed from " + files.size()
      + " to " + newFiles.size());
    return newFiles;
  }

  /**
   * Closes the readers of store files.
   * @param storeFiles The store files to be closed.
   */
  private void closeStoreFileReaders(List<StoreFile> storeFiles) {
    for (StoreFile storeFile : storeFiles) {
      try {
        storeFile.closeReader(true);
      } catch (IOException e) {
        LOG.warn("Failed to close the reader on store file " + storeFile.getPath(), e);
      }
    }
  }

  /**
   * Compacts a partition of selected small mob files and all the del files in a batch.
   * @param request The compaction request.
   * @param partition A compaction partition.
   * @param table The current table.
   * @param filesToCompact The files to be compacted.
   * @param batch The number of mob files to be compacted in a batch.
   * @param bulkloadPathOfPartition The directory where the bulkload column of the current
   *        partition is saved.
   * @param bulkloadColumnPath The directory where the bulkload files of current partition
   *        are saved.
   * @param newFiles The paths of new mob files after compactions.
   * @throws IOException
   */
  private void compactMobFilesInBatch(PartitionedMobCompactionRequest request,
    CompactionPartition partition, Table table, List<StoreFile> filesToCompact, int batch,
    Path bulkloadPathOfPartition, Path bulkloadColumnPath, List<Path> newFiles)
    throws IOException {
    // open scanner to the selected mob files and del files.
    StoreScanner scanner = createScanner(filesToCompact, ScanType.COMPACT_DROP_DELETES);
    // the mob files to be compacted, not include the del files.
    List<StoreFile> mobFilesToCompact = filesToCompact.subList(0, batch);
    // Pair(maxSeqId, cellsCount)
    Pair<Long, Long> fileInfo = getFileInfo(mobFilesToCompact);
    // open writers for the mob files and new ref store files.
    StoreFileWriter writer = null;
    StoreFileWriter refFileWriter = null;
    Path filePath = null;
    Path refFilePath = null;
    long mobCells = 0;
    try {
      InetSocketAddress[] favoredNodes = null;
      if (rss != null && regionInfo.getEncodedName() != null) {
        favoredNodes = rss.getFavoredNodesForRegion(regionInfo.getEncodedName());
      }
      writer = MobUtils.createWriter(favoredNodes, conf, fs,
        column, partition.getPartitionId().getDate(), tempPath, Long.MAX_VALUE,
        column.getCompactionCompression(), partition.getPartitionId().getStartKey(),
        compactionCacheConfig, cryptoContext);
      filePath = writer.getPath();
      byte[] fileName = Bytes.toBytes(filePath.getName());
      // create a temp file and open a writer for it in the bulkloadPath
      refFileWriter = MobUtils.createRefFileWriter(conf, fs, column, bulkloadColumnPath, fileInfo
        .getSecond().longValue(), compactionCacheConfig, cryptoContext);
      refFilePath = refFileWriter.getPath();
      List<Cell> cells = new ArrayList<Cell>();
      boolean hasMore = false;
      ScannerContext scannerContext =
              ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();
      do {
        hasMore = scanner.next(cells, scannerContext);
        for (Cell cell : cells) {
          // write the mob cell to the mob file.
          writer.append(cell);
          // write the new reference cell to the store file.
          KeyValue reference = MobUtils.createMobRefKeyValue(cell, fileName, tableNameTag);
          refFileWriter.append(reference);
          mobCells++;
        }
        cells.clear();
      } while (hasMore);
    } finally {
      // close the scanner.
      scanner.close();
      // append metadata to the mob file, and close the mob file writer.
      closeMobFileWriter(writer, fileInfo.getFirst(), mobCells, regionInfo.getStartKey());
      // append metadata and bulkload info to the ref mob file, and close the writer.
      closeRefFileWriter(refFileWriter, fileInfo.getFirst(), request.selectionTime);
    }
    if (mobCells > 0) {
      // commit mob file
      MobUtils.commitFile(conf, fs, filePath, mobFamilyDir, compactionCacheConfig);
      // bulkload the ref file
      bulkloadRefFile(table, bulkloadPathOfPartition, filePath.getName());
      newFiles.add(new Path(mobFamilyDir, filePath.getName()));
    } else {
      // remove the new files
      // the mob file is empty, delete it instead of committing.
      deletePath(filePath);
      // the ref file is empty, delete it instead of committing.
      deletePath(refFilePath);
    }
    // archive the old mob files, do not archive the del files.
    try {
      closeStoreFileReaders(mobFilesToCompact);
      MobUtils
        .removeMobFiles(conf, fs, tableName, mobTableDir, column.getName(), mobFilesToCompact);
    } catch (IOException e) {
      LOG.error("Failed to archive the files " + mobFilesToCompact, e);
    }
  }

  /**
   * Creates a store scanner.
   * @param filesToCompact The files to be compacted.
   * @param scanType The scan type.
   * @return The store scanner.
   * @throws IOException
   */
  private StoreScanner createScanner(List<StoreFile> filesToCompact, ScanType scanType)
    throws IOException {
    List scanners = StoreFileScanner.getScannersForStoreFiles(filesToCompact, false, true, false,
      false, HConstants.LATEST_TIMESTAMP);
    Scan scan = new Scan();
    scan.setMaxVersions(column.getMaxVersions());
    long ttl = HStore.determineTTLFromFamily(column);
    ScanInfo scanInfo = new ScanInfo(conf, column, ttl, 0, CellComparator.COMPARATOR);
    StoreScanner scanner = new StoreScanner(scan, scanInfo, scanType, null, scanners, 0L,
      HConstants.LATEST_TIMESTAMP);
    return scanner;
  }

  /**
   * Bulkloads the current file.
   * @param table The current table.
   * @param bulkloadDirectory The path of bulkload directory.
   * @param fileName The current file name.
   * @throws IOException
   */
  private void bulkloadRefFile(Table table, Path bulkloadDirectory, String fileName)
    throws IOException {
    // bulkload the ref file
    try {
      LoadIncrementalHFiles bulkload = new LoadIncrementalHFiles(conf);
      bulkload.doBulkLoad(bulkloadDirectory, (HTable)table);
    } catch (Exception e) {
      // delete the committed mob file
      deletePath(new Path(mobFamilyDir, fileName));
      throw new IOException(e);
    } finally {
      // delete the bulkload files in bulkloadPath
      deletePath(bulkloadDirectory);
    }
  }

  /**
   * Closes the mob file writer.
   * @param writer The mob file writer.
   * @param maxSeqId Maximum sequence id.
   * @param mobCellsCount The number of mob cells.
   * @param startKey The start key of the region where the mob file comes from.
   * @throws IOException
   */
  private void closeMobFileWriter(StoreFileWriter writer, long maxSeqId, long mobCellsCount,
    byte[] startKey) throws IOException {
    if (writer != null) {
      writer.appendMetadata(maxSeqId, false, mobCellsCount, startKey);
      try {
        writer.close();
      } catch (IOException e) {
        LOG.error("Failed to close the writer of the file " + writer.getPath(), e);
      }
    }
  }

  /**
   * Closes the ref file writer.
   * @param writer The ref file writer.
   * @param maxSeqId Maximum sequence id.
   * @param bulkloadTime The timestamp at which the bulk load file is created.
   * @throws IOException
   */
  private void closeRefFileWriter(StoreFileWriter writer, long maxSeqId, long bulkloadTime)
    throws IOException {
    if (writer != null) {
      writer.appendMetadata(maxSeqId, false);
      writer.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY, Bytes.toBytes(bulkloadTime));
      writer.appendFileInfo(StoreFile.SKIP_RESET_SEQ_ID, Bytes.toBytes(true));
      try {
        writer.close();
      } catch (IOException e) {
        LOG.error("Failed to close the writer of the ref file " + writer.getPath(), e);
      }
    }
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

  /**
   * Gets if the given mob file is expired.
   * @param mobFileName The name of a mob file.
   * @param expiredDate The expired date.
   * @return True if the mob file is expired.
   */
  private boolean isExpiredMobFile(MobFileName mobFileName, Date expiredDate) {
    try {
      Date fileDate = MobUtils.parseDate(mobFileName.getDate());
      if (fileDate.getTime() < expiredDate.getTime()) {
        return true;
      }
    } catch (ParseException e) {
      // do nothing
    }
    return false;
  }
}
