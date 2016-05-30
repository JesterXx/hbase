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
package org.apache.hadoop.hbase.master;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.procedure.MasterProcedureManager;
import org.apache.hadoop.hbase.procedure.Procedure;
import org.apache.hadoop.hbase.procedure.ProcedureCoordinator;
import org.apache.hadoop.hbase.procedure.ProcedureCoordinatorRpcs;
import org.apache.hadoop.hbase.procedure.ZKProcedureCoordinatorRpcs;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStore;
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
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.Lists;

/**
 * The mob compaction manager used in {@link HMaster}
 */
@InterfaceAudience.Private
public class MobCompactionManager extends MasterProcedureManager implements Stoppable {

  static final Log LOG = LogFactory.getLog(MobCompactionManager.class);
  private final HMaster master;
  private final ThreadPoolExecutor mobCompactionPool;
  private final int delFileMaxCount;
  private final int compactionBatchSize;
  protected final int compactionKVMax;
  private final Path tempPath;
  private final CacheConfig compactionCacheConfig;
  private boolean stopped;
  public static final String MOB_COMPACTION_PROCEDURE_COLUMN_KEY = "mobCompaction-column";
  public static final String MOB_COMPACTION_PROCEDURE_ALL_FILES_KEY = "mobCompaction-allFiles";

  public static final String MOB_COMPACTION_PROCEDURE_SIGNATURE = "mob-compaction-proc";

  private static final String MOB_COMPACTION_TIMEOUT_MILLIS_KEY =
    "hbase.master.mob.compaction.timeoutMillis";
  private static final int MOB_COMPACTION_TIMEOUT_MILLIS_DEFAULT = 1800000; // 30 min
  private static final String MOB_COMPACTION_WAKE_MILLIS_KEY =
    "hbase.master.mob.compaction.wakeMillis";
  private static final int MOB_COMPACTION_WAKE_MILLIS_DEFAULT = 500;

  private static final String MOB_COMPACTION_PROC_POOL_THREADS_KEY =
    "hbase.master.mob.compaction.procedure.threads";
  private static final int MOB_COMPACTION_PROC_POOL_THREADS_DEFAULT = 2;
  private boolean closePoolOnStop = true;

  private ProcedureCoordinator coordinator;
  private Map<TableName, Future<Void>> compactions = new HashMap<TableName, Future<Void>>();
  // This records the mappings of tables and mob compactions.
  // The value of this map is a mapping of server names and pairs of the execution information and
  // the related online regions.
  private Map<TableName, Map<ServerName, Pair<Boolean, List<byte[]>>>> compactingRegions =
    new HashMap<TableName, Map<ServerName, Pair<Boolean, List<byte[]>>>>();

  public MobCompactionManager(HMaster master) {
    this(master, null);
  }

  public MobCompactionManager(HMaster master, ThreadPoolExecutor mobCompactionPool) {
    this.master = master;
    Configuration conf = master.getConfiguration();
    delFileMaxCount = conf.getInt(MobConstants.MOB_DELFILE_MAX_COUNT,
      MobConstants.DEFAULT_MOB_DELFILE_MAX_COUNT);
    compactionBatchSize = conf.getInt(MobConstants.MOB_COMPACTION_BATCH_SIZE,
      MobConstants.DEFAULT_MOB_COMPACTION_BATCH_SIZE);
    compactionKVMax = conf.getInt(HConstants.COMPACTION_KV_MAX,
      HConstants.COMPACTION_KV_MAX_DEFAULT);
    tempPath = new Path(MobUtils.getMobHome(conf), MobConstants.TEMP_DIR_NAME);
    boolean cacheDataOnRead = conf.getBoolean(CacheConfig.CACHE_DATA_ON_READ_KEY,
      CacheConfig.DEFAULT_CACHE_DATA_ON_READ);
    Configuration configration = conf;
    if (cacheDataOnRead) {
      Configuration copyOfConf = new Configuration(conf);
      copyOfConf.setBoolean(CacheConfig.CACHE_DATA_ON_READ_KEY, Boolean.FALSE);
      configration = copyOfConf;
    }
    compactionCacheConfig = new CacheConfig(configration);
    // this pool is used to run the mob compaction
    if (mobCompactionPool != null) {
      this.mobCompactionPool = mobCompactionPool;
      closePoolOnStop = false;
    } else {
      int threads = conf.getInt(MOB_COMPACTION_PROC_POOL_THREADS_KEY,
        MOB_COMPACTION_PROC_POOL_THREADS_DEFAULT);
      this.mobCompactionPool = ProcedureCoordinator.defaultPool("MasterMobCompaction", threads);
    }
  }

  /**
   * Requests mob compaction
   * @param tableName The table the compact
   * @param columns The column descriptors
   * @param allFiles Whether add all mob files into the compaction.
   */
  public Future<Void> requestMobCompaction(TableName tableName, List<HColumnDescriptor> columns,
    boolean allFiles) throws IOException {
    // only compact enabled table
    if (!master.getAssignmentManager().getTableStateManager()
      .isTableState(tableName, TableState.State.ENABLED)) {
      LOG.warn("The table " + tableName + " is not enabled");
      throw new TableNotEnabledException(tableName);
    }
    synchronized (this) {
      // check if there is another mob compaction for the same table
      Future<Void> compaction = compactions.get(tableName);
      if (compaction != null && !compaction.isDone()) {
        String msg = "Another mob compaction on table " + tableName.getNameAsString()
          + " is in progress";
        LOG.error(msg);
        throw new IOException(msg);
      }
      Future<Void> future = mobCompactionPool.submit(new CompactionRunner(tableName, columns,
        allFiles));
      compactions.put(tableName, future);
      if (LOG.isDebugEnabled()) {
        LOG.debug("The mob compaction is requested for the columns " + columns + " of the table "
          + tableName.getNameAsString());
      }
      return future;
    }
  }

  /**
   * Gets the regions that run the mob compaction.
   * @param tableName The table to run the mob compaction.
   * @param serverName The server to run the mob compaction.
   * @return The start keys of regions that run the mob compaction.
   */
  public List<byte[]> getCompactingRegions(TableName tableName, ServerName serverName) {
    Map<ServerName, Pair<Boolean, List<byte[]>>> serverRegionMapping = compactingRegions
      .get(tableName);
    if (serverRegionMapping == null) {
      return Collections.emptyList();
    }
    Pair<Boolean, List<byte[]>> pair = serverRegionMapping.get(serverName);
    if (pair == null) {
      return Collections.emptyList();
    }
    return pair.getSecond();
  }

  /**
   * Updates the mob compaction in the given server as major.
   * @param tableName The table to run the mob compaction.
   * @param serverName The server to run the mob compaction.
   */
  public void updateAsMajorCompaction(TableName tableName, ServerName serverName) {
    Map<ServerName, Pair<Boolean, List<byte[]>>> serverRegionMapping = compactingRegions
      .get(tableName);
    if (serverRegionMapping == null) {
      return;
    }
    Pair<Boolean, List<byte[]>> pair = serverRegionMapping.get(serverName);
    if (pair == null) {
      return;
    }
    // mark it as true which means the mob compaction in the given server is major.
    pair.setFirst(Boolean.TRUE);
  }

  /**
   * A callable for mob compaction
   */
  private class CompactionRunner implements Callable<Void> {
    private final Configuration conf;
    private final FileSystem fs;
    private final TableName tableName;
    private final List<HColumnDescriptor> columns;
    private final TableLockManager tableLockManager;
    private final boolean allFiles;

    public CompactionRunner(TableName tableName, List<HColumnDescriptor> columns,
      boolean allFiles) {
      this.conf = master.getConfiguration();
      this.fs = master.getFileSystem();
      this.tableName = tableName;
      this.columns = columns;
      this.tableLockManager = master.getTableLockManager();
      this.allFiles = allFiles;
    }

    @Override
    public Void call() throws Exception{
      boolean tableLocked = false;
      TableLock lock = null;
      try {
        master.reportMobCompactionStart(tableName);
        // acquire lock to sync with major compaction
        // the tableLockManager might be null in testing. In that case, it is lock-free.
        if (tableLockManager != null) {
          lock = tableLockManager.writeLock(MobUtils.getTableLockName(tableName),
            "Run MOB Compaction");
          lock.acquire();
        }
        tableLocked = true;
        StringBuilder errorMsg = null;
        for (HColumnDescriptor column : columns) {
          try {
            doCompaction(conf, fs, tableName, column, allFiles);
          } catch (IOException e) {
            LOG.error("Failed to compact the mob files for the column " + column.getNameAsString()
              + " in the table " + tableName.getNameAsString(), e);
            if (errorMsg == null) {
              errorMsg = new StringBuilder();
            }
            errorMsg.append(column.getNameAsString() + " ");
          }
        }
        if (errorMsg != null) {
          throw new IOException("Failed to compact the mob files for the columns "
            + errorMsg.toString() + " in the table " + tableName.getNameAsString());
        }
      } catch (IOException e) {
        LOG.error(
          "Failed to perform the mob compaction for the table " + tableName.getNameAsString(), e);
        throw e;
      } finally {
        // release lock
        if (lock != null && tableLocked) {
          try {
            lock.release();
          } catch (IOException e) {
            LOG.error("Failed to release the write lock of mob compaction for the table "
              + tableName.getNameAsString(), e);
          }
        }
        // remove this compaction from memory.
        synchronized (MobCompactionManager.this) {
          compactions.remove(tableName);
        }
        try {
          master.reportMobCompactionEnd(tableName);
        } catch (IOException e) {
          LOG.error(
            "Failed to mark end of mob compation for the table " + tableName.getNameAsString(), e);
        }
      }
      return null;
    }

    /**
     * Performs mob compaction for a column.
     * @param conf The current configuration.
     * @param fs The current file system.
     * @param tableName The current table name.
     * @param column The current column descriptor.
     * @param allFiles If a major compaction is required.
     * @throws IOException
     */
    private void doCompaction(Configuration conf, FileSystem fs, TableName tableName,
      HColumnDescriptor column, boolean allFiles) throws IOException {
      // merge del files
      Encryption.Context cryptoContext = EncryptionUtil.createEncryptionContext(conf, column);
      Path mobTableDir = FSUtils.getTableDir(MobUtils.getMobHome(conf), tableName);
      Path mobFamilyDir = MobUtils.getMobFamilyPath(conf, tableName, column.getNameAsString());
      List<Path> delFilePaths = compactDelFiles(column, selectDelFiles(mobFamilyDir),
        EnvironmentEdgeManager.currentTime(), cryptoContext, mobTableDir, mobFamilyDir);
      // dispatch mob compaction request to region servers.
      boolean archiveDelFiles = dispatchMobCompaction(tableName, column, allFiles);
      // archive the del files if it is a major compaction
      if (archiveDelFiles && !delFilePaths.isEmpty()) {
        LOG.info("After a mob compaction with all files selected, archive the del files "
          + delFilePaths);
        List<StoreFile> delFiles = new ArrayList<StoreFile>();
        for (Path delFilePath : delFilePaths) {
          StoreFile sf = new StoreFile(fs, delFilePath, conf, compactionCacheConfig,
            BloomType.NONE);
          delFiles.add(sf);
        }
        try {
          MobUtils.removeMobFiles(conf, fs, tableName, mobTableDir, column.getName(), delFiles);
        } catch (IOException e) {
          LOG.error("Failed to archive the del files " + delFilePaths, e);
        }
      }
    }

    /**
     * Submits the procedure to region servers.
     * @param tableName The current table name.
     * @param column The current column descriptor.
     * @param allFiles If a major compaction is required.
     * @return True if all the mob files are compacted.
     * @throws IOException
     */
    private boolean dispatchMobCompaction(TableName tableName, HColumnDescriptor column,
      boolean allFiles) throws IOException {
      // Only all the regions are online when the procedure runs in region
      // servers, the compaction is considered as a major one.
      // Use procedure to dispatch the compaction to region servers.
      // Find all the online regions
      boolean allRegionsOnline = true;
      List<Pair<HRegionInfo, ServerName>> regionsAndLocations = MetaTableAccessor
        .getTableRegionsAndLocations(master.getConnection(), tableName, false);
      Map<String, List<byte[]>> regionServers = new HashMap<String, List<byte[]>>();
      for (Pair<HRegionInfo, ServerName> region : regionsAndLocations) {
        if (region != null && region.getFirst() != null && region.getSecond() != null) {
          HRegionInfo hri = region.getFirst();
          if (hri.isOffline()) {
            allRegionsOnline = false;
            continue;
          }
          String serverName = region.getSecond().toString();
          List<byte[]> regionNames = regionServers.get(serverName);
          if (regionNames != null) {
            regionNames.add(hri.getStartKey());
          } else {
            regionNames = new ArrayList<byte[]>();
            regionNames.add(hri.getStartKey());
            regionServers.put(serverName, regionNames);
          }
        }
      }
      boolean archiveDelFiles = false;
      Map<ServerName, Pair<Boolean, List<byte[]>>> serverRegionMapping = Collections.emptyMap();
      if (allRegionsOnline && !regionServers.isEmpty()) {
        // record the online regions of each region server
        serverRegionMapping = new HashMap<ServerName, Pair<Boolean, List<byte[]>>>();
        compactingRegions.put(tableName, serverRegionMapping);
        for (Entry<String, List<byte[]>> entry : regionServers.entrySet()) {
          String serverNameAsString = entry.getKey();
          ServerName serverName = ServerName.valueOf(serverNameAsString);
          List<byte[]> startKeysOfOnlineRegions = entry.getValue();
          Pair<Boolean, List<byte[]>> pair = new Pair<Boolean, List<byte[]>>();
          serverRegionMapping.put(serverName, pair);
          pair.setFirst(Boolean.FALSE);
          pair.setSecond(startKeysOfOnlineRegions);
        }
      }
      // start the procedure
      String procedureName = MobConstants.MOB_COMPACTION_PREFIX + tableName.getNameAsString();
      ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher(procedureName);
      // the format of data is allFile(one byte) + columnName
      byte[] data = new byte[2 + column.getNameAsString().length()];
      data[0] = (byte) (allFiles ? 1 : 0);
      data[1] = (byte) (allRegionsOnline ? 1 : 0);
      Bytes.putBytes(data, 2, column.getName(), 0, column.getName().length);
      Procedure proc = coordinator.startProcedure(monitor, procedureName, data,
        Lists.newArrayList(regionServers.keySet()));
      if (proc == null) {
        String msg = "Failed to submit distributed procedure for mob compaction '"
          + procedureName + "'";
        LOG.error(msg);
        throw new IOException(msg);
      }
      try {
        // wait for the mob compaction to complete.
        proc.waitForCompleted();
        LOG.info("Done waiting - mob compaction for " + procedureName);
        if (allRegionsOnline && !serverRegionMapping.isEmpty()) {
          // check if all the files are selected in compaction of all region servers.
          for (Entry<ServerName, Pair<Boolean, List<byte[]>>> entry : serverRegionMapping
            .entrySet()) {
            boolean major = entry.getValue().getFirst();
            LOG.info("Mob compaction " + procedureName + " in server " + entry.getKey() + " is "
              + (major ? "major" : "minor"));
            if (major) {
              archiveDelFiles = true;
            } else {
              archiveDelFiles = false;
              break;
            }
          }
        }
      } catch (InterruptedException e) {
        ForeignException ee = new ForeignException(
          "Interrupted while waiting for snapshot to finish", e);
        monitor.receive(ee);
        Thread.currentThread().interrupt();
      } catch (ForeignException e) {
        monitor.receive(e);
      } finally {
        // clean up
        compactingRegions.remove(tableName);
      }
      // return true if all the compactions are finished successfully and all files are selected
      // in all region servers.
      return allRegionsOnline && archiveDelFiles;
    }

    /**
     * Selects all the del files.
     * @param mobFamilyDir The directory where the compacted mob files are stored.
     * @return The paths of selected del files.
     * @throws IOException
     */
    private List<Path> selectDelFiles(Path mobFamilyDir) throws IOException {
      // since all the del files are used in mob compaction, they have to be merged one time
      // together.
      List<Path> allDelFiles = new ArrayList<Path>();
      for (FileStatus file : fs.listStatus(mobFamilyDir)) {
        if (!file.isFile()) {
          continue;
        }
        // group the del files and small files.
        Path originalPath = file.getPath();
        if (HFileLink.isHFileLink(file.getPath())) {
          HFileLink link = HFileLink.buildFromHFileLinkPattern(conf, file.getPath());
          FileStatus linkedFile = getLinkedFileStatus(link);
          if (linkedFile == null) {
            continue;
          }
          originalPath = link.getOriginPath();
        }
        if (StoreFileInfo.isDelFile(originalPath)) {
          allDelFiles.add(file.getPath());
        }
      }
      return allDelFiles;
    }

    private FileStatus getLinkedFileStatus(HFileLink link) throws IOException {
      Path[] locations = link.getLocations();
      for (Path location : locations) {
        FileStatus file = getFileStatus(location);
        if (file != null) {
          return file;
        }
      }
      return null;
    }

    private FileStatus getFileStatus(Path path) throws IOException {
      try {
        if (path != null) {
          FileStatus file = fs.getFileStatus(path);
          return file;
        }
      } catch (FileNotFoundException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("The file " + path + " can not be found", e);
        }
      }
      return null;
    }

    /**
     * Compacts the del files in batches which avoids opening too many files.
     * @param column The column family that the MOB compaction runs in.
     * @param delFilePaths The paths of the del files.
     * @param selectionTime The time when the compaction of del files starts
     * @param cryptoContext The current encryption context.
     * @param mobTableDir The directory of the MOB table.
     * @param mobFamilyDir The directory of MOB column family.
     * @return The paths of new del files after merging or the original files if no merging
     *         is necessary.
     * @throws IOException
     */
    private List<Path> compactDelFiles(HColumnDescriptor column, List<Path> delFilePaths,
      long selectionTime, Encryption.Context cryptoContext, Path mobTableDir, Path mobFamilyDir)
      throws IOException {
      if (delFilePaths.size() <= delFileMaxCount) {
        return delFilePaths;
      }
      // when there are more del files than the number that is allowed, merge them firstly.
      int offset = 0;
      List<Path> paths = new ArrayList<Path>();
      while (offset < delFilePaths.size()) {
        // get the batch
        int batch = compactionBatchSize;
        if (delFilePaths.size() - offset < compactionBatchSize) {
          batch = delFilePaths.size() - offset;
        }
        List<StoreFile> batchedDelFiles = new ArrayList<StoreFile>();
        if (batch == 1) {
          // only one file left, do not compact it, directly add it to the new files.
          paths.add(delFilePaths.get(offset));
          offset++;
          continue;
        }
        for (int i = offset; i < batch + offset; i++) {
          batchedDelFiles.add(new StoreFile(fs, delFilePaths.get(i), conf, compactionCacheConfig,
            BloomType.NONE));
        }
        // compact the del files in a batch.
        paths.add(compactDelFilesInBatch(column, batchedDelFiles, selectionTime, cryptoContext,
          mobTableDir, mobFamilyDir));
        // move to the next batch.
        offset += batch;
      }
      return compactDelFiles(column, paths, selectionTime, cryptoContext, mobTableDir, mobFamilyDir);
    }

    /**
     * Compacts the del file in a batch.
     * @param column The column family that the MOB compaction runs in.
     * @param delFiles The del files.
     * @param selectionTime The time when the compaction of del files starts
     * @param cryptoContext The current encryption context.
     * @param mobTableDir The directory of the MOB table.
     * @param mobFamilyDir The directory of MOB column family.
     * @return The path of new del file after merging.
     * @throws IOException
     */
    private Path compactDelFilesInBatch(HColumnDescriptor column, List<StoreFile> delFiles,
      long selectionTime, Encryption.Context cryptoContext, Path mobTableDir, Path mobFamilyDir)
      throws IOException {
      StoreFileWriter writer = null;
      Path filePath = null;
      // create a scanner for the del files.
      try (StoreScanner scanner =
        createScanner(column, delFiles, ScanType.COMPACT_RETAIN_DELETES)) {
        writer = MobUtils.createDelFileWriter(conf, fs, column,
          MobUtils.formatDate(new Date(selectionTime)), tempPath, Long.MAX_VALUE,
          column.getCompactionCompressionType(), HConstants.EMPTY_START_ROW, compactionCacheConfig,
          cryptoContext);
        filePath = writer.getPath();
        List<Cell> cells = new ArrayList<Cell>();
        boolean hasMore = false;
        ScannerContext scannerContext = ScannerContext.newBuilder().setBatchLimit(compactionKVMax)
          .build();
        do {
          hasMore = scanner.next(cells, scannerContext);
          for (Cell cell : cells) {
            writer.append(cell);
          }
          cells.clear();
        } while (hasMore);
      } finally {
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
        LOG.error(
          "Failed to archive the old del files " + delFiles + " in the column "
            + column.getNameAsString() + " of the table " + tableName.getNameAsString(), e);
      }
      return path;
    }

    /**
     * Creates a store scanner.
     * @param column The column family that that the MOB compaction runs in.
     * @param filesToCompact The files to be compacted.
     * @param scanType The scan type.
     * @return The store scanner.
     * @throws IOException
     */
    private StoreScanner createScanner(HColumnDescriptor column, List<StoreFile> filesToCompact,
      ScanType scanType) throws IOException {
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
  }

  @Override
  public void stop(String why) {
    if (this.stopped) {
      return;
    }
    this.stopped = true;
    for (Future<Void> compaction : compactions.values()) {
      if (compaction != null) {
        compaction.cancel(true);
      }
    }
    try {
      if (coordinator != null) {
        coordinator.close();
      }
    } catch (IOException e) {
      LOG.error("stop ProcedureCoordinator error", e);
    }
    if (closePoolOnStop) {
      if (this.mobCompactionPool != null) {
        mobCompactionPool.shutdown();
      }
    }
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  @Override
  public void initialize(MasterServices master, MetricsMaster metricsMaster)
    throws KeeperException, IOException, UnsupportedOperationException {
    // get the configuration for the coordinator
    Configuration conf = master.getConfiguration();
    long wakeFrequency = conf.getInt(MOB_COMPACTION_WAKE_MILLIS_KEY,
      MOB_COMPACTION_WAKE_MILLIS_DEFAULT);
    long timeoutMillis = conf.getLong(MOB_COMPACTION_TIMEOUT_MILLIS_KEY,
      MOB_COMPACTION_TIMEOUT_MILLIS_DEFAULT);

    // setup the procedure coordinator
    String name = master.getServerName().toString();
    ProcedureCoordinatorRpcs comms = new ZKProcedureCoordinatorRpcs(master.getZooKeeper(),
      getProcedureSignature(), name);

    this.coordinator = new ProcedureCoordinator(comms, mobCompactionPool, timeoutMillis,
      wakeFrequency);
  }

  @Override
  public boolean isProcedureDone(ProcedureDescription desc) throws IOException {
    TableName tableName = TableName.valueOf(desc.getInstance());
    Future<Void> compaction = compactions.get(tableName);
    if (compaction != null) {
      return compaction.isDone();
    }
    return true;
  }

  @Override
  public String getProcedureSignature() {
    return MOB_COMPACTION_PROCEDURE_SIGNATURE;
  }

  @Override
  public void execProcedure(ProcedureDescription desc) throws IOException {
    TableName tableName = TableName.valueOf(desc.getInstance());
    List<NameStringPair> props = desc.getConfigurationList();
    List<String> columnNames = new ArrayList<String>();
    boolean allFiles = false;
    for (NameStringPair prop : props) {
      if (MOB_COMPACTION_PROCEDURE_COLUMN_KEY.equalsIgnoreCase(prop.getName())) {
        columnNames.add(prop.getValue());
      } else if (MOB_COMPACTION_PROCEDURE_ALL_FILES_KEY.equalsIgnoreCase(prop.getName())) {
        allFiles = "true".equalsIgnoreCase(prop.getValue());
      }
    }
    HTableDescriptor htd = master.getTableDescriptors().get(tableName);
    List<HColumnDescriptor> compactedColumns = new ArrayList<HColumnDescriptor>();
    if (!columnNames.isEmpty()) {
      for (String columnName : columnNames) {
        HColumnDescriptor column = htd.getFamily(Bytes.toBytes(columnName));
        if (column == null) {
          LOG.error("Column family " + columnName + " does not exist");
          throw new DoNotRetryIOException("Column family " + columnName + " does not exist");
        }
        if (!column.isMobEnabled()) {
          String msg = "Column family " + column.getNameAsString() + " is not a mob column family";
          LOG.error(msg);
          throw new DoNotRetryIOException(msg);
        }
        compactedColumns.add(column);
      }
    } else {
      for (HColumnDescriptor column : htd.getColumnFamilies()) {
        if (column.isMobEnabled()) {
          compactedColumns.add(column);
        }
      }
    }
    requestMobCompaction(tableName, compactedColumns, allFiles);
  }
}
