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
import org.apache.hadoop.hbase.regionserver.StoreFile.Writer;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.Lists;

/**
 * The mob compaction thread used in {@link HMaster}
 */
public class MasterMobCompactionManager extends MasterProcedureManager implements Stoppable {

  static final Log LOG = LogFactory.getLog(MasterMobCompactionManager.class);
  private HMaster master;
  private Configuration conf;
  private ThreadPoolExecutor mobCompactionPool;
  private int delFileMaxCount;
  private int compactionBatchSize;
  protected int compactionKVMax;
  private Path tempPath;
  private CacheConfig compactionCacheConfig;
  private String compactionBaseZNode;
  private boolean stopped;
  private FileSystem fs;
  public static final String MOB_COMPACTION_ZNODE_NAME = "mobCompaction";
  public static final String MOB_COMPACTION_PROCEDURE_COLUMN_KEY = "mobCompaction-column";
  public static final String MOB_COMPACTION_PROCEDURE_ALL_FILES_KEY = "mobCompaction-allFiles";

  public static final String MOB_COMPACTION_PROCEDURE_SIGNATURE = "mob-compaction-proc";

  private static final String MOB_COMPACTION_TIMEOUT_MILLIS_KEY =
    "hbase.mob.compaction.master.timeoutMillis";
  private static final int MOB_COMPACTION_TIMEOUT_MILLIS_DEFAULT = 1800000; // 30 min
  private static final String MOB_COMPACTION_WAKE_MILLIS_KEY =
    "hbase.mob.compaction.master.wakeMillis";
  private static final int MOB_COMPACTION_WAKE_MILLIS_DEFAULT = 500;

  private static final String MOB_COMPACTION_PROC_POOL_THREADS_KEY =
    "hbase.mob.compaction.procedure.master.threads";
  private static final int MOB_COMPACTION_PROC_POOL_THREADS_DEFAULT = 2;

  private ProcedureCoordinator coordinator;
  private Map<TableName, Future<Void>> compactions = new HashMap<TableName, Future<Void>>();

  public MasterMobCompactionManager(HMaster master) {
    this(master, null);
  }

  public MasterMobCompactionManager(HMaster master, ThreadPoolExecutor mobCompactionPool) {
    this.master = master;
    this.fs = master.getFileSystem();
    this.conf = master.getConfiguration();
    delFileMaxCount = conf.getInt(MobConstants.MOB_DELFILE_MAX_COUNT,
      MobConstants.DEFAULT_MOB_DELFILE_MAX_COUNT);
    compactionBatchSize = conf.getInt(MobConstants.MOB_COMPACTION_BATCH_SIZE,
      MobConstants.DEFAULT_MOB_COMPACTION_BATCH_SIZE);
    compactionKVMax = this.conf.getInt(HConstants.COMPACTION_KV_MAX,
      HConstants.COMPACTION_KV_MAX_DEFAULT);
    tempPath = new Path(MobUtils.getMobHome(conf), MobConstants.TEMP_DIR_NAME);
    Configuration copyOfConf = new Configuration(conf);
    copyOfConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0f);
    compactionCacheConfig = new CacheConfig(copyOfConf);
    compactionBaseZNode = ZKUtil.joinZNode(master.getZooKeeper().getBaseZNode(),
      MOB_COMPACTION_ZNODE_NAME);
    // this pool is used to run the mob compaction
    if (mobCompactionPool != null) {
      this.mobCompactionPool = mobCompactionPool;
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
      Future<Void> future = mobCompactionPool.submit(new CompactionRunner(fs, tableName, columns,
        master.getTableLockManager(), allFiles));
      compactions.put(tableName, future);
      if (LOG.isDebugEnabled()) {
        LOG.debug("The mob compaction is requested for the columns " + columns + " of the table "
          + tableName.getNameAsString());
      }
      return future;
    }
  }

  /**
   * A callable for mob compaction
   */
  private class CompactionRunner implements Callable<Void> {
    private FileSystem fs;
    private TableName tableName;
    private List<HColumnDescriptor> columns;
    private TableLockManager tableLockManager;
    private boolean allFiles;

    public CompactionRunner(FileSystem fs, TableName tableName, List<HColumnDescriptor> columns,
      TableLockManager tableLockManager, boolean allFiles) {
      super();
      this.fs = fs;
      this.tableName = tableName;
      this.columns = columns;
      this.tableLockManager = tableLockManager;
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
            "Run MobCompactor");
          lock.acquire();
        }
        tableLocked = true;
        for (HColumnDescriptor column : columns) {
          doCompaction(conf, fs, tableName, column, allFiles);
        }
      } catch (IOException e) {
        LOG.error("Failed to perform the mob compaction", e);
      } finally {
        // release lock
        if (lock != null && tableLocked) {
          try {
            lock.release();
          } catch (IOException e) {
            LOG.error(
              "Failed to release the write lock for the table " + tableName.getNameAsString(), e);
          }
        }
        // remove this compaction from memory.
        synchronized (MasterMobCompactionManager.this) {
          compactions.remove(tableName.getNameAsString());
        }
        try {
          master.reportMobCompactionEnd(tableName);
        } catch (IOException e) {
          LOG.error("Failed to mark end of mob compation", e);
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
     */
    private void doCompaction(Configuration conf, FileSystem fs, TableName tableName,
      HColumnDescriptor column, boolean allFiles) {
      try {
        // merge del files
        Encryption.Context cryptoContext = MobUtils.createEncryptionContext(conf, column);
        Path mobTableDir = FSUtils.getTableDir(MobUtils.getMobHome(conf), tableName);
        Path mobFamilyDir = MobUtils.getMobFamilyPath(conf, tableName, column.getNameAsString());
        List<Path> delFilePaths = compactDelFiles(column, selectDelFiles(mobFamilyDir),
          EnvironmentEdgeManager.currentTime(), cryptoContext, mobTableDir, mobFamilyDir);
        // dispatch mob compaction request to region servers.
        boolean isAllFiles = dispatchMobCompaction(tableName, column, allFiles);
        // delete the del files if it is a major compaction
        if (isAllFiles && !delFilePaths.isEmpty()) {
          LOG.info("After a mob compaction with all files selected, archiving the del files "
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
      } catch (Exception e) {
        LOG.error("Failed to compact the mob files for the column " + column.getNameAsString()
          + " in the table " + tableName.getNameAsString(), e);
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
      // use procedure to dispatch the compaction to region servers.
      // Find all the online regions
      boolean allRegionsOnline = true;
      List<Pair<HRegionInfo, ServerName>> regionsAndLocations = MetaTableAccessor
        .getTableRegionsAndLocations(master.getConnection(), tableName, false);
      Map<String, List<String>> regionServers = new HashMap<String, List<String>>();
      for (Pair<HRegionInfo, ServerName> region : regionsAndLocations) {
        if (region != null && region.getFirst() != null && region.getSecond() != null) {
          HRegionInfo hri = region.getFirst();
          if (hri.isOffline() && (hri.isSplit() || hri.isSplitParent())) {
            allRegionsOnline = false;
            continue;
          }
          String serverName = region.getSecond().toString();
          List<String> regionNames = regionServers.get(serverName);
          if (regionNames != null) {
            regionNames.add(MD5Hash.getMD5AsHex(hri.getStartKey()));
          } else {
            regionNames = new ArrayList<String>();
            regionNames.add(MD5Hash.getMD5AsHex(hri.getStartKey()));
            regionServers.put(serverName, regionNames);
          }
        }
      }
      boolean success = false;
      if (allRegionsOnline && !regionServers.isEmpty()) {
        // add the map to zookeeper
        String compactionZNode = ZKUtil.joinZNode(compactionBaseZNode, tableName.getNameAsString());
        try {
          // clean the node if it exists
          ZKUtil.deleteNodeRecursively(master.getZooKeeper(), compactionZNode);
          ZKUtil.createEphemeralNodeAndWatch(master.getZooKeeper(), compactionZNode, null);
          for (Entry<String, List<String>> entry : regionServers.entrySet()) {
            String serverName = entry.getKey();
            List<String> startKeysOfOnlineRegions = entry.getValue();
            String compactionServerZNode = ZKUtil.joinZNode(compactionZNode, serverName);
            ZKUtil.createEphemeralNodeAndWatch(master.getZooKeeper(), compactionServerZNode,
              Bytes.toBytes(false));
            for (String startKeyOfOnlineRegion : startKeysOfOnlineRegions) {
              String compactionStartKeyZNode = ZKUtil.joinZNode(compactionServerZNode,
                startKeyOfOnlineRegion);
              ZKUtil.createEphemeralNodeAndWatch(master.getZooKeeper(), compactionStartKeyZNode,
                null);
            }
          }
        } catch (KeeperException e) {
          throw new IOException(e);
        }
      }
      // start the procedure
      ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher(
        tableName.getNameAsString());
      // the format of data is allFile(one byte) + columnName
      byte[] data = new byte[1 + column.getNameAsString().length()];
      data[0] = (byte) (allFiles ? 1 : 0);
      Bytes.putBytes(data, 1, column.getName(), 0, column.getName().length);
      Procedure proc = coordinator.startProcedure(monitor, tableName.getNameAsString(), data,
        Lists.newArrayList(regionServers.keySet()));
      if (proc == null) {
        String msg = "Failed to submit distributed procedure for mob compaction '"
          + tableName.getNameAsString() + "'";
        LOG.error(msg);
        throw new IOException(msg);
      }
      try {
        // wait for the mob compaction to complete.
        proc.waitForCompleted();
        LOG.info("Done waiting - mob compaction for " + tableName.getNameAsString());
        if (allRegionsOnline && !regionServers.isEmpty()) {
          // check if all the mob compactions is all files
          String compactionZNode = ZKUtil.joinZNode(compactionBaseZNode,
            tableName.getNameAsString());
          try {
            for (Entry<String, List<String>> entry : regionServers.entrySet()) {
              String serverName = entry.getKey();
              String compactionServerZNode = ZKUtil.joinZNode(compactionZNode, serverName);
              byte[] result = ZKUtil.getData(master.getZooKeeper(), compactionServerZNode);
              success = result != null && result.length == 1 && result[0] == 1;
            }
          } catch (KeeperException e) {
            LOG.warn("Exceptions happen after mob compaction", e);
            success = false;
          }
        }
      } catch (InterruptedException e) {
        ForeignException ee = new ForeignException(
          "Interrupted while waiting for snapshot to finish", e);
        monitor.receive(ee);
        Thread.currentThread().interrupt();
      } catch (ForeignException e) {
        monitor.receive(e);
      }
      // return if all the compactions are finished successfully
      return allRegionsOnline && success;
    }

    /**
     * Selects all the del files.
     * @param mobFamilyDir The directory where the compacted mob files are stored.
     * @return The paths of selected del files.
     * @throws IOException
     */
    private List<Path> selectDelFiles(Path mobFamilyDir) throws IOException {
      // since all the del files are used in mob compaction, they have to be merged in one time
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
        LOG.warn("The file " + path + " can not be found", e);
      }
      return null;
    }

    /**
     * Compacts the del files in batches which avoids opening too many files.
     * @param request The compaction request.
     * @param delFilePaths
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
      // when there are more del files than the number that is allowed, merge it firstly.
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
     * @param request The compaction request.
     * @param delFiles The del files.
     * @return The path of new del file after merging.
     * @throws IOException
     */
    private Path compactDelFilesInBatch(HColumnDescriptor column, List<StoreFile> delFiles,
      long selectionTime, Encryption.Context cryptoContext, Path mobTableDir, Path mobFamilyDir)
      throws IOException {
      // create a scanner for the del files.
      StoreScanner scanner = createScanner(column, delFiles, ScanType.COMPACT_RETAIN_DELETES);
      Writer writer = null;
      Path filePath = null;
      try {
        writer = MobUtils.createDelFileWriter(conf, fs, column,
          MobUtils.formatDate(new Date(selectionTime)), tempPath, Long.MAX_VALUE,
          column.getCompactionCompression(), HConstants.EMPTY_START_ROW, compactionCacheConfig,
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
     * Creates a store scanner.
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
      ScanInfo scanInfo = new ScanInfo(column, ttl, 0, CellComparator.COMPARATOR);
      StoreScanner scanner = new StoreScanner(scan, scanInfo, scanType, null, scanners, 0L,
        HConstants.LATEST_TIMESTAMP);
      return scanner;
    }
  }

  @Override
  public void stop(String why) {
    if (this.stopped)
      return;
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
