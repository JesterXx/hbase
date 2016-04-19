/**
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.mob.MobFileName;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.mob.compactions.MobCompactionRequest.CompactionType;
import org.apache.hadoop.hbase.mob.compactions.RegionServerMobCompactionProcedureManager.MobCompactionSubprocedurePool;
import org.apache.hadoop.hbase.procedure.ProcedureMember;
import org.apache.hadoop.hbase.procedure.Subprocedure;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterMobCompactionStatusProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import com.google.protobuf.ServiceException;

/**
 * The subprocedure implementation for mob compaction.
 * The mob compaction is distributed to region servers, and executed in subprocedure
 * in each region server.
 */
public class MobCompactionSubprocedure extends Subprocedure {
  private static final Log LOG = LogFactory.getLog(MobCompactionSubprocedure.class);

  private final Configuration conf;
  private final String procName;
  private final TableName tableName;
  private final String columnName;
  private final RegionServerServices rss;
  private final List<Region> regions;
  private final MobCompactionSubprocedurePool taskManager;
  private boolean allFiles;
  private boolean allRegionsOnline;
  private Path mobFamilyDir;
  private CacheConfig cacheConfig;

  public MobCompactionSubprocedure(ProcedureMember member, String procName,
    ForeignExceptionDispatcher errorListener, long wakeFrequency, long timeout,
    RegionServerServices rss, List<Region> regions, TableName tableName, String columnName,
    MobCompactionSubprocedurePool taskManager, boolean allFiles, boolean allRegionsOnline) {
    super(member, procName, errorListener, wakeFrequency, timeout);
    this.procName = procName;
    this.tableName = tableName;
    this.columnName = columnName;
    this.rss = rss;
    this.regions = regions;
    this.taskManager = taskManager;
    this.allFiles = allFiles;
    this.allRegionsOnline = allRegionsOnline;
    this.conf = rss.getConfiguration();
    mobFamilyDir = MobUtils.getMobFamilyPath(conf, tableName, columnName);
    Configuration copyOfConf = new Configuration(conf);
    copyOfConf.setBoolean(CacheConfig.PREFETCH_BLOCKS_ON_OPEN_KEY, false);
    this.cacheConfig = new CacheConfig(copyOfConf);
  }

  /**
   * Compacts mob files in the current region server.
   */
  @Override
  public void acquireBarrier() throws ForeignException {
    if (regions.isEmpty()) {
      // No regions on this RS, we are basically done.
      return;
    }
    List<FileStatus> files = null;
    try {
      files = Arrays.asList(rss.getFileSystem().listStatus(mobFamilyDir));
    } catch (IOException e) {
      throw new ForeignException(getMemberName(), e);
    }
    if (files.isEmpty()) {
      return;
    }
    monitor.rethrowException();
    Map<String, byte[]> prefixAndKeys = new HashMap<String, byte[]>();
    // find the mapping from file prefix to startKey
    for (FileStatus file : files) {
      Path path = file.getPath();
      if (HFileLink.isHFileLink(path)) {
        HFileLink link;
        try {
          link = HFileLink.buildFromHFileLinkPattern(conf, path);
          FileStatus linkedFile = MobUtils.getReferencedFileStatus(rss.getFileSystem(), link);
          path = linkedFile.getPath();
        } catch (IOException e) {
          throw new ForeignException(getMemberName(), e);
        }
      }
      String prefix = MobFileName.create(path.getName()).getStartKey();
      if (prefixAndKeys.get(prefix) == null) {
        StoreFile sf = null;
        try {
          sf = new StoreFile(rss.getFileSystem(), path, conf, cacheConfig, BloomType.NONE);
          Reader reader = sf.createReader().getHFileReader();
          Map<byte[], byte[]> fileInfo = reader.loadFileInfo();
          byte[] startKey = fileInfo.get(StoreFile.MOB_REGION_STARTKEY);
          if (startKey == null) {
            // use the key of the first cell as the start key of a region where the mob file
            // comes from.
            startKey = reader.getFirstRowKey();
            if (startKey == null) {
              startKey = HConstants.EMPTY_START_ROW;
            }
          }
          prefixAndKeys.put(prefix, startKey);
        } catch (IOException e) {
          throw new ForeignException(getMemberName(), e);
        } finally {
          if (sf != null) {
            try {
              sf.closeReader(false);
            } catch (IOException e) {
              LOG.warn("Failed to close the store file " + path, e);
            }
          }
        }
      }
    }

    List<byte[]> sortedStartKeys = new ArrayList<byte[]>(regions.size());
    for (Region region : regions) {
      // submit one task per region for parallelize by region.
      taskManager.submitTask(new RegionMobCompactionTask(region, files, prefixAndKeys));
      sortedStartKeys.add(region.getRegionInfo().getStartKey());
      monitor.rethrowException();
    }

    // wait for everything to complete.
    boolean success = false;
    LOG.debug("Mob compaction tasks submitted for " + regions.size() + " regions");
    try {
      success = taskManager.waitForOutstandingTasks();
      LOG.info("Mob compaction tasks for region server " + rss.getServerName() + " are finished["
        + success + "]");
    } catch (InterruptedException e) {
      throw new ForeignException(getMemberName(), e);
    }
    // add nodes to zookeeper if all the tasks are finished successfully
    if (allRegionsOnline && success && !regions.isEmpty()) {
      // compare the regions passed from master and existing regions in the current region server.
      // if they are the same, it means all regions are online, all mob files owned by this region
      // server can be compacted. We call tell master this thing by setting data in zookeeper.
      try {
        List<String> foundRegionStartKeys = getCompactRegions();
        if (foundRegionStartKeys.size() == sortedStartKeys.size()) {
          Collections.sort(sortedStartKeys, Bytes.BYTES_COMPARATOR);
          List<String> onlineRegionStartKeys = new ArrayList<String>(sortedStartKeys.size());
          for (byte[] startKey : sortedStartKeys) {
            onlineRegionStartKeys.add(MD5Hash.getMD5AsHex(startKey));
          }
          boolean equals = true;
          for (int i = 0; i < foundRegionStartKeys.size(); i++) {
            if (!foundRegionStartKeys.get(i).equals(onlineRegionStartKeys.get(i))) {
              equals = false;
              break;
            }
          }
          if (equals) {
            updateCompactionAsMajor();
          }
        }
      } catch (ServiceException e) {
        throw new ForeignException(getMemberName(), e);
      } catch (IOException e) {
        throw new ForeignException(getMemberName(), e);
      }
    }
  }

  /**
   * Gets the regions that run the mob compaction.
   * @return The MD5 of start keys of regions that run the mob compaction.
   */
  private List<String> getCompactRegions() throws ServiceException, IOException {
    MasterMobCompactionStatusProtos.GetMobCompactRegionsRequest request =
      MasterMobCompactionStatusProtos.GetMobCompactRegionsRequest
      .newBuilder().setServerName(rss.getServerName().getServerName())
      .setTableName(tableName.getNameAsString()).build();
    ClientProtos.CoprocessorServiceCall call = ClientProtos.CoprocessorServiceCall
      .newBuilder()
      .setRow(ByteStringer.wrap(HConstants.EMPTY_BYTE_ARRAY))
      .setServiceName(
        MasterMobCompactionStatusProtos.MasterMobCompactionStatusService.getDescriptor()
          .getFullName())
      .setMethodName(
        MasterMobCompactionStatusProtos.MasterMobCompactionStatusService.getDescriptor()
          .getMethods().get(0).getName()).setRequest(request.toByteString()).build();
    CoprocessorServiceResponse servieResponse = ProtobufUtil.execService(null, rss
      .getClusterConnection().getMaster(), call);
    MasterMobCompactionStatusProtos.GetMobCompactRegionsResponse response =
      MasterMobCompactionStatusProtos.GetMobCompactRegionsResponse
      .parseFrom(servieResponse.getValue().getValue());
    return response.getRegionStartKeyList();
  }

  /**
   * Updates the mob compaction as major in the current server.
   */
  private void updateCompactionAsMajor() throws ServiceException, IOException {
    MasterMobCompactionStatusProtos.UpdateMobCompactionAsMajorRequest request =
      MasterMobCompactionStatusProtos.UpdateMobCompactionAsMajorRequest
      .newBuilder().setServerName(rss.getServerName().getServerName())
      .setTableName(tableName.getNameAsString()).build();
    ClientProtos.CoprocessorServiceCall call = ClientProtos.CoprocessorServiceCall
      .newBuilder()
      .setRow(ByteStringer.wrap(HConstants.EMPTY_BYTE_ARRAY))
      .setServiceName(
        MasterMobCompactionStatusProtos.MasterMobCompactionStatusService.getDescriptor()
          .getFullName())
      .setMethodName(
        MasterMobCompactionStatusProtos.MasterMobCompactionStatusService.getDescriptor()
          .getMethods().get(1).getName()).setRequest(request.toByteString()).build();
    ProtobufUtil.execService(null, rss.getClusterConnection().getMaster(), call);
  }

  // Callable for mob compaction.
  private class RegionMobCompactionTask implements Callable<Boolean> {
    Region region;
    List<FileStatus> files;
    Map<String, byte[]> prefixAndKeys;

    RegionMobCompactionTask(Region region, List<FileStatus> files, Map<String, byte[]> prefixAndKeys) {
      this.region = region;
      this.files = files;
      this.prefixAndKeys = prefixAndKeys;
    }

    @Override
    public Boolean call() throws Exception {
      LOG.debug("Starting region operation mob compaction on " + region);
      region.startRegionOperation();
      try {
        LOG.debug("Mob compaction of region " + region.toString() + " started...");
        return compactRegion();
      } finally {
        LOG.debug("Closing region operation mob compaction on " + region);
        region.closeRegionOperation();
      }
    }

    /**
     * Performs mob compaction in the current region.
     * @return True if all the files are selected.
     * @throws IOException
     */
    private boolean compactRegion()
      throws IOException {
      HColumnDescriptor column = region.getTableDesc().getFamily(Bytes.toBytes(columnName));
      PartitionedMobCompactor compactor = new PartitionedMobCompactor(rss, region, tableName,
        column, prefixAndKeys);
      compactor.compact(files, allFiles);
      return compactor.getPartitionedMobCompactionRequest().getCompactionType() ==
        CompactionType.ALL_FILES;
    }
  }

  @Override
  public byte[] insideBarrier() throws ForeignException {
    // No-Op
    return new byte[0];
  }

  @Override
  public void cleanup(Exception e) {
    LOG.info(
      "Aborting all mob compaction subprocedure task threads for '" + procName
        + "' due to error", e);
    try {
      taskManager.cancelTasks();
    } catch (InterruptedException e1) {
      Thread.currentThread().interrupt();
    }
  }
}
