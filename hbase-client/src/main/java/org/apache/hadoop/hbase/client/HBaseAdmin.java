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
package org.apache.hadoop.hbase.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.replication.ReplicationSerDeHelper;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaRetriever;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateConfigurationRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.TableSchema;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AbortProcedureRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AbortProcedureResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AddColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AddColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AssignRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteSnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DisableTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DisableTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DrainRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ExecProcedureRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ExecProcedureResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetClusterStatusRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetCompletedSnapshotsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetNamespaceDescriptorRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProcedureResultRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProcedureResultResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetSchemaAlterStatusRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetSchemaAlterStatusResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableNamesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsInMaintenanceModeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsInMaintenanceModeResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsProcedureDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsProcedureDoneResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListDrainingRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespaceDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListProceduresRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListTableDescriptorsByNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListTableNamesByNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MajorCompactionTimestampForRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MajorCompactionTimestampRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MergeTableRegionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MergeTableRegionsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RemoveDrainFromRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RestoreSnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RestoreSnapshotResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SecurityCapabilitiesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetNormalizerRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ShutdownRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SnapshotResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.StopMasterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.TruncateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.TruncateTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.GetReplicationPeerConfigResponse;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ForeignExceptionUtil;
import org.apache.hadoop.hbase.util.NonceKey;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.KeeperException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;

/**
 * HBaseAdmin is no longer a client API. It is marked InterfaceAudience.Private indicating that
 * this is an HBase-internal class as defined in
 * https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/InterfaceClassification.html
 * There are no guarantees for backwards source / binary compatibility and methods or class can
 * change or go away without deprecation.
 * Use {@link Connection#getAdmin()} to obtain an instance of {@link Admin} instead of constructing
 * an HBaseAdmin directly.
 *
 * <p>Connection should be an <i>unmanaged</i> connection obtained via
 * {@link ConnectionFactory#createConnection(Configuration)}
 *
 * @see ConnectionFactory
 * @see Connection
 * @see Admin
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HBaseAdmin implements Admin {
  private static final Log LOG = LogFactory.getLog(HBaseAdmin.class);

  private static final String ZK_IDENTIFIER_PREFIX =  "hbase-admin-on-";

  private ClusterConnection connection;

  private volatile Configuration conf;
  private final long pause;
  private final int numRetries;
  private final int syncWaitTimeout;
  private boolean aborted;
  private int operationTimeout;
  private int rpcTimeout;

  private RpcRetryingCallerFactory rpcCallerFactory;
  private RpcControllerFactory rpcControllerFactory;

  private NonceGenerator ng;

  @Override
  public int getOperationTimeout() {
    return operationTimeout;
  }

  HBaseAdmin(ClusterConnection connection) throws IOException {
    this.conf = connection.getConfiguration();
    this.connection = connection;

    // TODO: receive ConnectionConfiguration here rather than re-parsing these configs every time.
    this.pause = this.conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
        HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    this.numRetries = this.conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    this.operationTimeout = this.conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
        HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
    this.rpcTimeout = this.conf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
        HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
    this.syncWaitTimeout = this.conf.getInt(
      "hbase.client.sync.wait.timeout.msec", 10 * 60000); // 10min

    this.rpcCallerFactory = connection.getRpcRetryingCallerFactory();
    this.rpcControllerFactory = connection.getRpcControllerFactory();

    this.ng = this.connection.getNonceGenerator();
  }

  @Override
  public void abort(String why, Throwable e) {
    // Currently does nothing but throw the passed message and exception
    this.aborted = true;
    throw new RuntimeException(why, e);
  }

  @Override
  public boolean isAborted() {
    return this.aborted;
  }

  @Override
  public boolean abortProcedure(final long procId, final boolean mayInterruptIfRunning)
  throws IOException {
    return get(abortProcedureAsync(procId, mayInterruptIfRunning), this.syncWaitTimeout,
      TimeUnit.MILLISECONDS);
  }

  @Override
  public Future<Boolean> abortProcedureAsync(final long procId, final boolean mayInterruptIfRunning)
      throws IOException {
    Boolean abortProcResponse =
        executeCallable(new MasterCallable<AbortProcedureResponse>(getConnection(),
            getRpcControllerFactory()) {
      @Override
      protected AbortProcedureResponse rpcCall() throws Exception {
        AbortProcedureRequest abortProcRequest =
            AbortProcedureRequest.newBuilder().setProcId(procId).build();
        return master.abortProcedure(getRpcController(), abortProcRequest);
      }
    }).getIsProcedureAborted();
    return new AbortProcedureFuture(this, procId, abortProcResponse);
  }

  private static class AbortProcedureFuture extends ProcedureFuture<Boolean> {
    private boolean isAbortInProgress;

    public AbortProcedureFuture(
        final HBaseAdmin admin,
        final Long procId,
        final Boolean abortProcResponse) {
      super(admin, procId);
      this.isAbortInProgress = abortProcResponse;
    }

    @Override
    public Boolean get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if (!this.isAbortInProgress) {
        return false;
      }
      super.get(timeout, unit);
      return true;
    }
  }

  /** @return Connection used by this object. */
  @Override
  public Connection getConnection() {
    return connection;
  }

  @Override
  public boolean tableExists(final TableName tableName) throws IOException {
    return executeCallable(new RpcRetryingCallable<Boolean>() {
      @Override
      protected Boolean rpcCall(int callTimeout) throws Exception {
        return MetaTableAccessor.tableExists(connection, tableName);
      }
    });
  }

  @Override
  public HTableDescriptor[] listTables() throws IOException {
    return listTables((Pattern)null, false);
  }

  @Override
  public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
    return listTables(pattern, false);
  }

  @Override
  public HTableDescriptor[] listTables(String regex) throws IOException {
    return listTables(Pattern.compile(regex), false);
  }

  @Override
  public HTableDescriptor[] listTables(final Pattern pattern, final boolean includeSysTables)
      throws IOException {
    return executeCallable(new MasterCallable<HTableDescriptor[]>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected HTableDescriptor[] rpcCall() throws Exception {
        GetTableDescriptorsRequest req =
            RequestConverter.buildGetTableDescriptorsRequest(pattern, includeSysTables);
        return ProtobufUtil.getHTableDescriptorArray(master.getTableDescriptors(getRpcController(),
            req));
      }
    });
  }

  @Override
  public HTableDescriptor[] listTables(String regex, boolean includeSysTables)
      throws IOException {
    return listTables(Pattern.compile(regex), includeSysTables);
  }

  @Override
  public TableName[] listTableNames() throws IOException {
    return listTableNames((Pattern)null, false);
  }

  @Override
  public TableName[] listTableNames(Pattern pattern) throws IOException {
    return listTableNames(pattern, false);
  }

  @Override
  public TableName[] listTableNames(String regex) throws IOException {
    return listTableNames(Pattern.compile(regex), false);
  }

  @Override
  public TableName[] listTableNames(final Pattern pattern, final boolean includeSysTables)
      throws IOException {
    return executeCallable(new MasterCallable<TableName[]>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected TableName[] rpcCall() throws Exception {
        GetTableNamesRequest req =
            RequestConverter.buildGetTableNamesRequest(pattern, includeSysTables);
        return ProtobufUtil.getTableNameArray(master.getTableNames(getRpcController(), req)
            .getTableNamesList());
      }
    });
  }

  @Override
  public TableName[] listTableNames(final String regex, final boolean includeSysTables)
      throws IOException {
    return listTableNames(Pattern.compile(regex), includeSysTables);
  }

  @Override
  public HTableDescriptor getTableDescriptor(final TableName tableName) throws IOException {
    return getTableDescriptor(tableName, getConnection(), rpcCallerFactory, rpcControllerFactory,
       operationTimeout, rpcTimeout);
  }

  static HTableDescriptor getTableDescriptor(final TableName tableName, Connection connection,
      RpcRetryingCallerFactory rpcCallerFactory, final RpcControllerFactory rpcControllerFactory,
      int operationTimeout, int rpcTimeout) throws IOException {
    if (tableName == null) return null;
    HTableDescriptor htd =
        executeCallable(new MasterCallable<HTableDescriptor>(connection, rpcControllerFactory) {
      @Override
      protected HTableDescriptor rpcCall() throws Exception {
        GetTableDescriptorsRequest req =
            RequestConverter.buildGetTableDescriptorsRequest(tableName);
        GetTableDescriptorsResponse htds = master.getTableDescriptors(getRpcController(), req);
        if (!htds.getTableSchemaList().isEmpty()) {
          return ProtobufUtil.convertToHTableDesc(htds.getTableSchemaList().get(0));
        }
        return null;
      }
    }, rpcCallerFactory, operationTimeout, rpcTimeout);
    if (htd != null) {
      return htd;
    }
    throw new TableNotFoundException(tableName.getNameAsString());
  }

  private long getPauseTime(int tries) {
    int triesCount = tries;
    if (triesCount >= HConstants.RETRY_BACKOFF.length) {
      triesCount = HConstants.RETRY_BACKOFF.length - 1;
    }
    return this.pause * HConstants.RETRY_BACKOFF[triesCount];
  }

  @Override
  public void createTable(HTableDescriptor desc)
  throws IOException {
    createTable(desc, null);
  }

  @Override
  public void createTable(HTableDescriptor desc, byte [] startKey,
      byte [] endKey, int numRegions)
  throws IOException {
    if(numRegions < 3) {
      throw new IllegalArgumentException("Must create at least three regions");
    } else if(Bytes.compareTo(startKey, endKey) >= 0) {
      throw new IllegalArgumentException("Start key must be smaller than end key");
    }
    if (numRegions == 3) {
      createTable(desc, new byte[][]{startKey, endKey});
      return;
    }
    byte [][] splitKeys = Bytes.split(startKey, endKey, numRegions - 3);
    if(splitKeys == null || splitKeys.length != numRegions - 1) {
      throw new IllegalArgumentException("Unable to split key range into enough regions");
    }
    createTable(desc, splitKeys);
  }

  @Override
  public void createTable(final HTableDescriptor desc, byte [][] splitKeys)
      throws IOException {
    get(createTableAsync(desc, splitKeys), syncWaitTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public Future<Void> createTableAsync(final HTableDescriptor desc, final byte[][] splitKeys)
      throws IOException {
    if (desc.getTableName() == null) {
      throw new IllegalArgumentException("TableName cannot be null");
    }
    if (splitKeys != null && splitKeys.length > 0) {
      Arrays.sort(splitKeys, Bytes.BYTES_COMPARATOR);
      // Verify there are no duplicate split keys
      byte[] lastKey = null;
      for (byte[] splitKey : splitKeys) {
        if (Bytes.compareTo(splitKey, HConstants.EMPTY_BYTE_ARRAY) == 0) {
          throw new IllegalArgumentException(
              "Empty split key must not be passed in the split keys.");
        }
        if (lastKey != null && Bytes.equals(splitKey, lastKey)) {
          throw new IllegalArgumentException("All split keys must be unique, " +
            "found duplicate: " + Bytes.toStringBinary(splitKey) +
            ", " + Bytes.toStringBinary(lastKey));
        }
        lastKey = splitKey;
      }
    }

    CreateTableResponse response = executeCallable(
      new MasterCallable<CreateTableResponse>(getConnection(), getRpcControllerFactory()) {
        @Override
        protected CreateTableResponse rpcCall() throws Exception {
          setPriority(desc.getTableName());
          CreateTableRequest request = RequestConverter.buildCreateTableRequest(
            desc, splitKeys, ng.getNonceGroup(), ng.newNonce());
          return master.createTable(getRpcController(), request);
        }
      });
    return new CreateTableFuture(this, desc, splitKeys, response);
  }

  private static class CreateTableFuture extends TableFuture<Void> {
    private final HTableDescriptor desc;
    private final byte[][] splitKeys;

    public CreateTableFuture(final HBaseAdmin admin, final HTableDescriptor desc,
        final byte[][] splitKeys, final CreateTableResponse response) {
      super(admin, desc.getTableName(),
              (response != null && response.hasProcId()) ? response.getProcId() : null);
      this.splitKeys = splitKeys;
      this.desc = desc;
    }

    @Override
    protected HTableDescriptor getTableDescriptor() {
      return desc;
    }

    @Override
    public String getOperationType() {
      return "CREATE";
    }

    @Override
    protected Void waitOperationResult(final long deadlineTs) throws IOException, TimeoutException {
      waitForTableEnabled(deadlineTs);
      waitForAllRegionsOnline(deadlineTs, splitKeys);
      return null;
    }
  }

  @Override
  public void deleteTable(final TableName tableName) throws IOException {
    get(deleteTableAsync(tableName), syncWaitTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public Future<Void> deleteTableAsync(final TableName tableName) throws IOException {
    DeleteTableResponse response = executeCallable(
      new MasterCallable<DeleteTableResponse>(getConnection(), getRpcControllerFactory()) {
        @Override
        protected DeleteTableResponse rpcCall() throws Exception {
          setPriority(tableName);
          DeleteTableRequest req =
              RequestConverter.buildDeleteTableRequest(tableName, ng.getNonceGroup(),ng.newNonce());
          return master.deleteTable(getRpcController(), req);
        }
      });
    return new DeleteTableFuture(this, tableName, response);
  }

  private static class DeleteTableFuture extends TableFuture<Void> {
    public DeleteTableFuture(final HBaseAdmin admin, final TableName tableName,
        final DeleteTableResponse response) {
      super(admin, tableName,
              (response != null && response.hasProcId()) ? response.getProcId() : null);
    }

    @Override
    public String getOperationType() {
      return "DELETE";
    }

    @Override
    protected Void waitOperationResult(final long deadlineTs)
        throws IOException, TimeoutException {
      waitTableNotFound(deadlineTs);
      return null;
    }

    @Override
    protected Void postOperationResult(final Void result, final long deadlineTs)
        throws IOException, TimeoutException {
      // Delete cached information to prevent clients from using old locations
      ((ClusterConnection) getAdmin().getConnection()).clearRegionCache(getTableName());
      return super.postOperationResult(result, deadlineTs);
    }
  }

  @Override
  public HTableDescriptor[] deleteTables(String regex) throws IOException {
    return deleteTables(Pattern.compile(regex));
  }

  /**
   * Delete tables matching the passed in pattern and wait on completion.
   *
   * Warning: Use this method carefully, there is no prompting and the effect is
   * immediate. Consider using {@link #listTables(java.util.regex.Pattern) } and
   * {@link #deleteTable(TableName)}
   *
   * @param pattern The pattern to match table names against
   * @return Table descriptors for tables that couldn't be deleted
   * @throws IOException
   */
  @Override
  public HTableDescriptor[] deleteTables(Pattern pattern) throws IOException {
    List<HTableDescriptor> failed = new LinkedList<HTableDescriptor>();
    for (HTableDescriptor table : listTables(pattern)) {
      try {
        deleteTable(table.getTableName());
      } catch (IOException ex) {
        LOG.info("Failed to delete table " + table.getTableName(), ex);
        failed.add(table);
      }
    }
    return failed.toArray(new HTableDescriptor[failed.size()]);
  }

  @Override
  public void truncateTable(final TableName tableName, final boolean preserveSplits)
      throws IOException {
    get(truncateTableAsync(tableName, preserveSplits), syncWaitTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public Future<Void> truncateTableAsync(final TableName tableName, final boolean preserveSplits)
      throws IOException {
    TruncateTableResponse response =
        executeCallable(new MasterCallable<TruncateTableResponse>(getConnection(),
            getRpcControllerFactory()) {
          @Override
          protected TruncateTableResponse rpcCall() throws Exception {
            setPriority(tableName);
            LOG.info("Started truncating " + tableName);
            TruncateTableRequest req = RequestConverter.buildTruncateTableRequest(
              tableName, preserveSplits, ng.getNonceGroup(), ng.newNonce());
            return master.truncateTable(getRpcController(), req);
          }
        });
    return new TruncateTableFuture(this, tableName, preserveSplits, response);
  }

  private static class TruncateTableFuture extends TableFuture<Void> {
    private final boolean preserveSplits;

    public TruncateTableFuture(final HBaseAdmin admin, final TableName tableName,
        final boolean preserveSplits, final TruncateTableResponse response) {
      super(admin, tableName,
             (response != null && response.hasProcId()) ? response.getProcId() : null);
      this.preserveSplits = preserveSplits;
    }

    @Override
    public String getOperationType() {
      return "TRUNCATE";
    }

    @Override
    protected Void waitOperationResult(final long deadlineTs) throws IOException, TimeoutException {
      waitForTableEnabled(deadlineTs);
      // once the table is enabled, we know the operation is done. so we can fetch the splitKeys
      byte[][] splitKeys = preserveSplits ? getAdmin().getTableSplits(getTableName()) : null;
      waitForAllRegionsOnline(deadlineTs, splitKeys);
      return null;
    }
  }

  private byte[][] getTableSplits(final TableName tableName) throws IOException {
    byte[][] splits = null;
    try (RegionLocator locator = getConnection().getRegionLocator(tableName)) {
      byte[][] startKeys = locator.getStartKeys();
      if (startKeys.length == 1) {
        return splits;
      }
      splits = new byte[startKeys.length - 1][];
      for (int i = 1; i < startKeys.length; i++) {
        splits[i - 1] = startKeys[i];
      }
    }
    return splits;
  }

  @Override
  public void enableTable(final TableName tableName)
  throws IOException {
    get(enableTableAsync(tableName), syncWaitTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public Future<Void> enableTableAsync(final TableName tableName) throws IOException {
    TableName.isLegalFullyQualifiedTableName(tableName.getName());
    EnableTableResponse response = executeCallable(
      new MasterCallable<EnableTableResponse>(getConnection(), getRpcControllerFactory()) {
        @Override
        protected EnableTableResponse rpcCall() throws Exception {
          setPriority(tableName);
          LOG.info("Started enable of " + tableName);
          EnableTableRequest req =
              RequestConverter.buildEnableTableRequest(tableName, ng.getNonceGroup(),ng.newNonce());
          return master.enableTable(getRpcController(),req);
        }
      });
    return new EnableTableFuture(this, tableName, response);
  }

  private static class EnableTableFuture extends TableFuture<Void> {
    public EnableTableFuture(final HBaseAdmin admin, final TableName tableName,
        final EnableTableResponse response) {
      super(admin, tableName,
              (response != null && response.hasProcId()) ? response.getProcId() : null);
    }

    @Override
    public String getOperationType() {
      return "ENABLE";
    }

    @Override
    protected Void waitOperationResult(final long deadlineTs) throws IOException, TimeoutException {
      waitForTableEnabled(deadlineTs);
      return null;
    }
  }

  @Override
  public HTableDescriptor[] enableTables(String regex) throws IOException {
    return enableTables(Pattern.compile(regex));
  }

  @Override
  public HTableDescriptor[] enableTables(Pattern pattern) throws IOException {
    List<HTableDescriptor> failed = new LinkedList<HTableDescriptor>();
    for (HTableDescriptor table : listTables(pattern)) {
      if (isTableDisabled(table.getTableName())) {
        try {
          enableTable(table.getTableName());
        } catch (IOException ex) {
          LOG.info("Failed to enable table " + table.getTableName(), ex);
          failed.add(table);
        }
      }
    }
    return failed.toArray(new HTableDescriptor[failed.size()]);
  }

  @Override
  public void disableTable(final TableName tableName)
  throws IOException {
    get(disableTableAsync(tableName), syncWaitTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public Future<Void> disableTableAsync(final TableName tableName) throws IOException {
    TableName.isLegalFullyQualifiedTableName(tableName.getName());
    DisableTableResponse response = executeCallable(
      new MasterCallable<DisableTableResponse>(getConnection(), getRpcControllerFactory()) {
        @Override
        protected DisableTableResponse rpcCall() throws Exception {
          setPriority(tableName);
          LOG.info("Started disable of " + tableName);
          DisableTableRequest req =
              RequestConverter.buildDisableTableRequest(
                tableName, ng.getNonceGroup(), ng.newNonce());
          return master.disableTable(getRpcController(), req);
        }
      });
    return new DisableTableFuture(this, tableName, response);
  }

  private static class DisableTableFuture extends TableFuture<Void> {
    public DisableTableFuture(final HBaseAdmin admin, final TableName tableName,
        final DisableTableResponse response) {
      super(admin, tableName,
              (response != null && response.hasProcId()) ? response.getProcId() : null);
    }

    @Override
    public String getOperationType() {
      return "DISABLE";
    }

    @Override
    protected Void waitOperationResult(long deadlineTs) throws IOException, TimeoutException {
      waitForTableDisabled(deadlineTs);
      return null;
    }
  }

  @Override
  public HTableDescriptor[] disableTables(String regex) throws IOException {
    return disableTables(Pattern.compile(regex));
  }

  @Override
  public HTableDescriptor[] disableTables(Pattern pattern) throws IOException {
    List<HTableDescriptor> failed = new LinkedList<HTableDescriptor>();
    for (HTableDescriptor table : listTables(pattern)) {
      if (isTableEnabled(table.getTableName())) {
        try {
          disableTable(table.getTableName());
        } catch (IOException ex) {
          LOG.info("Failed to disable table " + table.getTableName(), ex);
          failed.add(table);
        }
      }
    }
    return failed.toArray(new HTableDescriptor[failed.size()]);
  }

  @Override
  public boolean isTableEnabled(final TableName tableName) throws IOException {
    checkTableExists(tableName);
    return executeCallable(new RpcRetryingCallable<Boolean>() {
      @Override
      protected Boolean rpcCall(int callTimeout) throws Exception {
        TableState tableState = MetaTableAccessor.getTableState(getConnection(), tableName);
        if (tableState == null) {
          throw new TableNotFoundException(tableName);
        }
        return tableState.inStates(TableState.State.ENABLED);
      }
    });
  }

  @Override
  public boolean isTableDisabled(TableName tableName) throws IOException {
    checkTableExists(tableName);
    return connection.isTableDisabled(tableName);
  }

  @Override
  public boolean isTableAvailable(TableName tableName) throws IOException {
    return connection.isTableAvailable(tableName, null);
  }

  @Override
  public boolean isTableAvailable(TableName tableName, byte[][] splitKeys) throws IOException {
    return connection.isTableAvailable(tableName, splitKeys);
  }

  @Override
  public Pair<Integer, Integer> getAlterStatus(final TableName tableName) throws IOException {
    return executeCallable(new MasterCallable<Pair<Integer, Integer>>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected Pair<Integer, Integer> rpcCall() throws Exception {
        setPriority(tableName);
        GetSchemaAlterStatusRequest req = RequestConverter
            .buildGetSchemaAlterStatusRequest(tableName);
        GetSchemaAlterStatusResponse ret = master.getSchemaAlterStatus(getRpcController(), req);
        Pair<Integer, Integer> pair = new Pair<>(ret.getYetToUpdateRegions(),
            ret.getTotalRegions());
        return pair;
      }
    });
  }

  @Override
  public Pair<Integer, Integer> getAlterStatus(final byte[] tableName) throws IOException {
    return getAlterStatus(TableName.valueOf(tableName));
  }

  /**
   * {@inheritDoc}
   * @deprecated Since 2.0. Will be removed in 3.0. Use
   *     {@link #addColumnFamily(TableName, HColumnDescriptor)} instead.
   */
  @Override
  @Deprecated
  public void addColumn(final TableName tableName, final HColumnDescriptor columnFamily)
  throws IOException {
    addColumnFamily(tableName, columnFamily);
  }

  @Override
  public void addColumnFamily(final TableName tableName, final HColumnDescriptor columnFamily)
      throws IOException {
    get(addColumnFamilyAsync(tableName, columnFamily), syncWaitTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public Future<Void> addColumnFamilyAsync(final TableName tableName,
      final HColumnDescriptor columnFamily) throws IOException {
    AddColumnResponse response =
        executeCallable(new MasterCallable<AddColumnResponse>(getConnection(),
            getRpcControllerFactory()) {
          @Override
          protected AddColumnResponse rpcCall() throws Exception {
            setPriority(tableName);
            AddColumnRequest req =
                RequestConverter.buildAddColumnRequest(tableName, columnFamily, ng.getNonceGroup(),
                  ng.newNonce());
            return master.addColumn(getRpcController(), req);
          }
        });
    return new AddColumnFamilyFuture(this, tableName, response);
  }

  private static class AddColumnFamilyFuture extends ModifyTableFuture {
    public AddColumnFamilyFuture(final HBaseAdmin admin, final TableName tableName,
        final AddColumnResponse response) {
      super(admin, tableName, (response != null && response.hasProcId()) ? response.getProcId()
          : null);
    }

    @Override
    public String getOperationType() {
      return "ADD_COLUMN_FAMILY";
    }
  }

  /**
   * {@inheritDoc}
   * @deprecated Since 2.0. Will be removed in 3.0. Use
   *     {@link #deleteColumnFamily(TableName, byte[])} instead.
   */
  @Override
  @Deprecated
  public void deleteColumn(final TableName tableName, final byte[] columnFamily)
  throws IOException {
    deleteColumnFamily(tableName, columnFamily);
  }

  @Override
  public void deleteColumnFamily(final TableName tableName, final byte[] columnFamily)
      throws IOException {
    get(deleteColumnFamilyAsync(tableName, columnFamily), syncWaitTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public Future<Void> deleteColumnFamilyAsync(final TableName tableName, final byte[] columnFamily)
      throws IOException {
    DeleteColumnResponse response =
        executeCallable(new MasterCallable<DeleteColumnResponse>(getConnection(),
            getRpcControllerFactory()) {
          @Override
          protected DeleteColumnResponse rpcCall() throws Exception {
            setPriority(tableName);
            DeleteColumnRequest req =
                RequestConverter.buildDeleteColumnRequest(tableName, columnFamily,
                  ng.getNonceGroup(), ng.newNonce());
            return master.deleteColumn(getRpcController(), req);
          }
        });
    return new DeleteColumnFamilyFuture(this, tableName, response);
  }

  private static class DeleteColumnFamilyFuture extends ModifyTableFuture {
    public DeleteColumnFamilyFuture(final HBaseAdmin admin, final TableName tableName,
        final DeleteColumnResponse response) {
      super(admin, tableName, (response != null && response.hasProcId()) ? response.getProcId()
          : null);
    }

    @Override
    public String getOperationType() {
      return "DELETE_COLUMN_FAMILY";
    }
  }

  /**
   * {@inheritDoc}
   * @deprecated As of 2.0. Will be removed in 3.0. Use
   *     {@link #modifyColumnFamily(TableName, HColumnDescriptor)} instead.
   */
  @Override
  @Deprecated
  public void modifyColumn(final TableName tableName, final HColumnDescriptor columnFamily)
  throws IOException {
    modifyColumnFamily(tableName, columnFamily);
  }

  @Override
  public void modifyColumnFamily(final TableName tableName,
      final HColumnDescriptor columnFamily) throws IOException {
    get(modifyColumnFamilyAsync(tableName, columnFamily), syncWaitTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public Future<Void> modifyColumnFamilyAsync(final TableName tableName,
      final HColumnDescriptor columnFamily) throws IOException {
    ModifyColumnResponse response =
        executeCallable(new MasterCallable<ModifyColumnResponse>(getConnection(),
            getRpcControllerFactory()) {
          @Override
          protected ModifyColumnResponse rpcCall() throws Exception {
            setPriority(tableName);
            ModifyColumnRequest req =
                RequestConverter.buildModifyColumnRequest(tableName, columnFamily,
                  ng.getNonceGroup(), ng.newNonce());
            return master.modifyColumn(getRpcController(), req);
          }
        });
    return new ModifyColumnFamilyFuture(this, tableName, response);
  }

  private static class ModifyColumnFamilyFuture extends ModifyTableFuture {
    public ModifyColumnFamilyFuture(final HBaseAdmin admin, final TableName tableName,
        final ModifyColumnResponse response) {
      super(admin, tableName, (response != null && response.hasProcId()) ? response.getProcId()
          : null);
    }

    @Override
    public String getOperationType() {
      return "MODIFY_COLUMN_FAMILY";
    }
  }

  @Override
  public void closeRegion(final String regionname, final String serverName) throws IOException {
    closeRegion(Bytes.toBytes(regionname), serverName);
  }

  @Override
  public void closeRegion(final byte [] regionname, final String serverName) throws IOException {
    if (serverName != null) {
      Pair<HRegionInfo, ServerName> pair = MetaTableAccessor.getRegion(connection, regionname);
      if (pair == null || pair.getFirst() == null) {
        throw new UnknownRegionException(Bytes.toStringBinary(regionname));
      } else {
        closeRegion(ServerName.valueOf(serverName), pair.getFirst());
      }
    } else {
      Pair<HRegionInfo, ServerName> pair = MetaTableAccessor.getRegion(connection, regionname);
      if (pair == null) {
        throw new UnknownRegionException(Bytes.toStringBinary(regionname));
      } else if (pair.getSecond() == null) {
        throw new NoServerForRegionException(Bytes.toStringBinary(regionname));
      } else {
        closeRegion(pair.getSecond(), pair.getFirst());
      }
    }
  }

  @Override
  public boolean closeRegionWithEncodedRegionName(final String encodedRegionName,
      final String serverName)
  throws IOException {
    if (null == serverName || ("").equals(serverName.trim())) {
      throw new IllegalArgumentException("The servername cannot be null or empty.");
    }
    ServerName sn = ServerName.valueOf(serverName);
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    // Close the region without updating zk state.
    CloseRegionRequest request =
      ProtobufUtil.buildCloseRegionRequest(sn, encodedRegionName);
    // TODO: There is no timeout on this controller. Set one!
    HBaseRpcController controller = this.rpcControllerFactory.newController();
    try {
      CloseRegionResponse response = admin.closeRegion(controller, request);
      boolean closed = response.getClosed();
      if (false == closed) {
        LOG.error("Not able to close the region " + encodedRegionName + ".");
      }
      return closed;
    } catch (Exception e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public void closeRegion(final ServerName sn, final HRegionInfo hri) throws IOException {
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    // TODO: There is no timeout on this controller. Set one!
    HBaseRpcController controller = rpcControllerFactory.newController();

    // Close the region without updating zk state.
    ProtobufUtil.closeRegion(controller, admin, sn, hri.getRegionName());
  }

  @Override
  public List<HRegionInfo> getOnlineRegions(final ServerName sn) throws IOException {
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    // TODO: There is no timeout on this controller. Set one!
    HBaseRpcController controller = rpcControllerFactory.newController();
    return ProtobufUtil.getOnlineRegions(controller, admin);
  }

  @Override
  public void flush(final TableName tableName) throws IOException {
    checkTableExists(tableName);
    if (isTableDisabled(tableName)) {
      LOG.info("Table is disabled: " + tableName.getNameAsString());
      return;
    }
    execProcedure("flush-table-proc", tableName.getNameAsString(),
      new HashMap<String, String>());
  }

  @Override
  public void flushRegion(final byte[] regionName) throws IOException {
    Pair<HRegionInfo, ServerName> regionServerPair = getRegion(regionName);
    if (regionServerPair == null) {
      throw new IllegalArgumentException("Unknown regionname: " + Bytes.toStringBinary(regionName));
    }
    if (regionServerPair.getSecond() == null) {
      throw new NoServerForRegionException(Bytes.toStringBinary(regionName));
    }
    final HRegionInfo hRegionInfo = regionServerPair.getFirst();
    ServerName serverName = regionServerPair.getSecond();
    final AdminService.BlockingInterface admin = this.connection.getAdmin(serverName);
    Callable<Void> callable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        // TODO: There is no timeout on this controller. Set one!
        HBaseRpcController controller = rpcControllerFactory.newController();
        FlushRegionRequest request =
            RequestConverter.buildFlushRegionRequest(hRegionInfo.getRegionName());
        admin.flushRegion(controller, request);
        return null;
      }
    };
    ProtobufUtil.call(callable);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void compact(final TableName tableName)
    throws IOException {
    compact(tableName, null, false, CompactType.NORMAL);
  }

  @Override
  public void compactRegion(final byte[] regionName)
    throws IOException {
    compactRegion(regionName, null, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void compact(final TableName tableName, final byte[] columnFamily)
    throws IOException {
    compact(tableName, columnFamily, false, CompactType.NORMAL);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void compactRegion(final byte[] regionName, final byte[] columnFamily)
    throws IOException {
    compactRegion(regionName, columnFamily, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void compactRegionServer(final ServerName sn, boolean major)
  throws IOException, InterruptedException {
    for (HRegionInfo region : getOnlineRegions(sn)) {
      compact(sn, region, major, null);
    }
  }

  @Override
  public void majorCompact(final TableName tableName)
  throws IOException {
    compact(tableName, null, true, CompactType.NORMAL);
  }

  @Override
  public void majorCompactRegion(final byte[] regionName)
  throws IOException {
    compactRegion(regionName, null, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void majorCompact(final TableName tableName, final byte[] columnFamily)
  throws IOException {
    compact(tableName, columnFamily, true, CompactType.NORMAL);
  }

  @Override
  public void majorCompactRegion(final byte[] regionName, final byte[] columnFamily)
  throws IOException {
    compactRegion(regionName, columnFamily, true);
  }

  /**
   * Compact a table.
   * Asynchronous operation.
   *
   * @param tableName table or region to compact
   * @param columnFamily column family within a table or region
   * @param major True if we are to do a major compaction.
   * @param compactType {@link org.apache.hadoop.hbase.client.CompactType}
   * @throws IOException if a remote or network exception occurs
   */
  private void compact(final TableName tableName, final byte[] columnFamily,final boolean major,
                       CompactType compactType) throws IOException {
    switch (compactType) {
      case MOB:
        ServerName master = getMasterAddress();
        compact(master, getMobRegionInfo(tableName), major, columnFamily);
        break;
      case NORMAL:
      default:
        ZooKeeperWatcher zookeeper = null;
        try {
          checkTableExists(tableName);
          zookeeper = new ZooKeeperWatcher(conf, ZK_IDENTIFIER_PREFIX + connection.toString(),
                  new ThrowableAbortable());
          List<Pair<HRegionInfo, ServerName>> pairs;
          if (TableName.META_TABLE_NAME.equals(tableName)) {
            pairs = new MetaTableLocator().getMetaRegionsAndLocations(zookeeper);
          } else {
            pairs = MetaTableAccessor.getTableRegionsAndLocations(connection, tableName);
          }
          for (Pair<HRegionInfo, ServerName> pair: pairs) {
            if (pair.getFirst().isOffline()) continue;
            if (pair.getSecond() == null) continue;
            try {
              compact(pair.getSecond(), pair.getFirst(), major, columnFamily);
            } catch (NotServingRegionException e) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Trying to" + (major ? " major" : "") + " compact " +
                        pair.getFirst() + ": " +
                        StringUtils.stringifyException(e));
              }
            }
          }
        } finally {
          if (zookeeper != null) {
            zookeeper.close();
          }
        }
        break;
    }
  }

  /**
   * Compact an individual region.
   * Asynchronous operation.
   *
   * @param regionName region to compact
   * @param columnFamily column family within a table or region
   * @param major True if we are to do a major compaction.
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  private void compactRegion(final byte[] regionName, final byte[] columnFamily,final boolean major)
  throws IOException {
    Pair<HRegionInfo, ServerName> regionServerPair = getRegion(regionName);
    if (regionServerPair == null) {
      throw new IllegalArgumentException("Invalid region: " + Bytes.toStringBinary(regionName));
    }
    if (regionServerPair.getSecond() == null) {
      throw new NoServerForRegionException(Bytes.toStringBinary(regionName));
    }
    compact(regionServerPair.getSecond(), regionServerPair.getFirst(), major, columnFamily);
  }

  private void compact(final ServerName sn, final HRegionInfo hri,
      final boolean major, final byte [] family)
  throws IOException {
    final AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    Callable<Void> callable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        // TODO: There is no timeout on this controller. Set one!
        HBaseRpcController controller = rpcControllerFactory.newController();
        CompactRegionRequest request =
            RequestConverter.buildCompactRegionRequest(hri.getRegionName(), major, family);
        admin.compactRegion(controller, request);
        return null;
      }
    };
    ProtobufUtil.call(callable);
  }

  @Override
  public void move(final byte [] encodedRegionName, final byte [] destServerName)
  throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        setPriority(encodedRegionName);
        MoveRegionRequest request =
            RequestConverter.buildMoveRegionRequest(encodedRegionName, destServerName);
        master.moveRegion(getRpcController(), request);
        return null;
      }
    });
  }

  @Override
  public void assign(final byte [] regionName) throws MasterNotRunningException,
      ZooKeeperConnectionException, IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        setPriority(regionName);
        AssignRegionRequest request =
            RequestConverter.buildAssignRegionRequest(getRegionName(regionName));
        master.assignRegion(getRpcController(), request);
        return null;
      }
    });
  }

  @Override
  public void unassign(final byte [] regionName, final boolean force)
  throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    final byte[] toBeUnassigned = getRegionName(regionName);
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        setPriority(regionName);
        UnassignRegionRequest request =
            RequestConverter.buildUnassignRegionRequest(toBeUnassigned, force);
        master.unassignRegion(getRpcController(), request);
        return null;
      }
    });
  }

  @Override
  public void offline(final byte [] regionName)
  throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        setPriority(regionName);
        master.offlineRegion(getRpcController(),
            RequestConverter.buildOfflineRegionRequest(regionName));
        return null;
      }
    });
  }

  @Override
  public boolean setBalancerRunning(final boolean on, final boolean synchronous)
  throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        SetBalancerRunningRequest req =
            RequestConverter.buildSetBalancerRunningRequest(on, synchronous);
        return master.setBalancerRunning(getRpcController(), req).getPrevBalanceValue();
      }
    });
  }

  @Override
  public boolean balancer() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        return master.balance(getRpcController(),
            RequestConverter.buildBalanceRequest(false)).getBalancerRan();
      }
    });
  }

  @Override
  public boolean balancer(final boolean force) throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        return master.balance(getRpcController(),
            RequestConverter.buildBalanceRequest(force)).getBalancerRan();
      }
    });
  }

  @Override
  public boolean isBalancerEnabled() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        return master.isBalancerEnabled(getRpcController(),
          RequestConverter.buildIsBalancerEnabledRequest()).getEnabled();
      }
    });
  }

  @Override
  public boolean normalize() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        return master.normalize(getRpcController(),
            RequestConverter.buildNormalizeRequest()).getNormalizerRan();
      }
    });
  }

  @Override
  public boolean isNormalizerEnabled() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        return master.isNormalizerEnabled(getRpcController(),
          RequestConverter.buildIsNormalizerEnabledRequest()).getEnabled();
      }
    });
  }

  @Override
  public boolean setNormalizerRunning(final boolean on) throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        SetNormalizerRunningRequest req =
          RequestConverter.buildSetNormalizerRunningRequest(on);
        return master.setNormalizerRunning(getRpcController(), req).getPrevNormalizerValue();
      }
    });
  }

  @Override
  public boolean enableCatalogJanitor(final boolean enable) throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        return master.enableCatalogJanitor(getRpcController(),
          RequestConverter.buildEnableCatalogJanitorRequest(enable)).getPrevValue();
      }
    });
  }

  @Override
  public int runCatalogScan() throws IOException {
    return executeCallable(new MasterCallable<Integer>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Integer rpcCall() throws Exception {
        return master.runCatalogScan(getRpcController(),
          RequestConverter.buildCatalogScanRequest()).getScanResult();
      }
    });
  }

  @Override
  public boolean isCatalogJanitorEnabled() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        return master.isCatalogJanitorEnabled(getRpcController(),
          RequestConverter.buildIsCatalogJanitorEnabledRequest()).getValue();
      }
    });
  }

  @Override
  public boolean setCleanerChoreRunning(final boolean on) throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override public Boolean rpcCall() throws Exception {
        return master.setCleanerChoreRunning(getRpcController(), RequestConverter
                                                                   .buildSetCleanerChoreRunningRequest(
                                                                     on)).getPrevValue();
      }
    });
  }

  @Override
  public boolean runCleanerChore() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override public Boolean rpcCall() throws Exception {
        return master
                 .runCleanerChore(getRpcController(), RequestConverter
                                                        .buildRunCleanerChoreRequest())
                 .getCleanerChoreRan();
      }
    });
  }

  @Override
  public boolean isCleanerChoreEnabled() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override public Boolean rpcCall() throws Exception {
        return master.isCleanerChoreEnabled(getRpcController(),
                                            RequestConverter.buildIsCleanerChoreEnabledRequest())
                     .getValue();
      }
    });
  }

  private boolean isEncodedRegionName(byte[] regionName) throws IOException {
    try {
      HRegionInfo.parseRegionName(regionName);
      return false;
    } catch (IOException e) {
      if (StringUtils.stringifyException(e)
        .contains(HRegionInfo.INVALID_REGION_NAME_FORMAT_MESSAGE)) {
        return true;
      }
      throw e;
    }
  }

  /**
   * Merge two regions. Synchronous operation.
   * Note: It is not feasible to predict the length of merge.
   *   Therefore, this is for internal testing only.
   * @param nameOfRegionA encoded or full name of region a
   * @param nameOfRegionB encoded or full name of region b
   * @param forcible true if do a compulsory merge, otherwise we will only merge
   *          two adjacent regions
   * @throws IOException
   */
  @VisibleForTesting
  public void mergeRegionsSync(
      final byte[] nameOfRegionA,
      final byte[] nameOfRegionB,
      final boolean forcible) throws IOException {
    get(
      mergeRegionsAsync(nameOfRegionA, nameOfRegionB, forcible),
      syncWaitTimeout,
      TimeUnit.MILLISECONDS);
  }

  /**
   * Merge two regions. Asynchronous operation.
   * @param nameOfRegionA encoded or full name of region a
   * @param nameOfRegionB encoded or full name of region b
   * @param forcible true if do a compulsory merge, otherwise we will only merge
   *          two adjacent regions
   * @throws IOException
   * @deprecated Since 2.0. Will be removed in 3.0. Use
   *     {@link #mergeRegionsAsync(byte[], byte[], boolean)} instead.
   */
  @Deprecated
  @Override
  public void mergeRegions(final byte[] nameOfRegionA,
      final byte[] nameOfRegionB, final boolean forcible)
      throws IOException {
    mergeRegionsAsync(nameOfRegionA, nameOfRegionB, forcible);
  }

  /**
   * Merge two regions. Asynchronous operation.
   * @param nameOfRegionA encoded or full name of region a
   * @param nameOfRegionB encoded or full name of region b
   * @param forcible true if do a compulsory merge, otherwise we will only merge
   *          two adjacent regions
   * @throws IOException
   */
  @Override
  public Future<Void> mergeRegionsAsync(
      final byte[] nameOfRegionA,
      final byte[] nameOfRegionB,
      final boolean forcible) throws IOException {
    byte[][] nameofRegionsToMerge = new byte[2][];
    nameofRegionsToMerge[0] = nameOfRegionA;
    nameofRegionsToMerge[1] = nameOfRegionB;
    return mergeRegionsAsync(nameofRegionsToMerge, forcible);
  }

  /**
   * Merge two regions. Asynchronous operation.
   * @param nameofRegionsToMerge encoded or full name of daughter regions
   * @param forcible true if do a compulsory merge, otherwise we will only merge
   *          adjacent regions
   * @throws IOException
   */
  @Override
  public Future<Void> mergeRegionsAsync(
      final byte[][] nameofRegionsToMerge,
      final boolean forcible) throws IOException {
    assert(nameofRegionsToMerge.length >= 2);
    byte[][] encodedNameofRegionsToMerge = new byte[nameofRegionsToMerge.length][];
    for(int i = 0; i < nameofRegionsToMerge.length; i++) {
      encodedNameofRegionsToMerge[i] = isEncodedRegionName(nameofRegionsToMerge[i]) ?
        nameofRegionsToMerge[i] : HRegionInfo.encodeRegionName(nameofRegionsToMerge[i]).getBytes();
    }

    TableName tableName = null;
    Pair<HRegionInfo, ServerName> pair;

    for(int i = 0; i < nameofRegionsToMerge.length; i++) {
      pair = getRegion(nameofRegionsToMerge[i]);

      if (pair != null) {
        if (pair.getFirst().getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID) {
          throw new IllegalArgumentException ("Can't invoke merge on non-default regions directly");
        }
        if (tableName == null) {
          tableName = pair.getFirst().getTable();
        } else  if (!tableName.equals(pair.getFirst().getTable())) {
          throw new IllegalArgumentException ("Cannot merge regions from two different tables " +
              tableName + " and " + pair.getFirst().getTable());
        }
      } else {
        throw new UnknownRegionException (
          "Can't invoke merge on unknown region "
          + Bytes.toStringBinary(encodedNameofRegionsToMerge[i]));
      }
    }

    MergeTableRegionsResponse response =
        executeCallable(new MasterCallable<MergeTableRegionsResponse>(getConnection(),
            getRpcControllerFactory()) {
      @Override
      protected MergeTableRegionsResponse rpcCall() throws Exception {
        MergeTableRegionsRequest request = RequestConverter
            .buildMergeTableRegionsRequest(
                encodedNameofRegionsToMerge,
                forcible,
                ng.getNonceGroup(),
                ng.newNonce());
        return master.mergeTableRegions(getRpcController(), request);
      }
    });
    return new MergeTableRegionsFuture(this, tableName, response);
  }

  private static class MergeTableRegionsFuture extends TableFuture<Void> {
    public MergeTableRegionsFuture(
        final HBaseAdmin admin,
        final TableName tableName,
        final MergeTableRegionsResponse response) {
      super(admin, tableName,
          (response != null && response.hasProcId()) ? response.getProcId() : null);
    }

    public MergeTableRegionsFuture(
        final HBaseAdmin admin,
        final TableName tableName,
        final Long procId) {
      super(admin, tableName, procId);
    }

    @Override
    public String getOperationType() {
      return "MERGE_REGIONS";
    }
  }

  @Override
  public void split(final TableName tableName) throws IOException {
    split(tableName, null);
  }

  @Override
  public void splitRegion(final byte[] regionName) throws IOException {
    splitRegion(regionName, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void split(final TableName tableName, final byte [] splitPoint) throws IOException {
    ZooKeeperWatcher zookeeper = null;
    try {
      checkTableExists(tableName);
      zookeeper = new ZooKeeperWatcher(conf, ZK_IDENTIFIER_PREFIX + connection.toString(),
        new ThrowableAbortable());
      List<Pair<HRegionInfo, ServerName>> pairs;
      if (TableName.META_TABLE_NAME.equals(tableName)) {
        pairs = new MetaTableLocator().getMetaRegionsAndLocations(zookeeper);
      } else {
        pairs = MetaTableAccessor.getTableRegionsAndLocations(connection, tableName);
      }
      for (Pair<HRegionInfo, ServerName> pair: pairs) {
        // May not be a server for a particular row
        if (pair.getSecond() == null) continue;
        HRegionInfo r = pair.getFirst();
        // check for parents
        if (r.isSplitParent()) continue;
        // if a split point given, only split that particular region
        if (r.getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID ||
           (splitPoint != null && !r.containsRow(splitPoint))) continue;
        // call out to region server to do split now
        split(pair.getSecond(), pair.getFirst(), splitPoint);
      }
    } finally {
      if (zookeeper != null) {
        zookeeper.close();
      }
    }
  }

  @Override
  public void splitRegion(final byte[] regionName, final byte [] splitPoint) throws IOException {
    Pair<HRegionInfo, ServerName> regionServerPair = getRegion(regionName);
    if (regionServerPair == null) {
      throw new IllegalArgumentException("Invalid region: " + Bytes.toStringBinary(regionName));
    }
    if (regionServerPair.getFirst() != null &&
        regionServerPair.getFirst().getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID) {
      throw new IllegalArgumentException("Can't split replicas directly. "
          + "Replicas are auto-split when their primary is split.");
    }
    if (regionServerPair.getSecond() == null) {
      throw new NoServerForRegionException(Bytes.toStringBinary(regionName));
    }
    split(regionServerPair.getSecond(), regionServerPair.getFirst(), splitPoint);
  }

  @VisibleForTesting
  public void split(final ServerName sn, final HRegionInfo hri,
      byte[] splitPoint) throws IOException {
    if (hri.getStartKey() != null && splitPoint != null &&
         Bytes.compareTo(hri.getStartKey(), splitPoint) == 0) {
       throw new IOException("should not give a splitkey which equals to startkey!");
    }
    // TODO: There is no timeout on this controller. Set one!
    HBaseRpcController controller = rpcControllerFactory.newController();
    controller.setPriority(hri.getTable());

    // TODO: this does not do retries, it should. Set priority and timeout in controller
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    ProtobufUtil.split(controller, admin, hri, splitPoint);
  }

  @Override
  public void modifyTable(final TableName tableName, final HTableDescriptor htd)
      throws IOException {
    get(modifyTableAsync(tableName, htd), syncWaitTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public Future<Void> modifyTableAsync(final TableName tableName, final HTableDescriptor htd)
      throws IOException {
    if (!tableName.equals(htd.getTableName())) {
      throw new IllegalArgumentException("the specified table name '" + tableName +
        "' doesn't match with the HTD one: " + htd.getTableName());
    }
    ModifyTableResponse response = executeCallable(
      new MasterCallable<ModifyTableResponse>(getConnection(), getRpcControllerFactory()) {
        @Override
        protected ModifyTableResponse rpcCall() throws Exception {
          setPriority(tableName);
          ModifyTableRequest request = RequestConverter.buildModifyTableRequest(
            tableName, htd, ng.getNonceGroup(), ng.newNonce());
          return master.modifyTable(getRpcController(), request);
        }
      });
    return new ModifyTableFuture(this, tableName, response);
  }

  private static class ModifyTableFuture extends TableFuture<Void> {
    public ModifyTableFuture(final HBaseAdmin admin, final TableName tableName,
        final ModifyTableResponse response) {
      super(admin, tableName,
          (response != null && response.hasProcId()) ? response.getProcId() : null);
    }

    public ModifyTableFuture(final HBaseAdmin admin, final TableName tableName, final Long procId) {
      super(admin, tableName, procId);
    }

    @Override
    public String getOperationType() {
      return "MODIFY";
    }

    @Override
    protected Void postOperationResult(final Void result, final long deadlineTs)
        throws IOException, TimeoutException {
      // The modify operation on the table is asynchronous on the server side irrespective
      // of whether Procedure V2 is supported or not. So, we wait in the client till
      // all regions get updated.
      waitForSchemaUpdate(deadlineTs);
      return result;
    }
  }

  /**
   * @param regionName Name of a region.
   * @return a pair of HRegionInfo and ServerName if <code>regionName</code> is
   *  a verified region name (we call {@link
   *  MetaTableAccessor#getRegionLocation(Connection, byte[])}
   *  else null.
   * Throw IllegalArgumentException if <code>regionName</code> is null.
   * @throws IOException
   */
  Pair<HRegionInfo, ServerName> getRegion(final byte[] regionName) throws IOException {
    if (regionName == null) {
      throw new IllegalArgumentException("Pass a table name or region name");
    }
    Pair<HRegionInfo, ServerName> pair =
      MetaTableAccessor.getRegion(connection, regionName);
    if (pair == null) {
      final AtomicReference<Pair<HRegionInfo, ServerName>> result =
        new AtomicReference<Pair<HRegionInfo, ServerName>>(null);
      final String encodedName = Bytes.toString(regionName);
      MetaTableAccessor.Visitor visitor = new MetaTableAccessor.Visitor() {
        @Override
        public boolean visit(Result data) throws IOException {
          HRegionInfo info = MetaTableAccessor.getHRegionInfo(data);
          if (info == null) {
            LOG.warn("No serialized HRegionInfo in " + data);
            return true;
          }
          RegionLocations rl = MetaTableAccessor.getRegionLocations(data);
          boolean matched = false;
          ServerName sn = null;
          if (rl != null) {
            for (HRegionLocation h : rl.getRegionLocations()) {
              if (h != null && encodedName.equals(h.getRegionInfo().getEncodedName())) {
                sn = h.getServerName();
                info = h.getRegionInfo();
                matched = true;
              }
            }
          }
          if (!matched) return true;
          result.set(new Pair<HRegionInfo, ServerName>(info, sn));
          return false; // found the region, stop
        }
      };

      MetaTableAccessor.fullScanRegions(connection, visitor);
      pair = result.get();
    }
    return pair;
  }

  /**
   * If the input is a region name, it is returned as is. If it's an
   * encoded region name, the corresponding region is found from meta
   * and its region name is returned. If we can't find any region in
   * meta matching the input as either region name or encoded region
   * name, the input is returned as is. We don't throw unknown
   * region exception.
   */
  private byte[] getRegionName(
      final byte[] regionNameOrEncodedRegionName) throws IOException {
    if (Bytes.equals(regionNameOrEncodedRegionName,
        HRegionInfo.FIRST_META_REGIONINFO.getRegionName())
          || Bytes.equals(regionNameOrEncodedRegionName,
            HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes())) {
      return HRegionInfo.FIRST_META_REGIONINFO.getRegionName();
    }
    byte[] tmp = regionNameOrEncodedRegionName;
    Pair<HRegionInfo, ServerName> regionServerPair = getRegion(regionNameOrEncodedRegionName);
    if (regionServerPair != null && regionServerPair.getFirst() != null) {
      tmp = regionServerPair.getFirst().getRegionName();
    }
    return tmp;
  }

  /**
   * Check if table exists or not
   * @param tableName Name of a table.
   * @return tableName instance
   * @throws IOException if a remote or network exception occurs.
   * @throws TableNotFoundException if table does not exist.
   */
  private TableName checkTableExists(final TableName tableName)
      throws IOException {
    return executeCallable(new RpcRetryingCallable<TableName>() {
      @Override
      protected TableName rpcCall(int callTimeout) throws Exception {
        if (!MetaTableAccessor.tableExists(connection, tableName)) {
          throw new TableNotFoundException(tableName);
        }
        return tableName;
      }
    });
  }

  @Override
  public synchronized void shutdown() throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        setPriority(HConstants.HIGH_QOS);
        master.shutdown(getRpcController(), ShutdownRequest.newBuilder().build());
        return null;
      }
    });
  }

  @Override
  public synchronized void stopMaster() throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        setPriority(HConstants.HIGH_QOS);
        master.stopMaster(getRpcController(), StopMasterRequest.newBuilder().build());
        return null;
      }
    });
  }

  @Override
  public synchronized void stopRegionServer(final String hostnamePort)
  throws IOException {
    String hostname = Addressing.parseHostname(hostnamePort);
    int port = Addressing.parsePort(hostnamePort);
    final AdminService.BlockingInterface admin =
      this.connection.getAdmin(ServerName.valueOf(hostname, port, 0));
    // TODO: There is no timeout on this controller. Set one!
    HBaseRpcController controller = rpcControllerFactory.newController();
    controller.setPriority(HConstants.HIGH_QOS);
    StopServerRequest request = RequestConverter.buildStopServerRequest(
        "Called by admin client " + this.connection.toString());
    try {
      admin.stopServer(controller, request);
    } catch (Exception e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public boolean isMasterInMaintenanceMode() throws IOException {
    return executeCallable(new MasterCallable<IsInMaintenanceModeResponse>(getConnection(),
        this.rpcControllerFactory) {
      @Override
      protected IsInMaintenanceModeResponse rpcCall() throws Exception {
        return master.isMasterInMaintenanceMode(getRpcController(),
            IsInMaintenanceModeRequest.newBuilder().build());
      }
    }).getInMaintenanceMode();
  }

  @Override
  public ClusterStatus getClusterStatus() throws IOException {
    return executeCallable(new MasterCallable<ClusterStatus>(getConnection(),
        this.rpcControllerFactory) {
      @Override
      protected ClusterStatus rpcCall() throws Exception {
        GetClusterStatusRequest req = RequestConverter.buildGetClusterStatusRequest();
        return ProtobufUtil.convert(master.getClusterStatus(getRpcController(), req).
            getClusterStatus());
      }
    });
  }

  @Override
  public Map<byte[], RegionLoad> getRegionLoad(final ServerName sn) throws IOException {
    return getRegionLoad(sn, null);
  }

  @Override
  public Map<byte[], RegionLoad> getRegionLoad(final ServerName sn, final TableName tableName)
      throws IOException {
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    HBaseRpcController controller = rpcControllerFactory.newController();
    List<RegionLoad> regionLoads = ProtobufUtil.getRegionLoad(controller, admin, tableName);
    Map<byte[], RegionLoad> resultMap = new TreeMap<byte[], RegionLoad>(Bytes.BYTES_COMPARATOR);
    for (RegionLoad regionLoad : regionLoads) {
      resultMap.put(regionLoad.getName(), regionLoad);
    }
    return resultMap;
  }

  @Override
  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * Do a get with a timeout against the passed in <code>future<code>.
   */
  private static <T> T get(final Future<T> future, final long timeout, final TimeUnit units)
  throws IOException {
    try {
      // TODO: how long should we wait? Spin forever?
      return future.get(timeout, units);
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupt while waiting on " + future);
    } catch (TimeoutException e) {
      throw new TimeoutIOException(e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException)e.getCause();
      } else {
        throw new IOException(e.getCause());
      }
    }
  }

  @Override
  public void createNamespace(final NamespaceDescriptor descriptor)
  throws IOException {
    get(createNamespaceAsync(descriptor), this.syncWaitTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public Future<Void> createNamespaceAsync(final NamespaceDescriptor descriptor)
      throws IOException {
    CreateNamespaceResponse response =
        executeCallable(new MasterCallable<CreateNamespaceResponse>(getConnection(),
            getRpcControllerFactory()) {
      @Override
      protected CreateNamespaceResponse rpcCall() throws Exception {
        return master.createNamespace(getRpcController(),
          CreateNamespaceRequest.newBuilder().setNamespaceDescriptor(ProtobufUtil.
              toProtoNamespaceDescriptor(descriptor)).build());
      }
    });
    return new NamespaceFuture(this, descriptor.getName(), response.getProcId()) {
      @Override
      public String getOperationType() {
        return "CREATE_NAMESPACE";
      }
    };
  }

  @Override
  public void modifyNamespace(final NamespaceDescriptor descriptor)
  throws IOException {
    get(modifyNamespaceAsync(descriptor), this.syncWaitTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public Future<Void> modifyNamespaceAsync(final NamespaceDescriptor descriptor)
      throws IOException {
    ModifyNamespaceResponse response =
        executeCallable(new MasterCallable<ModifyNamespaceResponse>(getConnection(),
            getRpcControllerFactory()) {
      @Override
      protected ModifyNamespaceResponse rpcCall() throws Exception {
        // TODO: set priority based on NS?
        return master.modifyNamespace(getRpcController(), ModifyNamespaceRequest.newBuilder().
          setNamespaceDescriptor(ProtobufUtil.toProtoNamespaceDescriptor(descriptor)).build());
       }
    });
    return new NamespaceFuture(this, descriptor.getName(), response.getProcId()) {
      @Override
      public String getOperationType() {
        return "MODIFY_NAMESPACE";
      }
    };
  }

  @Override
  public void deleteNamespace(final String name)
  throws IOException {
    get(deleteNamespaceAsync(name), this.syncWaitTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public Future<Void> deleteNamespaceAsync(final String name)
      throws IOException {
    DeleteNamespaceResponse response =
        executeCallable(new MasterCallable<DeleteNamespaceResponse>(getConnection(),
            getRpcControllerFactory()) {
      @Override
      protected DeleteNamespaceResponse rpcCall() throws Exception {
        // TODO: set priority based on NS?
        return master.deleteNamespace(getRpcController(), DeleteNamespaceRequest.newBuilder().
          setNamespaceName(name).build());
        }
      });
    return new NamespaceFuture(this, name, response.getProcId()) {
      @Override
      public String getOperationType() {
        return "DELETE_NAMESPACE";
      }
    };
  }

  @Override
  public NamespaceDescriptor getNamespaceDescriptor(final String name)
      throws NamespaceNotFoundException, IOException {
    return executeCallable(new MasterCallable<NamespaceDescriptor>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected NamespaceDescriptor rpcCall() throws Exception {
        return ProtobufUtil.toNamespaceDescriptor(
            master.getNamespaceDescriptor(getRpcController(),
                GetNamespaceDescriptorRequest.newBuilder().
                  setNamespaceName(name).build()).getNamespaceDescriptor());
      }
    });
  }

  @Override
  public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
    return executeCallable(new MasterCallable<NamespaceDescriptor[]>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected NamespaceDescriptor[] rpcCall() throws Exception {
        List<HBaseProtos.NamespaceDescriptor> list =
            master.listNamespaceDescriptors(getRpcController(),
              ListNamespaceDescriptorsRequest.newBuilder().build()).getNamespaceDescriptorList();
        NamespaceDescriptor[] res = new NamespaceDescriptor[list.size()];
        for(int i = 0; i < list.size(); i++) {
          res[i] = ProtobufUtil.toNamespaceDescriptor(list.get(i));
        }
        return res;
      }
    });
  }

  @Override
  public ProcedureInfo[] listProcedures() throws IOException {
    return executeCallable(new MasterCallable<ProcedureInfo[]>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected ProcedureInfo[] rpcCall() throws Exception {
        List<ProcedureProtos.Procedure> procList = master.listProcedures(
            getRpcController(), ListProceduresRequest.newBuilder().build()).getProcedureList();
        ProcedureInfo[] procInfoList = new ProcedureInfo[procList.size()];
        for (int i = 0; i < procList.size(); i++) {
          procInfoList[i] = convert(procList.get(i));
        }
        return procInfoList;
      }
    });
  }

  private static ProcedureInfo convert(final ProcedureProtos.Procedure procProto) {
    NonceKey nonceKey = null;
    if (procProto.getNonce() != HConstants.NO_NONCE) {
      nonceKey = new NonceKey(procProto.getNonceGroup(), procProto.getNonce());
    }
    org.apache.hadoop.hbase.ProcedureState procedureState =
        org.apache.hadoop.hbase.ProcedureState.valueOf(procProto.getState().name());
    return new ProcedureInfo(procProto.getProcId(), procProto.getClassName(), procProto.getOwner(),
        procedureState, procProto.hasParentId() ? procProto.getParentId() : -1, nonceKey,
            procProto.hasException()?
                ForeignExceptionUtil.toIOException(procProto.getException()): null,
            procProto.getLastUpdate(), procProto.getStartTime(),
            procProto.hasResult()? procProto.getResult().toByteArray() : null);
  }

  @Override
  public HTableDescriptor[] listTableDescriptorsByNamespace(final String name) throws IOException {
    return executeCallable(new MasterCallable<HTableDescriptor[]>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected HTableDescriptor[] rpcCall() throws Exception {
        List<TableSchema> list =
            master.listTableDescriptorsByNamespace(getRpcController(),
                ListTableDescriptorsByNamespaceRequest.newBuilder().setNamespaceName(name)
                .build()).getTableSchemaList();
        HTableDescriptor[] res = new HTableDescriptor[list.size()];
        for(int i=0; i < list.size(); i++) {

          res[i] = ProtobufUtil.convertToHTableDesc(list.get(i));
        }
        return res;
      }
    });
  }

  @Override
  public TableName[] listTableNamesByNamespace(final String name) throws IOException {
    return executeCallable(new MasterCallable<TableName[]>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected TableName[] rpcCall() throws Exception {
        List<HBaseProtos.TableName> tableNames =
            master.listTableNamesByNamespace(getRpcController(), ListTableNamesByNamespaceRequest.
                newBuilder().setNamespaceName(name).build())
            .getTableNameList();
        TableName[] result = new TableName[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
          result[i] = ProtobufUtil.toTableName(tableNames.get(i));
        }
        return result;
      }
    });
  }

  /**
   * Check to see if HBase is running. Throw an exception if not.
   * @param conf system configuration
   * @throws MasterNotRunningException if the master is not running
   * @throws ZooKeeperConnectionException if unable to connect to zookeeper
   * @deprecated since hbase-2.0.0 because throws a ServiceException. We don't want to have
   * protobuf as part of our public API. Use {@link #available(Configuration)}
   */
  // Used by tests and by the Merge tool. Merge tool uses it to figure if HBase is up or not.
  // MOB uses it too.
  // NOTE: hbase-2.0.0 removes ServiceException from the throw.
  @Deprecated
  public static void checkHBaseAvailable(Configuration conf)
  throws MasterNotRunningException, ZooKeeperConnectionException, IOException,
  com.google.protobuf.ServiceException {
    available(conf);
  }

  /**
   * Is HBase available? Throw an exception if not.
   * @param conf system configuration
   * @throws MasterNotRunningException if the master is not running.
   * @throws ZooKeeperConnectionException if unable to connect to zookeeper.
   * // TODO do not expose ZKConnectionException.
   */
  public static void available(final Configuration conf)
  throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    Configuration copyOfConf = HBaseConfiguration.create(conf);
    // We set it to make it fail as soon as possible if HBase is not available
    copyOfConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    copyOfConf.setInt("zookeeper.recovery.retry", 0);

    // Check ZK first.
    // If the connection exists, we may have a connection to ZK that does not work anymore
    try (ClusterConnection connection =
        (ClusterConnection) ConnectionFactory.createConnection(copyOfConf)) {
      // Check ZK first.
      // If the connection exists, we may have a connection to ZK that does not work anymore
      ZooKeeperKeepAliveConnection zkw = null;
      try {
        // This is NASTY. FIX!!!! Dependent on internal implementation! TODO
        zkw = ((ConnectionImplementation) connection)
            .getKeepAliveZooKeeperWatcher();
          zkw.getRecoverableZooKeeper().getZooKeeper().exists(zkw.znodePaths.baseZNode, false);
      } catch (IOException e) {
        throw new ZooKeeperConnectionException("Can't connect to ZooKeeper", e);
      } catch (InterruptedException e) {
        throw (InterruptedIOException)
            new InterruptedIOException("Can't connect to ZooKeeper").initCause(e);
      } catch (KeeperException e){
        throw new ZooKeeperConnectionException("Can't connect to ZooKeeper", e);
      } finally {
        if (zkw != null) {
          zkw.close();
        }
      }
      // can throw MasterNotRunningException
      connection.isMasterRunning();
    }
  }

  @Override
  public List<HRegionInfo> getTableRegions(final TableName tableName)
  throws IOException {
    ZooKeeperWatcher zookeeper =
      new ZooKeeperWatcher(conf, ZK_IDENTIFIER_PREFIX + connection.toString(),
        new ThrowableAbortable());
    List<HRegionInfo> regions = null;
    try {
      if (TableName.META_TABLE_NAME.equals(tableName)) {
        regions = new MetaTableLocator().getMetaRegions(zookeeper);
      } else {
        regions = MetaTableAccessor.getTableRegions(connection, tableName, true);
      }
    } finally {
      zookeeper.close();
    }
    return regions;
  }

  @Override
  public synchronized void close() throws IOException {
  }

  @Override
  public HTableDescriptor[] getTableDescriptorsByTableName(final List<TableName> tableNames)
  throws IOException {
    return executeCallable(new MasterCallable<HTableDescriptor[]>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected HTableDescriptor[] rpcCall() throws Exception {
        GetTableDescriptorsRequest req =
            RequestConverter.buildGetTableDescriptorsRequest(tableNames);
          return ProtobufUtil.
              getHTableDescriptorArray(master.getTableDescriptors(getRpcController(), req));
      }
    });
  }

  /**
   * Get tableDescriptor
   * @param tableName one table name
   * @return HTD the HTableDescriptor or null if the table not exists
   * @throws IOException if a remote or network exception occurs
   */
  private HTableDescriptor getTableDescriptorByTableName(TableName tableName)
      throws IOException {
    List<TableName> tableNames = new ArrayList<TableName>(1);
    tableNames.add(tableName);

    HTableDescriptor[] htdl = getTableDescriptorsByTableName(tableNames);

    if (htdl == null || htdl.length == 0) {
      return null;
    }
    else {
      return htdl[0];
    }
  }

  @Override
  public HTableDescriptor[] getTableDescriptors(List<String> names)
  throws IOException {
    List<TableName> tableNames = new ArrayList<TableName>(names.size());
    for(String name : names) {
      tableNames.add(TableName.valueOf(name));
    }
    return getTableDescriptorsByTableName(tableNames);
  }

  private RollWALWriterResponse rollWALWriterImpl(final ServerName sn) throws IOException,
      FailedLogCloseException {
    final AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    RollWALWriterRequest request = RequestConverter.buildRollWALWriterRequest();
    // TODO: There is no timeout on this controller. Set one!
    HBaseRpcController controller = rpcControllerFactory.newController();
    try {
      return admin.rollWALWriter(controller, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  /**
   * Roll the log writer. I.e. when using a file system based write ahead log,
   * start writing log messages to a new file.
   *
   * Note that when talking to a version 1.0+ HBase deployment, the rolling is asynchronous.
   * This method will return as soon as the roll is requested and the return value will
   * always be null. Additionally, the named region server may schedule store flushes at the
   * request of the wal handling the roll request.
   *
   * When talking to a 0.98 or older HBase deployment, the rolling is synchronous and the
   * return value may be either null or a list of encoded region names.
   *
   * @param serverName
   *          The servername of the regionserver. A server name is made of host,
   *          port and startcode. This is mandatory. Here is an example:
   *          <code> host187.example.com,60020,1289493121758</code>
   * @return a set of {@link HRegionInfo#getEncodedName()} that would allow the wal to
   *         clean up some underlying files. null if there's nothing to flush.
   * @throws IOException if a remote or network exception occurs
   * @throws FailedLogCloseException
   * @deprecated use {@link #rollWALWriter(ServerName)}
   */
  @Deprecated
  public synchronized byte[][] rollHLogWriter(String serverName)
      throws IOException, FailedLogCloseException {
    ServerName sn = ServerName.valueOf(serverName);
    final RollWALWriterResponse response = rollWALWriterImpl(sn);
    int regionCount = response.getRegionToFlushCount();
    if (0 == regionCount) {
      return null;
    }
    byte[][] regionsToFlush = new byte[regionCount][];
    for (int i = 0; i < regionCount; i++) {
      regionsToFlush[i] = ProtobufUtil.toBytes(response.getRegionToFlush(i));
    }
    return regionsToFlush;
  }

  @Override
  public synchronized void rollWALWriter(ServerName serverName)
      throws IOException, FailedLogCloseException {
    rollWALWriterImpl(serverName);
  }

  @Override
  public String[] getMasterCoprocessors() {
    try {
      return getClusterStatus().getMasterCoprocessors();
    } catch (IOException e) {
      LOG.error("Could not getClusterStatus()",e);
      return null;
    }
  }

  @Override
  public CompactionState getCompactionState(final TableName tableName)
  throws IOException {
    return getCompactionState(tableName, CompactType.NORMAL);
  }

  @Override
  public CompactionState getCompactionStateForRegion(final byte[] regionName)
  throws IOException {
    final Pair<HRegionInfo, ServerName> regionServerPair = getRegion(regionName);
    if (regionServerPair == null) {
      throw new IllegalArgumentException("Invalid region: " + Bytes.toStringBinary(regionName));
    }
    if (regionServerPair.getSecond() == null) {
      throw new NoServerForRegionException(Bytes.toStringBinary(regionName));
    }
    ServerName sn = regionServerPair.getSecond();
    final AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    // TODO: There is no timeout on this controller. Set one!
    HBaseRpcController controller = rpcControllerFactory.newController();
    GetRegionInfoRequest request = RequestConverter.buildGetRegionInfoRequest(
      regionServerPair.getFirst().getRegionName(), true);
    GetRegionInfoResponse response;
    try {
      response = admin.getRegionInfo(controller, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
    if (response.getCompactionState() != null) {
      return ProtobufUtil.createCompactionState(response.getCompactionState());
    }
    return null;
  }

  @Override
  public void snapshot(final String snapshotName,
                       final TableName tableName) throws IOException,
      SnapshotCreationException, IllegalArgumentException {
    snapshot(snapshotName, tableName, SnapshotType.FLUSH);
  }

  @Override
  public void snapshot(final byte[] snapshotName, final TableName tableName)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    snapshot(Bytes.toString(snapshotName), tableName, SnapshotType.FLUSH);
  }

  @Override
  public void snapshot(final String snapshotName, final TableName tableName,
      SnapshotType type)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    snapshot(new SnapshotDescription(snapshotName, tableName, type));
  }

  @Override
  public void snapshot(SnapshotDescription snapshotDesc)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    // actually take the snapshot
    HBaseProtos.SnapshotDescription snapshot =
      ProtobufUtil.createHBaseProtosSnapshotDesc(snapshotDesc);
    SnapshotResponse response = asyncSnapshot(snapshot);
    final IsSnapshotDoneRequest request =
        IsSnapshotDoneRequest.newBuilder().setSnapshot(snapshot).build();
    IsSnapshotDoneResponse done = null;
    long start = EnvironmentEdgeManager.currentTime();
    long max = response.getExpectedTimeout();
    long maxPauseTime = max / this.numRetries;
    int tries = 0;
    LOG.debug("Waiting a max of " + max + " ms for snapshot '" +
        ClientSnapshotDescriptionUtils.toString(snapshot) + "'' to complete. (max " +
        maxPauseTime + " ms per retry)");
    while (tries == 0
        || ((EnvironmentEdgeManager.currentTime() - start) < max && !done.getDone())) {
      try {
        // sleep a backoff <= pauseTime amount
        long sleep = getPauseTime(tries++);
        sleep = sleep > maxPauseTime ? maxPauseTime : sleep;
        LOG.debug("(#" + tries + ") Sleeping: " + sleep +
          "ms while waiting for snapshot completion.");
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException("Interrupted").initCause(e);
      }
      LOG.debug("Getting current status of snapshot from master...");
      done = executeCallable(new MasterCallable<IsSnapshotDoneResponse>(getConnection(),
          getRpcControllerFactory()) {
        @Override
        protected IsSnapshotDoneResponse rpcCall() throws Exception {
          return master.isSnapshotDone(getRpcController(), request);
        }
      });
    }
    if (!done.getDone()) {
      throw new SnapshotCreationException("Snapshot '" + snapshot.getName()
          + "' wasn't completed in expectedTime:" + max + " ms", snapshotDesc);
    }
  }

  @Override
  public void takeSnapshotAsync(SnapshotDescription snapshotDesc) throws IOException,
      SnapshotCreationException {
    asyncSnapshot(ProtobufUtil.createHBaseProtosSnapshotDesc(snapshotDesc));
  }

  private SnapshotResponse asyncSnapshot(HBaseProtos.SnapshotDescription snapshot)
      throws IOException {
    ClientSnapshotDescriptionUtils.assertSnapshotRequestIsValid(snapshot);
    final SnapshotRequest request = SnapshotRequest.newBuilder().setSnapshot(snapshot)
        .build();
    // run the snapshot on the master
    return executeCallable(new MasterCallable<SnapshotResponse>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected SnapshotResponse rpcCall() throws Exception {
        return master.snapshot(getRpcController(), request);
      }
    });
  }

  @Override
  public boolean isSnapshotFinished(final SnapshotDescription snapshotDesc)
      throws IOException, HBaseSnapshotException, UnknownSnapshotException {
    final HBaseProtos.SnapshotDescription snapshot =
        ProtobufUtil.createHBaseProtosSnapshotDesc(snapshotDesc);
    return executeCallable(new MasterCallable<IsSnapshotDoneResponse>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected IsSnapshotDoneResponse rpcCall() throws Exception {
        return master.isSnapshotDone(getRpcController(),
          IsSnapshotDoneRequest.newBuilder().setSnapshot(snapshot).build());
      }
    }).getDone();
  }

  @Override
  public void restoreSnapshot(final byte[] snapshotName)
      throws IOException, RestoreSnapshotException {
    restoreSnapshot(Bytes.toString(snapshotName));
  }

  @Override
  public void restoreSnapshot(final String snapshotName)
      throws IOException, RestoreSnapshotException {
    boolean takeFailSafeSnapshot =
      conf.getBoolean("hbase.snapshot.restore.take.failsafe.snapshot", false);
    restoreSnapshot(snapshotName, takeFailSafeSnapshot);
  }

  @Override
  public void restoreSnapshot(final byte[] snapshotName, final boolean takeFailSafeSnapshot)
      throws IOException, RestoreSnapshotException {
    restoreSnapshot(Bytes.toString(snapshotName), takeFailSafeSnapshot);
  }

  /*
   * Check whether the snapshot exists and contains disabled table
   *
   * @param snapshotName name of the snapshot to restore
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if no valid snapshot is found
   */
  private TableName getTableNameBeforeRestoreSnapshot(final String snapshotName)
      throws IOException, RestoreSnapshotException {
    TableName tableName = null;
    for (SnapshotDescription snapshotInfo: listSnapshots()) {
      if (snapshotInfo.getName().equals(snapshotName)) {
        tableName = snapshotInfo.getTableName();
        break;
      }
    }

    if (tableName == null) {
      throw new RestoreSnapshotException(
        "Unable to find the table name for snapshot=" + snapshotName);
    }
    return tableName;
  }

  @Override
  public void restoreSnapshot(final String snapshotName, final boolean takeFailSafeSnapshot)
      throws IOException, RestoreSnapshotException {
    TableName tableName = getTableNameBeforeRestoreSnapshot(snapshotName);

    // The table does not exists, switch to clone.
    if (!tableExists(tableName)) {
      cloneSnapshot(snapshotName, tableName);
      return;
    }

    // Check if the table is disabled
    if (!isTableDisabled(tableName)) {
      throw new TableNotDisabledException(tableName);
    }

    // Take a snapshot of the current state
    String failSafeSnapshotSnapshotName = null;
    if (takeFailSafeSnapshot) {
      failSafeSnapshotSnapshotName = conf.get("hbase.snapshot.restore.failsafe.name",
        "hbase-failsafe-{snapshot.name}-{restore.timestamp}");
      failSafeSnapshotSnapshotName = failSafeSnapshotSnapshotName
        .replace("{snapshot.name}", snapshotName)
        .replace("{table.name}", tableName.toString().replace(TableName.NAMESPACE_DELIM, '.'))
        .replace("{restore.timestamp}", String.valueOf(EnvironmentEdgeManager.currentTime()));
      LOG.info("Taking restore-failsafe snapshot: " + failSafeSnapshotSnapshotName);
      snapshot(failSafeSnapshotSnapshotName, tableName);
    }

    try {
      // Restore snapshot
      get(
        internalRestoreSnapshotAsync(snapshotName, tableName),
        syncWaitTimeout,
        TimeUnit.MILLISECONDS);
    } catch (IOException e) {
      // Somthing went wrong during the restore...
      // if the pre-restore snapshot is available try to rollback
      if (takeFailSafeSnapshot) {
        try {
          get(
            internalRestoreSnapshotAsync(failSafeSnapshotSnapshotName, tableName),
            syncWaitTimeout,
            TimeUnit.MILLISECONDS);
          String msg = "Restore snapshot=" + snapshotName +
            " failed. Rollback to snapshot=" + failSafeSnapshotSnapshotName + " succeeded.";
          LOG.error(msg, e);
          throw new RestoreSnapshotException(msg, e);
        } catch (IOException ex) {
          String msg = "Failed to restore and rollback to snapshot=" + failSafeSnapshotSnapshotName;
          LOG.error(msg, ex);
          throw new RestoreSnapshotException(msg, e);
        }
      } else {
        throw new RestoreSnapshotException("Failed to restore snapshot=" + snapshotName, e);
      }
    }

    // If the restore is succeeded, delete the pre-restore snapshot
    if (takeFailSafeSnapshot) {
      try {
        LOG.info("Deleting restore-failsafe snapshot: " + failSafeSnapshotSnapshotName);
        deleteSnapshot(failSafeSnapshotSnapshotName);
      } catch (IOException e) {
        LOG.error("Unable to remove the failsafe snapshot: " + failSafeSnapshotSnapshotName, e);
      }
    }
  }

  @Override
  public Future<Void> restoreSnapshotAsync(final String snapshotName)
      throws IOException, RestoreSnapshotException {
    TableName tableName = getTableNameBeforeRestoreSnapshot(snapshotName);

    // The table does not exists, switch to clone.
    if (!tableExists(tableName)) {
      return cloneSnapshotAsync(snapshotName, tableName);
    }

    // Check if the table is disabled
    if (!isTableDisabled(tableName)) {
      throw new TableNotDisabledException(tableName);
    }

    return internalRestoreSnapshotAsync(snapshotName, tableName);
  }

  @Override
  public void cloneSnapshot(final byte[] snapshotName, final TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException {
    cloneSnapshot(Bytes.toString(snapshotName), tableName);
  }

  @Override
  public void cloneSnapshot(final String snapshotName, final TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException {
    if (tableExists(tableName)) {
      throw new TableExistsException(tableName);
    }
    get(
      internalRestoreSnapshotAsync(snapshotName, tableName),
      Integer.MAX_VALUE,
      TimeUnit.MILLISECONDS);
  }

  @Override
  public Future<Void> cloneSnapshotAsync(final String snapshotName, final TableName tableName)
      throws IOException, TableExistsException {
    if (tableExists(tableName)) {
      throw new TableExistsException(tableName);
    }
    return internalRestoreSnapshotAsync(snapshotName, tableName);
  }

  @Override
  public byte[] execProcedureWithRet(String signature, String instance, Map<String, String> props)
      throws IOException {
    ProcedureDescription.Builder builder = ProcedureDescription.newBuilder();
    builder.setSignature(signature).setInstance(instance);
    for (Entry<String, String> entry : props.entrySet()) {
      NameStringPair pair = NameStringPair.newBuilder().setName(entry.getKey())
          .setValue(entry.getValue()).build();
      builder.addConfiguration(pair);
    }

    final ExecProcedureRequest request = ExecProcedureRequest.newBuilder()
        .setProcedure(builder.build()).build();
    // run the procedure on the master
    ExecProcedureResponse response = executeCallable(new MasterCallable<ExecProcedureResponse>(
        getConnection(), getRpcControllerFactory()) {
      @Override
      protected ExecProcedureResponse rpcCall() throws Exception {
        return master.execProcedureWithRet(getRpcController(), request);
      }
    });

    return response.hasReturnData() ? response.getReturnData().toByteArray() : null;
  }

  @Override
  public void execProcedure(String signature, String instance, Map<String, String> props)
      throws IOException {
    ProcedureDescription.Builder builder = ProcedureDescription.newBuilder();
    builder.setSignature(signature).setInstance(instance);
    for (Entry<String, String> entry : props.entrySet()) {
      NameStringPair pair = NameStringPair.newBuilder().setName(entry.getKey())
          .setValue(entry.getValue()).build();
      builder.addConfiguration(pair);
    }

    final ExecProcedureRequest request = ExecProcedureRequest.newBuilder()
        .setProcedure(builder.build()).build();
    // run the procedure on the master
    ExecProcedureResponse response = executeCallable(new MasterCallable<ExecProcedureResponse>(
        getConnection(), getRpcControllerFactory()) {
      @Override
      protected ExecProcedureResponse rpcCall() throws Exception {
        return master.execProcedure(getRpcController(), request);
      }
    });

    long start = EnvironmentEdgeManager.currentTime();
    long max = response.getExpectedTimeout();
    long maxPauseTime = max / this.numRetries;
    int tries = 0;
    LOG.debug("Waiting a max of " + max + " ms for procedure '" +
        signature + " : " + instance + "'' to complete. (max " + maxPauseTime + " ms per retry)");
    boolean done = false;
    while (tries == 0
        || ((EnvironmentEdgeManager.currentTime() - start) < max && !done)) {
      try {
        // sleep a backoff <= pauseTime amount
        long sleep = getPauseTime(tries++);
        sleep = sleep > maxPauseTime ? maxPauseTime : sleep;
        LOG.debug("(#" + tries + ") Sleeping: " + sleep +
          "ms while waiting for procedure completion.");
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException("Interrupted").initCause(e);
      }
      LOG.debug("Getting current status of procedure from master...");
      done = isProcedureFinished(signature, instance, props);
    }
    if (!done) {
      throw new IOException("Procedure '" + signature + " : " + instance
          + "' wasn't completed in expectedTime:" + max + " ms");
    }
  }

  @Override
  public boolean isProcedureFinished(String signature, String instance, Map<String, String> props)
      throws IOException {
    final ProcedureDescription.Builder builder = ProcedureDescription.newBuilder();
    builder.setSignature(signature).setInstance(instance);
    for (Entry<String, String> entry : props.entrySet()) {
      NameStringPair pair = NameStringPair.newBuilder().setName(entry.getKey())
          .setValue(entry.getValue()).build();
      builder.addConfiguration(pair);
    }
    final ProcedureDescription desc = builder.build();
    return executeCallable(
        new MasterCallable<IsProcedureDoneResponse>(getConnection(), getRpcControllerFactory()) {
          @Override
          protected IsProcedureDoneResponse rpcCall() throws Exception {
            return master.isProcedureDone(getRpcController(), IsProcedureDoneRequest
                .newBuilder().setProcedure(desc).build());
          }
        }).getDone();
  }

  /**
   * Execute Restore/Clone snapshot and wait for the server to complete (blocking).
   * To check if the cloned table exists, use {@link #isTableAvailable} -- it is not safe to
   * create an HTable instance to this table before it is available.
   * @param snapshotName snapshot to restore
   * @param tableName table name to restore the snapshot on
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  private Future<Void> internalRestoreSnapshotAsync(
      final String snapshotName,
      final TableName tableName) throws IOException, RestoreSnapshotException {
    final HBaseProtos.SnapshotDescription snapshot = HBaseProtos.SnapshotDescription.newBuilder()
        .setName(snapshotName).setTable(tableName.getNameAsString()).build();

    // actually restore the snapshot
    ClientSnapshotDescriptionUtils.assertSnapshotRequestIsValid(snapshot);

    RestoreSnapshotResponse response = executeCallable(
        new MasterCallable<RestoreSnapshotResponse>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected RestoreSnapshotResponse rpcCall() throws Exception {
        final RestoreSnapshotRequest request = RestoreSnapshotRequest.newBuilder()
            .setSnapshot(snapshot)
            .setNonceGroup(ng.getNonceGroup())
            .setNonce(ng.newNonce())
            .build();
        return master.restoreSnapshot(getRpcController(), request);
      }
    });

    return new RestoreSnapshotFuture(this, snapshot, tableName, response);
  }

  private static class RestoreSnapshotFuture extends TableFuture<Void> {
    public RestoreSnapshotFuture(
        final HBaseAdmin admin,
        final HBaseProtos.SnapshotDescription snapshot,
        final TableName tableName,
        final RestoreSnapshotResponse response) {
      super(admin, tableName,
          (response != null && response.hasProcId()) ? response.getProcId() : null);

      if (response != null && !response.hasProcId()) {
        throw new UnsupportedOperationException("Client could not call old version of Server");
      }
    }

    public RestoreSnapshotFuture(
        final HBaseAdmin admin,
        final TableName tableName,
        final Long procId) {
      super(admin, tableName, procId);
    }

    @Override
    public String getOperationType() {
      return "MODIFY";
    }
  }

  @Override
  public List<SnapshotDescription> listSnapshots() throws IOException {
    return executeCallable(new MasterCallable<List<SnapshotDescription>>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected List<SnapshotDescription> rpcCall() throws Exception {
        List<HBaseProtos.SnapshotDescription> snapshotsList = master
            .getCompletedSnapshots(getRpcController(),
                GetCompletedSnapshotsRequest.newBuilder().build())
            .getSnapshotsList();
        List<SnapshotDescription> result = new ArrayList<SnapshotDescription>(snapshotsList.size());
        for (HBaseProtos.SnapshotDescription snapshot : snapshotsList) {
          result.add(ProtobufUtil.createSnapshotDesc(snapshot));
        }
        return result;
      }
    });
  }

  @Override
  public List<SnapshotDescription> listSnapshots(String regex) throws IOException {
    return listSnapshots(Pattern.compile(regex));
  }

  @Override
  public List<SnapshotDescription> listSnapshots(Pattern pattern) throws IOException {
    List<SnapshotDescription> matched = new LinkedList<SnapshotDescription>();
    List<SnapshotDescription> snapshots = listSnapshots();
    for (SnapshotDescription snapshot : snapshots) {
      if (pattern.matcher(snapshot.getName()).matches()) {
        matched.add(snapshot);
      }
    }
    return matched;
  }

  @Override
  public List<SnapshotDescription> listTableSnapshots(String tableNameRegex,
      String snapshotNameRegex) throws IOException {
    return listTableSnapshots(Pattern.compile(tableNameRegex), Pattern.compile(snapshotNameRegex));
  }

  @Override
  public List<SnapshotDescription> listTableSnapshots(Pattern tableNamePattern,
      Pattern snapshotNamePattern) throws IOException {
    TableName[] tableNames = listTableNames(tableNamePattern);

    List<SnapshotDescription> tableSnapshots = new LinkedList<SnapshotDescription>();
    List<SnapshotDescription> snapshots = listSnapshots(snapshotNamePattern);

    List<TableName> listOfTableNames = Arrays.asList(tableNames);
    for (SnapshotDescription snapshot : snapshots) {
      if (listOfTableNames.contains(snapshot.getTableName())) {
        tableSnapshots.add(snapshot);
      }
    }
    return tableSnapshots;
  }

  @Override
  public void deleteSnapshot(final byte[] snapshotName) throws IOException {
    deleteSnapshot(Bytes.toString(snapshotName));
  }

  @Override
  public void deleteSnapshot(final String snapshotName) throws IOException {
    // make sure the snapshot is possibly valid
    TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(snapshotName));
    // do the delete
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        master.deleteSnapshot(getRpcController(),
          DeleteSnapshotRequest.newBuilder().setSnapshot(
                HBaseProtos.SnapshotDescription.newBuilder().setName(snapshotName).build())
              .build()
        );
        return null;
      }
    });
  }

  @Override
  public void deleteSnapshots(final String regex) throws IOException {
    deleteSnapshots(Pattern.compile(regex));
  }

  @Override
  public void deleteSnapshots(final Pattern pattern) throws IOException {
    List<SnapshotDescription> snapshots = listSnapshots(pattern);
    for (final SnapshotDescription snapshot : snapshots) {
      try {
        internalDeleteSnapshot(snapshot);
      } catch (IOException ex) {
        LOG.info(
          "Failed to delete snapshot " + snapshot.getName() + " for table " + snapshot.getTableNameAsString(),
          ex);
      }
    }
  }

  private void internalDeleteSnapshot(final SnapshotDescription snapshot) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        this.master.deleteSnapshot(getRpcController(), DeleteSnapshotRequest.newBuilder()
          .setSnapshot(ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot)).build());
        return null;
      }
    });
  }

  @Override
  public void deleteTableSnapshots(String tableNameRegex, String snapshotNameRegex)
      throws IOException {
    deleteTableSnapshots(Pattern.compile(tableNameRegex), Pattern.compile(snapshotNameRegex));
  }

  @Override
  public void deleteTableSnapshots(Pattern tableNamePattern, Pattern snapshotNamePattern)
      throws IOException {
    List<SnapshotDescription> snapshots = listTableSnapshots(tableNamePattern, snapshotNamePattern);
    for (SnapshotDescription snapshot : snapshots) {
      try {
        internalDeleteSnapshot(snapshot);
        LOG.debug("Successfully deleted snapshot: " + snapshot.getName());
      } catch (IOException e) {
        LOG.error("Failed to delete snapshot: " + snapshot.getName(), e);
      }
    }
  }

  @Override
  public void setQuota(final QuotaSettings quota) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        this.master.setQuota(getRpcController(), QuotaSettings.buildSetQuotaRequestProto(quota));
        return null;
      }
    });
  }

  @Override
  public QuotaRetriever getQuotaRetriever(final QuotaFilter filter) throws IOException {
    return QuotaRetriever.open(conf, filter);
  }

  private <C extends RetryingCallable<V> & Closeable, V> V executeCallable(C callable)
      throws IOException {
    return executeCallable(callable, rpcCallerFactory, operationTimeout, rpcTimeout);
  }

  static private <C extends RetryingCallable<V> & Closeable, V> V executeCallable(C callable,
             RpcRetryingCallerFactory rpcCallerFactory, int operationTimeout, int rpcTimeout)
  throws IOException {
    RpcRetryingCaller<V> caller = rpcCallerFactory.newCaller(rpcTimeout);
    try {
      return caller.callWithRetries(callable, operationTimeout);
    } finally {
      callable.close();
    }
  }

  @Override
  // Coprocessor Endpoint against the Master.
  public CoprocessorRpcChannel coprocessorService() {
    return new SyncCoprocessorRpcChannel() {
      @Override
      protected Message callExecService(final RpcController controller,
          final Descriptors.MethodDescriptor method, final Message request,
          final Message responsePrototype)
      throws IOException {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Call: " + method.getName() + ", " + request.toString());
        }
        // Try-with-resources so close gets called when we are done.
        try (MasterCallable<CoprocessorServiceResponse> callable =
            new MasterCallable<CoprocessorServiceResponse>(connection,
                connection.getRpcControllerFactory()) {
          @Override
          protected CoprocessorServiceResponse rpcCall() throws Exception {
            CoprocessorServiceRequest csr =
                CoprocessorRpcUtils.getCoprocessorServiceRequest(method, request);
            return this.master.execMasterService(getRpcController(), csr);
          }
        };) {
          // TODO: Are we retrying here? Does not seem so. We should use RetryingRpcCaller
          callable.prepare(false);
          int operationTimeout = connection.getConnectionConfiguration().getOperationTimeout();
          CoprocessorServiceResponse result = callable.call(operationTimeout);
          return CoprocessorRpcUtils.getResponse(result, responsePrototype);
        }
      }
    };
  }

  /**
   * Simple {@link Abortable}, throwing RuntimeException on abort.
   */
  private static class ThrowableAbortable implements Abortable {
    @Override
    public void abort(String why, Throwable e) {
      throw new RuntimeException(why, e);
    }

    @Override
    public boolean isAborted() {
      return true;
    }
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(final ServerName serverName) {
    return new SyncCoprocessorRpcChannel() {
      @Override
      protected Message callExecService(RpcController controller,
          Descriptors.MethodDescriptor method, Message request, Message responsePrototype)
      throws IOException {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Call: " + method.getName() + ", " + request.toString());
        }
        CoprocessorServiceRequest csr =
            CoprocessorRpcUtils.getCoprocessorServiceRequest(method, request);
        // TODO: Are we retrying here? Does not seem so. We should use RetryingRpcCaller
        // TODO: Make this same as RegionCoprocessorRpcChannel and MasterCoprocessorRpcChannel. They
        // are all different though should do same thing; e.g. RpcChannel setup.
        ClientProtos.ClientService.BlockingInterface stub = connection.getClient(serverName);
        CoprocessorServiceResponse result;
        try {
          result = stub.
              execRegionServerService(connection.getRpcControllerFactory().newController(), csr);
          return CoprocessorRpcUtils.getResponse(result, responsePrototype);
        } catch (ServiceException e) {
          throw ProtobufUtil.handleRemoteException(e);
        }
      }
    };
  }

  @Override
  public void updateConfiguration(final ServerName server) throws IOException {
    final AdminService.BlockingInterface admin = this.connection.getAdmin(server);
    Callable<Void> callable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        admin.updateConfiguration(null, UpdateConfigurationRequest.getDefaultInstance());
        return null;
      }
    };
    ProtobufUtil.call(callable);
  }

  @Override
  public void updateConfiguration() throws IOException {
    for (ServerName server : this.getClusterStatus().getServers()) {
      updateConfiguration(server);
    }

    updateConfiguration(this.getClusterStatus().getMaster());

    for (ServerName server : this.getClusterStatus().getBackupMasters()) {
      updateConfiguration(server);
    }
  }

  @Override
  public int getMasterInfoPort() throws IOException {
    // TODO: Fix!  Reaching into internal implementation!!!!
    ConnectionImplementation connection = (ConnectionImplementation)this.connection;
    ZooKeeperKeepAliveConnection zkw = connection.getKeepAliveZooKeeperWatcher();
    try {
      return MasterAddressTracker.getMasterInfoPort(zkw);
    } catch (KeeperException e) {
      throw new IOException("Failed to get master info port from MasterAddressTracker", e);
    }
  }

  private ServerName getMasterAddress() throws IOException {
    // TODO: Fix!  Reaching into internal implementation!!!!
    ConnectionImplementation connection = (ConnectionImplementation)this.connection;
    ZooKeeperKeepAliveConnection zkw = connection.getKeepAliveZooKeeperWatcher();
    try {
      return MasterAddressTracker.getMasterAddress(zkw);
    } catch (KeeperException e) {
      throw new IOException("Failed to get master server name from MasterAddressTracker", e);
    }
  }

  @Override
  public long getLastMajorCompactionTimestamp(final TableName tableName) throws IOException {
    return executeCallable(new MasterCallable<Long>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Long rpcCall() throws Exception {
        MajorCompactionTimestampRequest req =
            MajorCompactionTimestampRequest.newBuilder()
                .setTableName(ProtobufUtil.toProtoTableName(tableName)).build();
        return master.getLastMajorCompactionTimestamp(getRpcController(), req).
            getCompactionTimestamp();
      }
    });
  }

  @Override
  public long getLastMajorCompactionTimestampForRegion(final byte[] regionName) throws IOException {
    return executeCallable(new MasterCallable<Long>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Long rpcCall() throws Exception {
        MajorCompactionTimestampForRegionRequest req =
            MajorCompactionTimestampForRegionRequest.newBuilder().setRegion(RequestConverter
                      .buildRegionSpecifier(RegionSpecifierType.REGION_NAME, regionName)).build();
        return master.getLastMajorCompactionTimestampForRegion(getRpcController(), req)
            .getCompactionTimestamp();
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void compact(final TableName tableName, final byte[] columnFamily, CompactType compactType)
    throws IOException, InterruptedException {
    compact(tableName, columnFamily, false, compactType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void compact(final TableName tableName, CompactType compactType)
    throws IOException, InterruptedException {
    compact(tableName, null, false, compactType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void majorCompact(final TableName tableName, final byte[] columnFamily,
    CompactType compactType) throws IOException, InterruptedException {
    compact(tableName, columnFamily, true, compactType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void majorCompact(final TableName tableName, CompactType compactType)
          throws IOException, InterruptedException {
    compact(tableName, null, true, compactType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompactionState getCompactionState(final TableName tableName,
    CompactType compactType) throws IOException {
    AdminProtos.GetRegionInfoResponse.CompactionState state =
        AdminProtos.GetRegionInfoResponse.CompactionState.NONE;
    checkTableExists(tableName);
    // TODO: There is no timeout on this controller. Set one!
    final HBaseRpcController rpcController = rpcControllerFactory.newController();
    switch (compactType) {
      case MOB:
        final AdminProtos.AdminService.BlockingInterface masterAdmin =
          this.connection.getAdmin(getMasterAddress());
        Callable<AdminProtos.GetRegionInfoResponse.CompactionState> callable =
            new Callable<AdminProtos.GetRegionInfoResponse.CompactionState>() {
          @Override
          public AdminProtos.GetRegionInfoResponse.CompactionState call() throws Exception {
            HRegionInfo info = getMobRegionInfo(tableName);
            GetRegionInfoRequest request = RequestConverter.buildGetRegionInfoRequest(
                info.getRegionName(), true);
            GetRegionInfoResponse response = masterAdmin.getRegionInfo(rpcController, request);
            return response.getCompactionState();
          }
        };
        state = ProtobufUtil.call(callable);
        break;
      case NORMAL:
      default:
        ZooKeeperWatcher zookeeper = null;
        try {
          List<Pair<HRegionInfo, ServerName>> pairs;
          if (TableName.META_TABLE_NAME.equals(tableName)) {
            zookeeper = new ZooKeeperWatcher(conf, ZK_IDENTIFIER_PREFIX + connection.toString(),
              new ThrowableAbortable());
            pairs = new MetaTableLocator().getMetaRegionsAndLocations(zookeeper);
          } else {
            pairs = MetaTableAccessor.getTableRegionsAndLocations(connection, tableName);
          }
          for (Pair<HRegionInfo, ServerName> pair: pairs) {
            if (pair.getFirst().isOffline()) continue;
            if (pair.getSecond() == null) continue;
            final ServerName sn = pair.getSecond();
            final byte [] regionName = pair.getFirst().getRegionName();
            final AdminService.BlockingInterface snAdmin = this.connection.getAdmin(sn);
            try {
              Callable<GetRegionInfoResponse> regionInfoCallable =
                  new Callable<GetRegionInfoResponse>() {
                @Override
                public GetRegionInfoResponse call() throws Exception {
                  GetRegionInfoRequest request = RequestConverter.buildGetRegionInfoRequest(
                      regionName, true);
                  return snAdmin.getRegionInfo(rpcController, request);
                }
              };
              GetRegionInfoResponse response = ProtobufUtil.call(regionInfoCallable);
              switch (response.getCompactionState()) {
                case MAJOR_AND_MINOR:
                  return CompactionState.MAJOR_AND_MINOR;
                case MAJOR:
                  if (state == AdminProtos.GetRegionInfoResponse.CompactionState.MINOR) {
                    return CompactionState.MAJOR_AND_MINOR;
                  }
                  state = AdminProtos.GetRegionInfoResponse.CompactionState.MAJOR;
                  break;
                case MINOR:
                  if (state == AdminProtos.GetRegionInfoResponse.CompactionState.MAJOR) {
                    return CompactionState.MAJOR_AND_MINOR;
                  }
                  state = AdminProtos.GetRegionInfoResponse.CompactionState.MINOR;
                  break;
                case NONE:
                default: // nothing, continue
              }
            } catch (NotServingRegionException e) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Trying to get compaction state of " +
                        pair.getFirst() + ": " +
                        StringUtils.stringifyException(e));
              }
            } catch (RemoteException e) {
              if (e.getMessage().indexOf(NotServingRegionException.class.getName()) >= 0) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Trying to get compaction state of " + pair.getFirst() + ": "
                          + StringUtils.stringifyException(e));
                }
              } else {
                throw e;
              }
            }
          }
        } finally {
          if (zookeeper != null) {
            zookeeper.close();
          }
        }
        break;
    }
    if(state != null) {
      return ProtobufUtil.createCompactionState(state);
    }
    return null;
  }

  /**
   * Future that waits on a procedure result.
   * Returned by the async version of the Admin calls,
   * and used internally by the sync calls to wait on the result of the procedure.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  protected static class ProcedureFuture<V> implements Future<V> {
    private ExecutionException exception = null;
    private boolean procResultFound = false;
    private boolean done = false;
    private boolean cancelled = false;
    private V result = null;

    private final HBaseAdmin admin;
    private final Long procId;

    public ProcedureFuture(final HBaseAdmin admin, final Long procId) {
      this.admin = admin;
      this.procId = procId;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      AbortProcedureRequest abortProcRequest = AbortProcedureRequest.newBuilder()
          .setProcId(procId).setMayInterruptIfRunning(mayInterruptIfRunning).build();
      try {
        cancelled = abortProcedureResult(abortProcRequest).getIsProcedureAborted();
        if (cancelled) {
          done = true;
        }
      } catch (IOException e) {
        // Cancell thrown exception for some reason. At this time, we are not sure whether
        // the cancell succeeds or fails. We assume that it is failed, but print out a warning
        // for debugging purpose.
        LOG.warn(
          "Cancelling the procedure with procId=" + procId + " throws exception " + e.getMessage(),
          e);
        cancelled = false;
      }
      return cancelled;
    }

    @Override
    public boolean isCancelled() {
      return cancelled;
    }

    protected AbortProcedureResponse abortProcedureResult(
        final AbortProcedureRequest request) throws IOException {
      return admin.executeCallable(new MasterCallable<AbortProcedureResponse>(
          admin.getConnection(), admin.getRpcControllerFactory()) {
        @Override
        protected AbortProcedureResponse rpcCall() throws Exception {
          return master.abortProcedure(getRpcController(), request);
        }
      });
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
      // TODO: should we ever spin forever?
      throw new UnsupportedOperationException();
    }

    @Override
    public V get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if (!done) {
        long deadlineTs = EnvironmentEdgeManager.currentTime() + unit.toMillis(timeout);
        try {
          try {
            // if the master support procedures, try to wait the result
            if (procId != null) {
              result = waitProcedureResult(procId, deadlineTs);
            }
            // if we don't have a proc result, try the compatibility wait
            if (!procResultFound) {
              result = waitOperationResult(deadlineTs);
            }
            result = postOperationResult(result, deadlineTs);
            done = true;
          } catch (IOException e) {
            result = postOperationFailure(e, deadlineTs);
            done = true;
          }
        } catch (IOException e) {
          exception = new ExecutionException(e);
          done = true;
        }
      }
      if (exception != null) {
        throw exception;
      }
      return result;
    }

    @Override
    public boolean isDone() {
      return done;
    }

    protected HBaseAdmin getAdmin() {
      return admin;
    }

    private V waitProcedureResult(long procId, long deadlineTs)
        throws IOException, TimeoutException, InterruptedException {
      GetProcedureResultRequest request = GetProcedureResultRequest.newBuilder()
          .setProcId(procId)
          .build();

      int tries = 0;
      IOException serviceEx = null;
      while (EnvironmentEdgeManager.currentTime() < deadlineTs) {
        GetProcedureResultResponse response = null;
        try {
          // Try to fetch the result
          response = getProcedureResult(request);
        } catch (IOException e) {
          serviceEx = unwrapException(e);

          // the master may be down
          LOG.warn("failed to get the procedure result procId=" + procId, serviceEx);

          // Not much to do, if we have a DoNotRetryIOException
          if (serviceEx instanceof DoNotRetryIOException) {
            // TODO: looks like there is no way to unwrap this exception and get the proper
            // UnsupportedOperationException aside from looking at the message.
            // anyway, if we fail here we just failover to the compatibility side
            // and that is always a valid solution.
            LOG.warn("Proc-v2 is unsupported on this master: " + serviceEx.getMessage(), serviceEx);
            procResultFound = false;
            return null;
          }
        }

        // If the procedure is no longer running, we should have a result
        if (response != null && response.getState() != GetProcedureResultResponse.State.RUNNING) {
          procResultFound = response.getState() != GetProcedureResultResponse.State.NOT_FOUND;
          return convertResult(response);
        }

        try {
          Thread.sleep(getAdmin().getPauseTime(tries++));
        } catch (InterruptedException e) {
          throw new InterruptedException(
            "Interrupted while waiting for the result of proc " + procId);
        }
      }
      if (serviceEx != null) {
        throw serviceEx;
      } else {
        throw new TimeoutException("The procedure " + procId + " is still running");
      }
    }

    private static IOException unwrapException(IOException e) {
      if (e instanceof RemoteException) {
        return ((RemoteException)e).unwrapRemoteException();
      }
      return e;
    }

    protected GetProcedureResultResponse getProcedureResult(final GetProcedureResultRequest request)
        throws IOException {
      return admin.executeCallable(new MasterCallable<GetProcedureResultResponse>(
          admin.getConnection(), admin.getRpcControllerFactory()) {
        @Override
        protected GetProcedureResultResponse rpcCall() throws Exception {
          return master.getProcedureResult(getRpcController(), request);
        }
      });
    }

    /**
     * Convert the procedure result response to a specified type.
     * @param response the procedure result object to parse
     * @return the result data of the procedure.
     */
    protected V convertResult(final GetProcedureResultResponse response) throws IOException {
      if (response.hasException()) {
        throw ForeignExceptionUtil.toIOException(response.getException());
      }
      return null;
    }

    /**
     * Fallback implementation in case the procedure is not supported by the server.
     * It should try to wait until the operation is completed.
     * @param deadlineTs the timestamp after which this method should throw a TimeoutException
     * @return the result data of the operation
     */
    protected V waitOperationResult(final long deadlineTs)
        throws IOException, TimeoutException {
      return null;
    }

    /**
     * Called after the operation is completed and the result fetched. this allows to perform extra
     * steps after the procedure is completed. it allows to apply transformations to the result that
     * will be returned by get().
     * @param result the result of the procedure
     * @param deadlineTs the timestamp after which this method should throw a TimeoutException
     * @return the result of the procedure, which may be the same as the passed one
     */
    protected V postOperationResult(final V result, final long deadlineTs)
        throws IOException, TimeoutException {
      return result;
    }

    /**
     * Called after the operation is terminated with a failure.
     * this allows to perform extra steps after the procedure is terminated.
     * it allows to apply transformations to the result that will be returned by get().
     * The default implementation will rethrow the exception
     * @param exception the exception got from fetching the result
     * @param deadlineTs the timestamp after which this method should throw a TimeoutException
     * @return the result of the procedure, which may be the same as the passed one
     */
    protected V postOperationFailure(final IOException exception, final long deadlineTs)
        throws IOException, TimeoutException {
      throw exception;
    }

    protected interface WaitForStateCallable {
      boolean checkState(int tries) throws IOException;
      void throwInterruptedException() throws InterruptedIOException;
      void throwTimeoutException(long elapsed) throws TimeoutException;
    }

    protected void waitForState(final long deadlineTs, final WaitForStateCallable callable)
        throws IOException, TimeoutException {
      int tries = 0;
      IOException serverEx = null;
      long startTime = EnvironmentEdgeManager.currentTime();
      while (EnvironmentEdgeManager.currentTime() < deadlineTs) {
        serverEx = null;
        try {
          if (callable.checkState(tries)) {
            return;
          }
        } catch (IOException e) {
          serverEx = e;
        }
        try {
          Thread.sleep(getAdmin().getPauseTime(tries++));
        } catch (InterruptedException e) {
          callable.throwInterruptedException();
        }
      }
      if (serverEx != null) {
        throw unwrapException(serverEx);
      } else {
        callable.throwTimeoutException(EnvironmentEdgeManager.currentTime() - startTime);
      }
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  protected static abstract class TableFuture<V> extends ProcedureFuture<V> {
    private final TableName tableName;

    public TableFuture(final HBaseAdmin admin, final TableName tableName, final Long procId) {
      super(admin, procId);
      this.tableName = tableName;
    }

    @Override
    public String toString() {
      return getDescription();
    }

    /**
     * @return the table name
     */
    protected TableName getTableName() {
      return tableName;
    }

    /**
     * @return the table descriptor
     */
    protected HTableDescriptor getTableDescriptor() throws IOException {
      return getAdmin().getTableDescriptorByTableName(getTableName());
    }

    /**
     * @return the operation type like CREATE, DELETE, DISABLE etc.
     */
    public abstract String getOperationType();

    /**
     * @return a description of the operation
     */
    protected String getDescription() {
      return "Operation: " + getOperationType() + ", "
          + "Table Name: " + tableName.getNameWithNamespaceInclAsString();

    };

    protected abstract class TableWaitForStateCallable implements WaitForStateCallable {
      @Override
      public void throwInterruptedException() throws InterruptedIOException {
        throw new InterruptedIOException("Interrupted while waiting for operation: "
            + getOperationType() + " on table: " + tableName.getNameWithNamespaceInclAsString());
      }

      @Override
      public void throwTimeoutException(long elapsedTime) throws TimeoutException {
        throw new TimeoutException("The operation: " + getOperationType() + " on table: " +
            tableName.getNameAsString() + " has not completed after " + elapsedTime + "ms");
      }
    }

    @Override
    protected V postOperationResult(final V result, final long deadlineTs)
        throws IOException, TimeoutException {
      LOG.info(getDescription() + " completed");
      return super.postOperationResult(result, deadlineTs);
    }

    @Override
    protected V postOperationFailure(final IOException exception, final long deadlineTs)
        throws IOException, TimeoutException {
      LOG.info(getDescription() + " failed with " + exception.getMessage());
      return super.postOperationFailure(exception, deadlineTs);
    }

    protected void waitForTableEnabled(final long deadlineTs)
        throws IOException, TimeoutException {
      waitForState(deadlineTs, new TableWaitForStateCallable() {
        @Override
        public boolean checkState(int tries) throws IOException {
          try {
            if (getAdmin().isTableAvailable(tableName)) {
              return true;
            }
          } catch (TableNotFoundException tnfe) {
            LOG.debug("Table " + tableName.getNameWithNamespaceInclAsString()
                + " was not enabled, sleeping. tries=" + tries);
          }
          return false;
        }
      });
    }

    protected void waitForTableDisabled(final long deadlineTs)
        throws IOException, TimeoutException {
      waitForState(deadlineTs, new TableWaitForStateCallable() {
        @Override
        public boolean checkState(int tries) throws IOException {
          return getAdmin().isTableDisabled(tableName);
        }
      });
    }

    protected void waitTableNotFound(final long deadlineTs)
        throws IOException, TimeoutException {
      waitForState(deadlineTs, new TableWaitForStateCallable() {
        @Override
        public boolean checkState(int tries) throws IOException {
          return !getAdmin().tableExists(tableName);
        }
      });
    }

    protected void waitForSchemaUpdate(final long deadlineTs)
        throws IOException, TimeoutException {
      waitForState(deadlineTs, new TableWaitForStateCallable() {
        @Override
        public boolean checkState(int tries) throws IOException {
          return getAdmin().getAlterStatus(tableName).getFirst() == 0;
        }
      });
    }

    protected void waitForAllRegionsOnline(final long deadlineTs, final byte[][] splitKeys)
        throws IOException, TimeoutException {
      final HTableDescriptor desc = getTableDescriptor();
      final AtomicInteger actualRegCount = new AtomicInteger(0);
      final MetaTableAccessor.Visitor visitor = new MetaTableAccessor.Visitor() {
        @Override
        public boolean visit(Result rowResult) throws IOException {
          RegionLocations list = MetaTableAccessor.getRegionLocations(rowResult);
          if (list == null) {
            LOG.warn("No serialized HRegionInfo in " + rowResult);
            return true;
          }
          HRegionLocation l = list.getRegionLocation();
          if (l == null) {
            return true;
          }
          if (!l.getRegionInfo().getTable().equals(desc.getTableName())) {
            return false;
          }
          if (l.getRegionInfo().isOffline() || l.getRegionInfo().isSplit()) return true;
          HRegionLocation[] locations = list.getRegionLocations();
          for (HRegionLocation location : locations) {
            if (location == null) continue;
            ServerName serverName = location.getServerName();
            // Make sure that regions are assigned to server
            if (serverName != null && serverName.getHostAndPort() != null) {
              actualRegCount.incrementAndGet();
            }
          }
          return true;
        }
      };

      int tries = 0;
      int numRegs = (splitKeys == null ? 1 : splitKeys.length + 1) * desc.getRegionReplication();
      while (EnvironmentEdgeManager.currentTime() < deadlineTs) {
        actualRegCount.set(0);
        MetaTableAccessor.scanMetaForTableRegions(getAdmin().getConnection(), visitor,
          desc.getTableName());
        if (actualRegCount.get() == numRegs) {
          // all the regions are online
          return;
        }

        try {
          Thread.sleep(getAdmin().getPauseTime(tries++));
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Interrupted when opening" + " regions; "
              + actualRegCount.get() + " of " + numRegs + " regions processed so far");
        }
      }
      throw new TimeoutException("Only " + actualRegCount.get() + " of " + numRegs
          + " regions are online; retries exhausted.");
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  protected static abstract class NamespaceFuture extends ProcedureFuture<Void> {
    private final String namespaceName;

    public NamespaceFuture(final HBaseAdmin admin, final String namespaceName, final Long procId) {
      super(admin, procId);
      this.namespaceName = namespaceName;
    }

    /**
     * @return the namespace name
     */
    protected String getNamespaceName() {
      return namespaceName;
    }

    /**
     * @return the operation type like CREATE_NAMESPACE, DELETE_NAMESPACE, etc.
     */
    public abstract String getOperationType();

    @Override
    public String toString() {
      return "Operation: " + getOperationType() + ", Namespace: " + getNamespaceName();
    }
  }

  @Override
  public List<SecurityCapability> getSecurityCapabilities() throws IOException {
    try {
      return executeCallable(new MasterCallable<List<SecurityCapability>>(getConnection(),
          getRpcControllerFactory()) {
        @Override
        protected List<SecurityCapability> rpcCall() throws Exception {
          SecurityCapabilitiesRequest req = SecurityCapabilitiesRequest.newBuilder().build();
          return ProtobufUtil.toSecurityCapabilityList(
            master.getSecurityCapabilities(getRpcController(), req).getCapabilitiesList());
        }
      });
    } catch (IOException e) {
      if (e instanceof RemoteException) {
        e = ((RemoteException)e).unwrapRemoteException();
      }
      throw e;
    }
  }

  @Override
  public boolean[] setSplitOrMergeEnabled(final boolean enabled, final boolean synchronous,
                                          final MasterSwitchType... switchTypes)
    throws IOException {
    return executeCallable(new MasterCallable<boolean[]>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected boolean[] rpcCall() throws Exception {
        MasterProtos.SetSplitOrMergeEnabledResponse response =
            master.setSplitOrMergeEnabled(getRpcController(),
                RequestConverter.buildSetSplitOrMergeEnabledRequest(enabled, synchronous,
                    switchTypes));
        boolean[] result = new boolean[switchTypes.length];
        int i = 0;
        for (Boolean prevValue : response.getPrevValueList()) {
          result[i++] = prevValue;
        }
        return result;
      }
    });
  }

  @Override
  public boolean isSplitOrMergeEnabled(final MasterSwitchType switchType) throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        return master.isSplitOrMergeEnabled(getRpcController(),
          RequestConverter.buildIsSplitOrMergeEnabledRequest(switchType)).getEnabled();
      }
    });
  }

  private HRegionInfo getMobRegionInfo(TableName tableName) {
    return new HRegionInfo(tableName, Bytes.toBytes(".mob"),
            HConstants.EMPTY_END_ROW, false, 0);
  }

  private RpcControllerFactory getRpcControllerFactory() {
    return this.rpcControllerFactory;
  }

  @Override
  public void addReplicationPeer(String peerId, ReplicationPeerConfig peerConfig)
      throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        master.addReplicationPeer(getRpcController(),
          RequestConverter.buildAddReplicationPeerRequest(peerId, peerConfig));
        return null;
      }
    });
  }

  @Override
  public void removeReplicationPeer(String peerId) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        master.removeReplicationPeer(getRpcController(),
          RequestConverter.buildRemoveReplicationPeerRequest(peerId));
        return null;
      }
    });
  }

  @Override
  public void enableReplicationPeer(final String peerId) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        master.enableReplicationPeer(getRpcController(),
          RequestConverter.buildEnableReplicationPeerRequest(peerId));
        return null;
      }
    });
  }

  @Override
  public void disableReplicationPeer(final String peerId) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        master.disableReplicationPeer(getRpcController(),
          RequestConverter.buildDisableReplicationPeerRequest(peerId));
        return null;
      }
    });
  }

  @Override
  public ReplicationPeerConfig getReplicationPeerConfig(final String peerId) throws IOException {
    return executeCallable(new MasterCallable<ReplicationPeerConfig>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected ReplicationPeerConfig rpcCall() throws Exception {
        GetReplicationPeerConfigResponse response = master.getReplicationPeerConfig(
          getRpcController(), RequestConverter.buildGetReplicationPeerConfigRequest(peerId));
        return ReplicationSerDeHelper.convert(response.getPeerConfig());
      }
    });
  }

  @Override
  public void updateReplicationPeerConfig(final String peerId,
      final ReplicationPeerConfig peerConfig) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        master.updateReplicationPeerConfig(getRpcController(),
          RequestConverter.buildUpdateReplicationPeerConfigRequest(peerId, peerConfig));
        return null;
      }
    });
  }

  @Override
  public void appendReplicationPeerTableCFs(String id,
      Map<TableName, ? extends Collection<String>> tableCfs) throws ReplicationException,
      IOException {
    if (tableCfs == null) {
      throw new ReplicationException("tableCfs is null");
    }
    ReplicationPeerConfig peerConfig = getReplicationPeerConfig(id);
    Map<TableName, List<String>> preTableCfs = peerConfig.getTableCFsMap();
    if (preTableCfs == null) {
      peerConfig.setTableCFsMap(tableCfs);
    } else {
      for (Map.Entry<TableName, ? extends Collection<String>> entry : tableCfs.entrySet()) {
        TableName table = entry.getKey();
        Collection<String> appendCfs = entry.getValue();
        if (preTableCfs.containsKey(table)) {
          List<String> cfs = preTableCfs.get(table);
          if (cfs == null || appendCfs == null || appendCfs.isEmpty()) {
            preTableCfs.put(table, null);
          } else {
            Set<String> cfSet = new HashSet<String>(cfs);
            cfSet.addAll(appendCfs);
            preTableCfs.put(table, Lists.newArrayList(cfSet));
          }
        } else {
          if (appendCfs == null || appendCfs.isEmpty()) {
            preTableCfs.put(table, null);
          } else {
            preTableCfs.put(table, Lists.newArrayList(appendCfs));
          }
        }
      }
    }
    updateReplicationPeerConfig(id, peerConfig);
  }

  @Override
  public void removeReplicationPeerTableCFs(String id,
      Map<TableName, ? extends Collection<String>> tableCfs) throws ReplicationException,
      IOException {
    if (tableCfs == null) {
      throw new ReplicationException("tableCfs is null");
    }
    ReplicationPeerConfig peerConfig = getReplicationPeerConfig(id);
    Map<TableName, List<String>> preTableCfs = peerConfig.getTableCFsMap();
    if (preTableCfs == null) {
      throw new ReplicationException("Table-Cfs for peer" + id + " is null");
    }
    for (Map.Entry<TableName, ? extends Collection<String>> entry : tableCfs.entrySet()) {

      TableName table = entry.getKey();
      Collection<String> removeCfs = entry.getValue();
      if (preTableCfs.containsKey(table)) {
        List<String> cfs = preTableCfs.get(table);
        if (cfs == null && (removeCfs == null || removeCfs.isEmpty())) {
          preTableCfs.remove(table);
        } else if (cfs != null && (removeCfs != null && !removeCfs.isEmpty())) {
          Set<String> cfSet = new HashSet<String>(cfs);
          cfSet.removeAll(removeCfs);
          if (cfSet.isEmpty()) {
            preTableCfs.remove(table);
          } else {
            preTableCfs.put(table, Lists.newArrayList(cfSet));
          }
        } else if (cfs == null && (removeCfs != null && !removeCfs.isEmpty())) {
          throw new ReplicationException("Cannot remove cf of table: " + table
              + " which doesn't specify cfs from table-cfs config in peer: " + id);
        } else if (cfs != null && (removeCfs == null || removeCfs.isEmpty())) {
          throw new ReplicationException("Cannot remove table: " + table
              + " which has specified cfs from table-cfs config in peer: " + id);
        }
      } else {
        throw new ReplicationException("No table: " + table + " in table-cfs config of peer: " + id);
      }
    }
    updateReplicationPeerConfig(id, peerConfig);
  }

  @Override
  public List<ReplicationPeerDescription> listReplicationPeers() throws IOException {
    return listReplicationPeers((Pattern)null);
  }

  @Override
  public List<ReplicationPeerDescription> listReplicationPeers(String regex) throws IOException {
    return listReplicationPeers(Pattern.compile(regex));
  }

  @Override
  public List<ReplicationPeerDescription> listReplicationPeers(Pattern pattern)
      throws IOException {
    return executeCallable(new MasterCallable<List<ReplicationPeerDescription>>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected List<ReplicationPeerDescription> rpcCall() throws Exception {
        List<ReplicationProtos.ReplicationPeerDescription> peersList = master.listReplicationPeers(
          getRpcController(), RequestConverter.buildListReplicationPeersRequest(pattern))
            .getPeerDescList();
        List<ReplicationPeerDescription> result = new ArrayList<>(peersList.size());
        for (ReplicationProtos.ReplicationPeerDescription peer : peersList) {
          result.add(ReplicationSerDeHelper.toReplicationPeerDescription(peer));
        }
        return result;
      }
    });
  }

  @Override
  public void drainRegionServers(List<ServerName> servers) throws IOException {
    final List<HBaseProtos.ServerName> pbServers = new ArrayList<HBaseProtos.ServerName>(servers.size());
    for (ServerName server : servers) {
      // Parse to ServerName to do simple validation.
      ServerName.parseServerName(server.toString());
      pbServers.add(ProtobufUtil.toServerName(server));
    }

    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      public Void rpcCall() throws ServiceException {
        DrainRegionServersRequest req =
            DrainRegionServersRequest.newBuilder().addAllServerName(pbServers).build();
        master.drainRegionServers(getRpcController(), req);
        return null;
      }
    });
  }

  @Override
  public List<ServerName> listDrainingRegionServers() throws IOException {
    return executeCallable(new MasterCallable<List<ServerName>>(getConnection(),
              getRpcControllerFactory()) {
      @Override
      public List<ServerName> rpcCall() throws ServiceException {
        ListDrainingRegionServersRequest req = ListDrainingRegionServersRequest.newBuilder().build();
        List<ServerName> servers = new ArrayList<ServerName>();
        for (HBaseProtos.ServerName server : master.listDrainingRegionServers(null, req)
            .getServerNameList()) {
          servers.add(ProtobufUtil.toServerName(server));
        }
        return servers;
      }
    });
  }

  @Override
  public void removeDrainFromRegionServers(List<ServerName> servers) throws IOException {
    final List<HBaseProtos.ServerName> pbServers = new ArrayList<HBaseProtos.ServerName>(servers.size());
    for (ServerName server : servers) {
      pbServers.add(ProtobufUtil.toServerName(server));
    }

    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      public Void rpcCall() throws ServiceException {
        RemoveDrainFromRegionServersRequest req = RemoveDrainFromRegionServersRequest.newBuilder()
            .addAllServerName(pbServers).build();
        master.removeDrainFromRegionServers(getRpcController(), req);
        return null;
      }
    });
  }

  @Override
  public List<TableCFs> listReplicatedTableCFs() throws IOException {
    List<TableCFs> replicatedTableCFs = new ArrayList<>();
    HTableDescriptor[] tables = listTables();
    for (HTableDescriptor table : tables) {
      HColumnDescriptor[] columns = table.getColumnFamilies();
      Map<String, Integer> cfs = new HashMap<>();
      for (HColumnDescriptor column : columns) {
        if (column.getScope() != HConstants.REPLICATION_SCOPE_LOCAL) {
          cfs.put(column.getNameAsString(), column.getScope());
        }
      }
      if (!cfs.isEmpty()) {
        replicatedTableCFs.add(new TableCFs(table.getTableName(), cfs));
      }
    }
    return replicatedTableCFs;
  }

  @Override
  public void enableTableReplication(final TableName tableName) throws IOException {
    if (tableName == null) {
      throw new IllegalArgumentException("Table name cannot be null");
    }
    if (!tableExists(tableName)) {
      throw new TableNotFoundException("Table '" + tableName.getNameAsString()
          + "' does not exists.");
    }
    byte[][] splits = getTableSplits(tableName);
    checkAndSyncTableDescToPeers(tableName, splits);
    setTableRep(tableName, true);
  }

  @Override
  public void disableTableReplication(final TableName tableName) throws IOException {
    if (tableName == null) {
      throw new IllegalArgumentException("Table name is null");
    }
    if (!tableExists(tableName)) {
      throw new TableNotFoundException("Table '" + tableName.getNamespaceAsString()
          + "' does not exists.");
    }
    setTableRep(tableName, false);
  }

  /**
   * Copies the REPLICATION_SCOPE of table descriptor passed as an argument. Before copy, the method
   * ensures that the name of table and column-families should match.
   * @param peerHtd descriptor on peer cluster
   * @param localHtd - The HTableDescriptor of table from source cluster.
   * @return true If the name of table and column families match and REPLICATION_SCOPE copied
   *         successfully. false If there is any mismatch in the names.
   */
  private boolean copyReplicationScope(final HTableDescriptor peerHtd,
      final HTableDescriptor localHtd) {
    // Copy the REPLICATION_SCOPE only when table names and the names of
    // Column-Families are same.
    int result = peerHtd.getTableName().compareTo(localHtd.getTableName());

    if (result == 0) {
      Iterator<HColumnDescriptor> remoteHCDIter = peerHtd.getFamilies().iterator();
      Iterator<HColumnDescriptor> localHCDIter = localHtd.getFamilies().iterator();
          
      while (remoteHCDIter.hasNext() && localHCDIter.hasNext()) {
        HColumnDescriptor remoteHCD = remoteHCDIter.next();
        HColumnDescriptor localHCD = localHCDIter.next();
        
        String remoteHCDName = remoteHCD.getNameAsString();
        String localHCDName = localHCD.getNameAsString();

        if (remoteHCDName.equals(localHCDName)) {
          remoteHCD.setScope(localHCD.getScope());
        } else {
          result = -1;
          break;
        }
      }
      if (remoteHCDIter.hasNext() || localHCDIter.hasNext()) {
        return false;
      }
    }

    return result == 0;
  }

  /**
   * Compare the contents of the descriptor with another one passed as a parameter for replication
   * purpose. The REPLICATION_SCOPE field is ignored during comparison.
   * @param peerHtd descriptor on peer cluster
   * @param localHtd descriptor on source cluster which needs to be replicated.
   * @return true if the contents of the two descriptors match (ignoring just REPLICATION_SCOPE).
   * @see java.lang.Object#equals(java.lang.Object)
   */
  private boolean compareForReplication(HTableDescriptor peerHtd, HTableDescriptor localHtd) {
    if (peerHtd == localHtd) {
      return true;
    }
    if (peerHtd == null) {
      return false;
    }
    boolean result = false;

    // Create a copy of peer HTD as we need to change its replication
    // scope to match with the local HTD.
    HTableDescriptor peerHtdCopy = new HTableDescriptor(peerHtd);

    result = copyReplicationScope(peerHtdCopy, localHtd);

    // If copy was successful, compare the two tables now.
    if (result) {
      result = (peerHtdCopy.compareTo(localHtd) == 0);
    }

    return result;
  }

  /**
   * Connect to peer and check the table descriptor on peer:
   * <ol>
   * <li>Create the same table on peer when not exist.</li>
   * <li>Throw an exception if the table already has replication enabled on any of the column
   * families.</li>
   * <li>Throw an exception if the table exists on peer cluster but descriptors are not same.</li>
   * </ol>
   * @param tableName name of the table to sync to the peer
   * @param splits table split keys
   * @throws IOException
   */
  private void checkAndSyncTableDescToPeers(final TableName tableName, final byte[][] splits)
      throws IOException {
    List<ReplicationPeerDescription> peers = listReplicationPeers();
    if (peers == null || peers.size() <= 0) {
      throw new IllegalArgumentException("Found no peer cluster for replication.");
    }

    for (ReplicationPeerDescription peerDesc : peers) {
      if (needToReplicate(tableName, peerDesc)) {
        Configuration peerConf = getPeerClusterConfiguration(peerDesc);
        try (Connection conn = ConnectionFactory.createConnection(peerConf);
            Admin repHBaseAdmin = conn.getAdmin()) {
          HTableDescriptor localHtd = getTableDescriptor(tableName);
          HTableDescriptor peerHtd = null;
          if (!repHBaseAdmin.tableExists(tableName)) {
            repHBaseAdmin.createTable(localHtd, splits);
          } else {
            peerHtd = repHBaseAdmin.getTableDescriptor(tableName);
            if (peerHtd == null) {
              throw new IllegalArgumentException("Failed to get table descriptor for table "
                  + tableName.getNameAsString() + " from peer cluster " + peerDesc.getPeerId());
            } else {
              // To support cyclic replication (HBASE-17460), we need to match the
              // REPLICATION_SCOPE of table on both the clusters. We should do this
              // only when the replication is not already enabled on local HTD (local
              // table on this cluster).
              //
              if (localHtd.isReplicationEnabled()) {
                throw new IllegalArgumentException("Table " + tableName.getNameAsString()
                    + " has replication already enabled for at least one Column Family.");
              } else {
                if (!compareForReplication(peerHtd, localHtd)) {
                  throw new IllegalArgumentException("Table " + tableName.getNameAsString()
                      + " exists in peer cluster " + peerDesc.getPeerId()
                      + ", but the table descriptors are not same when compared with source cluster."
                      + " Thus can not enable the table's replication switch.");
                }
              }
            }
          }
        }
      }
    }
  }

  /**
   * Decide whether the table need replicate to the peer cluster according to the peer config
   * @param table name of the table
   * @param peerConfig config for the peer
   * @return true if the table need replicate to the peer cluster
   */
  private boolean needToReplicate(TableName table, ReplicationPeerDescription peer) {
    ReplicationPeerConfig peerConfig = peer.getPeerConfig();
    Set<String> namespaces = peerConfig.getNamespaces();
    Map<TableName, List<String>> tableCFsMap = peerConfig.getTableCFsMap();
    // If null means user has explicitly not configured any namespaces and table CFs
    // so all the tables data are applicable for replication
    if (namespaces == null && tableCFsMap == null) {
      return true;
    }
    if (namespaces != null && namespaces.contains(table.getNamespaceAsString())) {
      return true;
    }
    if (tableCFsMap != null && tableCFsMap.containsKey(table)) {
      return true;
    }
    LOG.debug("Table " + table.getNameAsString()
        + " doesn't need replicate to peer cluster, peerId=" + peer.getPeerId() + ", clusterKey="
        + peerConfig.getClusterKey());
    return false;
  }

  /**
   * Set the table's replication switch if the table's replication switch is already not set.
   * @param tableName name of the table
   * @param isRepEnabled is replication switch enable or disable
   * @throws IOException if a remote or network exception occurs
   */
  private void setTableRep(final TableName tableName, boolean isRepEnabled) throws IOException {
    HTableDescriptor htd = getTableDescriptor(tableName);
    if (isTableRepEnabled(htd) ^ isRepEnabled) {
      for (HColumnDescriptor hcd : htd.getFamilies()) {
        hcd.setScope(isRepEnabled ? HConstants.REPLICATION_SCOPE_GLOBAL
            : HConstants.REPLICATION_SCOPE_LOCAL);
      }
      modifyTable(tableName, htd);
    }
  }

  /**
   * @param htd table descriptor details for the table to check
   * @return true if table's replication switch is enabled
   */
  private boolean isTableRepEnabled(HTableDescriptor htd) {
    for (HColumnDescriptor hcd : htd.getFamilies()) {
      if (hcd.getScope() != HConstants.REPLICATION_SCOPE_GLOBAL
          && hcd.getScope() != HConstants.REPLICATION_SCOPE_SERIAL) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns the configuration needed to talk to the remote slave cluster.
   * @param peer the description of replication peer
   * @return the configuration for the peer cluster, null if it was unable to get the configuration
   * @throws IOException
   */
  private Configuration getPeerClusterConfiguration(ReplicationPeerDescription peer)
      throws IOException {
    ReplicationPeerConfig peerConfig = peer.getPeerConfig();
    Configuration otherConf;
    try {
      otherConf = HBaseConfiguration.createClusterConf(this.conf, peerConfig.getClusterKey());
    } catch (IOException e) {
      throw new IOException("Can't get peer configuration for peerId=" + peer.getPeerId(), e);
    }

    if (!peerConfig.getConfiguration().isEmpty()) {
      CompoundConfiguration compound = new CompoundConfiguration();
      compound.add(otherConf);
      compound.addStringMap(peerConfig.getConfiguration());
      return compound;
    }

    return otherConf;
  }
}
