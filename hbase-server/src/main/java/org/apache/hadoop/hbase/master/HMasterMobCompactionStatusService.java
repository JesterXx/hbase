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

import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.generated.MasterMobCompactionStatusProtos.GetMobCompactRegionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterMobCompactionStatusProtos.GetMobCompactRegionsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterMobCompactionStatusProtos.MasterMobCompactionStatusService;
import org.apache.hadoop.hbase.protobuf.generated.MasterMobCompactionStatusProtos.UpdateMobCompactionAsMajorRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterMobCompactionStatusProtos.UpdateMobCompactionAsMajorResponse;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

public class HMasterMobCompactionStatusService extends MasterMobCompactionStatusService {

  private HMaster master;

  public HMasterMobCompactionStatusService(HMaster master) {
    this.master = master;
  }

  @Override
  public void getMobCompactRegions(RpcController controller, GetMobCompactRegionsRequest request,
    RpcCallback<GetMobCompactRegionsResponse> done) {
    String tableNameAsString = request.getTableName();
    String serverName = request.getServerName();
    TableName tableName = TableName.valueOf(tableNameAsString);
    List<String> regionStartKeys = master.mobCompactionManager.getCompactingRegions(tableName,
      serverName);
    GetMobCompactRegionsResponse.Builder builder = GetMobCompactRegionsResponse.newBuilder();
    if (!regionStartKeys.isEmpty()) {
      builder.addAllRegionStartKey(regionStartKeys);
    }
    done.run(builder.build());
  }

  @Override
  public void updateMobCompactionAsMajor(RpcController controller,
    UpdateMobCompactionAsMajorRequest request,
    RpcCallback<UpdateMobCompactionAsMajorResponse> done) {
    String tableNameAsString = request.getTableName();
    String serverName = request.getServerName();
    TableName tableName = TableName.valueOf(tableNameAsString);
    master.mobCompactionManager.updateAsMajorCompaction(tableName, serverName);
    done.run(UpdateMobCompactionAsMajorResponse.getDefaultInstance());
  }
}