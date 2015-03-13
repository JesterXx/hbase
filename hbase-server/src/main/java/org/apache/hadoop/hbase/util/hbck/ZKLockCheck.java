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
package org.apache.hadoop.hbase.util.hbck;

import java.io.IOException;

import org.apache.hadoop.hbase.InterProcessLock.MetadataHandler;
import org.apache.hadoop.hbase.master.ZKLockManager;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * Utility to check and fix zk locks. Need zookeeper connection.
 */
public class ZKLockCheck {

  private ZooKeeperWatcher zkWatcher;
  private ErrorReporter errorReporter;
  long expireTimeout;

  public ZKLockCheck(ZooKeeperWatcher zkWatcher, ErrorReporter errorReporter) {
    this.zkWatcher = zkWatcher;
    this.errorReporter = errorReporter;
    expireTimeout = zkWatcher.getConfiguration().getLong(
        ZKLockManager.ZK_LOCK_EXPIRE_TIMEOUT,
        ZKLockManager.DEFAULT_ZK_LOCK_EXPIRE_TIMEOUT_MS);
  }

  public void checkZKLocks() throws IOException {
    ZKLockManager zkLockManager = ZKLockManager.createZKLockManager(zkWatcher.getConfiguration(),
      zkWatcher, null);
    final long expireDate = EnvironmentEdgeManager.currentTime() - expireTimeout;

    MetadataHandler handler = new MetadataHandler() {
      @Override
      public void handleMetadata(byte[] ownerMetadata) {
        ZooKeeperProtos.ZKLock data = ZKLockManager.fromBytes(ownerMetadata);
        String msg = "ZK lock acquire attempt found:";
        if (data != null) {
           msg = msg +
              String.format("[lockName=%s, lockOwner=%s, threadId=%s, " +
              "purpose=%s, isShared=%s, createTime=%s]", data.getLockName(),
              ProtobufUtil.toServerName(data.getLockOwner()), data.getThreadId(),
              data.getPurpose(), data.getIsShared(), data.getCreateTime());
        }
        if (data != null && data.hasCreateTime() && data.getCreateTime() < expireDate) {
          errorReporter.reportError(HBaseFsck.ErrorReporter.ERROR_CODE.EXPIRED_ZK_LOCK, msg);
        } else {
          errorReporter.print(msg);
        }
      }
    };

    zkLockManager.visitAllLocks(handler);
  }

  public void fixExpiredZKLocks() throws IOException {
    ZKLockManager zkLockManager = ZKLockManager.createZKLockManager(zkWatcher.getConfiguration(),
      zkWatcher, null);
    zkLockManager.reapAllExpiredLocks();
  }
}
