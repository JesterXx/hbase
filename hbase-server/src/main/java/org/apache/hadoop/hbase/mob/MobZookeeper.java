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
package org.apache.hadoop.hbase.mob;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

public class MobZookeeper {

  private static final Log LOG = LogFactory.getLog(MobZookeeper.class);

  private ZooKeeperWatcher zkw;
  private String mobZnode;
  private static final String EPHEMERAL = "-ephemeral";

  public MobZookeeper(Configuration conf) throws IOException, KeeperException {
    zkw = new ZooKeeperWatcher(conf, "LobZookeeper", new DummyLobAbortable());
    mobZnode = ZKUtil.joinZNode(zkw.baseZNode, "MOB");
    if (ZKUtil.checkExists(zkw, mobZnode) == -1) {
      ZKUtil.createWithParents(zkw, mobZnode);
    }
  }

  public static MobZookeeper newInstance(Configuration conf) throws IOException, KeeperException {
    return new MobZookeeper(conf);
  }

  public boolean lockStore(String tableName, String storeName) {
    String lockName = MobUtils.getStoreLockName(tableName, storeName);
    boolean locked = false;
    try {
      locked = ZKUtil.createEphemeralNodeAndWatch(zkw,
          ZKUtil.joinZNode(mobZnode, lockName + EPHEMERAL), null);
      if (LOG.isDebugEnabled()) {
        LOG.debug(locked ? "Locked the store " + lockName : "Can not lock the store " + lockName);
      }
    } catch (KeeperException e) {
      LOG.error("Fail to lock the store " + lockName, e);
    }
    return locked;
  }

  public void unlockStore(String tableName, String storeName) {
    String lockName = MobUtils.getStoreLockName(tableName, storeName);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Unlocking the store " + lockName);
    }
    try {
      ZKUtil.deleteNode(zkw, ZKUtil.joinZNode(mobZnode, lockName + EPHEMERAL));
    } catch (KeeperException e) {
      LOG.warn("Fail to unlock the store " + lockName, e);
    }
  }

  public void close() {
    this.zkw.close();
  }

  private static class DummyLobAbortable implements Abortable {

    private boolean abort = false;

    public void abort(String why, Throwable e) {
      abort = true;
    }

    public boolean isAborted() {
      return abort;
    }

  }
}
