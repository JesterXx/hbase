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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.InterProcessLock;
import org.apache.hadoop.hbase.InterProcessLock.MetadataHandler;
import org.apache.hadoop.hbase.InterProcessReadWriteLock;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.LockTimeoutException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.hbase.zookeeper.lock.ZKInterProcessReadWriteLock;
import org.apache.zookeeper.KeeperException;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A manager for distributed locks.
 */
public class ZKLockManager {

  private static final Log LOG = LogFactory.getLog(ZKLockManager.class);

  /** Configuration key for time out for trying to acquire locks */
  protected static final String ZK_WRITE_LOCK_TIMEOUT_MS = "hbase.zk.write.lock.timeout.ms";

  /** Configuration key for time out for trying to acquire locks */
  protected static final String ZK_READ_LOCK_TIMEOUT_MS = "hbase.zk.read.lock.timeout.ms";

  protected static final long DEFAULT_ZK_WRITE_LOCK_TIMEOUT_MS = 600 * 1000; // 10 min default

  protected static final long DEFAULT_ZK_READ_LOCK_TIMEOUT_MS = 600 * 1000; // 10 min default

  public static final String ZK_LOCK_EXPIRE_TIMEOUT = "hbase.zk.lock.expire.ms";

  public static final long DEFAULT_ZK_LOCK_EXPIRE_TIMEOUT_MS = 600 * 1000; // 10 min default

  private static final MetadataHandler METADATA_HANDLER = new MetadataHandler() {
    @Override
    public void handleMetadata(byte[] ownerMetadata) {
      if (!LOG.isDebugEnabled()) {
        return;
      }
      ZooKeeperProtos.ZKLock data = fromBytes(ownerMetadata);
      if (data == null) {
        return;
      }
      LOG.debug("Table is locked by "
        + String.format("[lockName=%s, lockOwner=%s, threadId=%s, "
          + "purpose=%s, isShared=%s, createTime=%s]", data.getLockName(),
          ProtobufUtil.toServerName(data.getLockOwner()), data.getThreadId(), data.getPurpose(),
          data.getIsShared(), data.getCreateTime()));
    }
  };

  private final ServerName serverName;
  private final ZooKeeperWatcher zkWatcher;
  private final long writeLockTimeoutMs;
  private final long readLockTimeoutMs;
  private final long lockExpireTimeoutMs;

  /**
   * Initialize a new manager for zk locks.
   * @param zkWatcher
   * @param serverName Address of the server responsible for acquiring and
   * releasing the zk locks
   * @param writeLockTimeoutMs Timeout (in milliseconds) for acquiring a write lock,
   * or -1 for no timeout
   * @param readLockTimeoutMs Timeout (in milliseconds) for acquiring a read lock,
   * or -1 for no timeout
   */
  public ZKLockManager(ZooKeeperWatcher zkWatcher, ServerName serverName, long writeLockTimeoutMs,
    long readLockTimeoutMs, long lockExpireTimeoutMs) {
    this.zkWatcher = zkWatcher;
    this.serverName = serverName;
    this.writeLockTimeoutMs = writeLockTimeoutMs;
    this.readLockTimeoutMs = readLockTimeoutMs;
    this.lockExpireTimeoutMs = lockExpireTimeoutMs;
  }

  /**
   * Returns a ZKLock for exclusive access
   * @param lockName The lock name.
   * @param purpose Human readable reason for locking the table
   * @return A new ZKLock object for acquiring a write lock
   */
  public ZKLock writeLock(String lockName, String purpose) {
    return new ZKLock(lockName, zkWatcher, serverName, writeLockTimeoutMs, false, purpose);
  }

  /**
   * Returns a ZKLock for locking the table for shared access among read-lock holders
   * @param lockName The lock name.
   * @param purpose Human readable reason for locking the table
   * @return A new ZKLock object for acquiring a read lock
   */
  public ZKLock readLock(String lockName, String purpose) {
    return new ZKLock(lockName, zkWatcher, serverName, readLockTimeoutMs, true, purpose);
  }

  /**
   * Visits all zk locks(read and write), and lock attempts with the given callback
   * MetadataHandler.
   * @param handler the metadata handler to call
   * @throws IOException If there is an unrecoverable error
   */
  public void visitAllLocks(MetadataHandler handler) throws IOException {
    for (String lockName : getLockNames()) {
      String zkLockZNode = ZKUtil.joinZNode(zkWatcher.zkLockZNode, lockName);
      ZKInterProcessReadWriteLock lock = new ZKInterProcessReadWriteLock(zkWatcher, zkLockZNode,
        null);
      lock.readLock(null).visitLocks(handler);
      lock.writeLock(null).visitLocks(handler);
    }
  }

  /**
   * Force releases zk write locks and lock attempts even if this thread does
   * not own the lock. The behavior of the lock holders still thinking that they
   * have the lock is undefined. This should be used carefully and only when
   * we can ensure that all write-lock holders have died. For example if only
   * the master can hold write locks, then we can reap it's locks when the backup
   * master starts.
   * @throws IOException If there is an unrecoverable error
   */
  public void reapWriteLocks() throws IOException {
    // get the lock names
    try {
      for (String lockName : getLockNames()) {
        String lockZNode = ZKUtil.joinZNode(zkWatcher.zkLockZNode, lockName);
        ZKInterProcessReadWriteLock lock = new ZKInterProcessReadWriteLock(zkWatcher,
          lockZNode, null);
        lock.writeLock(null).reapAllLocks();
      }
    } catch (IOException ex) {
      throw ex;
    } catch (Exception ex) {
      LOG.warn("Caught exception while reaping table write locks", ex);
    }
  }

  /**
   * Force releases all zk locks(read and write) that have been held longer than
   * "hbase.zk.lock.expire.ms". Assumption is that the clock skew between zookeeper
   * and this servers is negligible.
   * The behavior of the lock holders still thinking that they have the lock is undefined.
   * @throws IOException If there is an unrecoverable error
   */
  public void reapAllExpiredLocks() throws IOException {
    // get the lock names
    try {
      for (String lockName : getLockNames()) {
        String lockZNode = ZKUtil.joinZNode(zkWatcher.zkLockZNode, lockName);
        ZKInterProcessReadWriteLock lock = new ZKInterProcessReadWriteLock(zkWatcher,
          lockZNode, null);
        lock.readLock(null).reapExpiredLocks(lockExpireTimeoutMs);
        lock.writeLock(null).reapExpiredLocks(lockExpireTimeoutMs);
      }
    } catch (IOException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  /**
   * Creates and returns a ZKLockManager according to the configuration
   */
  public static ZKLockManager createZKLockManager(Configuration conf,
    ZooKeeperWatcher zkWatcher, ServerName serverName) {
    long writeLockTimeoutMs = conf.getLong(ZK_WRITE_LOCK_TIMEOUT_MS,
      DEFAULT_ZK_WRITE_LOCK_TIMEOUT_MS);
    long readLockTimeoutMs =
      conf.getLong(ZK_READ_LOCK_TIMEOUT_MS, DEFAULT_ZK_READ_LOCK_TIMEOUT_MS);
    long lockExpireTimeoutMs = conf.getLong(ZK_LOCK_EXPIRE_TIMEOUT,
      DEFAULT_ZK_LOCK_EXPIRE_TIMEOUT_MS);

    return new ZKLockManager(zkWatcher, serverName, writeLockTimeoutMs, readLockTimeoutMs,
      lockExpireTimeoutMs);
  }

  /** Public for hbck */
  public static ZooKeeperProtos.ZKLock fromBytes(byte[] bytes) {
    int pblen = ProtobufUtil.lengthOfPBMagic();
    if (bytes == null || bytes.length < pblen) {
      return null;
    }
    try {
      ZooKeeperProtos.ZKLock data = ZooKeeperProtos.ZKLock.newBuilder()
        .mergeFrom(bytes, pblen, bytes.length - pblen).build();
      return data;
    } catch (InvalidProtocolBufferException ex) {
      LOG.warn("Exception in deserialization", ex);
    }
    return null;
  }

  private static byte[] toBytes(ZooKeeperProtos.ZKLock data) {
    return ProtobufUtil.prependPBMagic(data.toByteArray());
  }

  /**
   * A distributed lock for a table.
   */
  @InterfaceAudience.Private
  public class ZKLock {
    long lockTimeoutMs;
    String lockName;
    InterProcessLock lock;
    boolean isShared;
    ZooKeeperWatcher zkWatcher;
    ServerName serverName;
    String purpose;

    public ZKLock(String lockName, ZooKeeperWatcher zkWatcher, ServerName serverName,
      long lockTimeoutMs, boolean isShared, String purpose) {
      this.lockName = lockName;
      this.zkWatcher = zkWatcher;
      this.serverName = serverName;
      this.lockTimeoutMs = lockTimeoutMs;
      this.isShared = isShared;
      this.purpose = purpose;
    }

    /**
     * Acquires the lock, with the configured lock timeout.
     * @throws LockTimeoutException If unable to acquire a lock within a specified
     * time period (if any)
     * @throws IOException If unrecoverable error occurs
     */
    public void acquire() throws IOException {
      acquire(lockTimeoutMs);
    }

    /**
     * Acquires the lock, with the current lock timeout.
     * @param timeout The current timeout
     * @throws LockTimeoutException If unable to acquire a lock within a specified
     * time period (if any)
     * @throws IOException If unrecoverable error occurs
     */
    public void acquire(long timeout) throws IOException {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Attempt to acquire table " + (isShared ? "read" : "write") + " lock on: "
          + lockName + " for:" + purpose);
      }

      lock = createZKLock();
      try {
        if (lockTimeoutMs == -1) {
          // Wait indefinitely
          lock.acquire();
        } else {
          if (!lock.tryAcquire(lockTimeoutMs)) {
            throw new LockTimeoutException("Timed out acquiring " + (isShared ? "read" : "write")
              + "lock:" + lockName + "for:" + purpose + " after " + lockTimeoutMs + " ms.");
          }
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted acquiring a lock for " + lockName, e);
        Thread.currentThread().interrupt();
        throw new InterruptedIOException("Interrupted acquiring a lock");
      }
      if (LOG.isTraceEnabled())
        LOG.trace("Acquired table " + (isShared ? "read" : "write") + " lock on " + lockName
          + " for " + purpose);
    }

    /**
     * Releases the lock already held.
     * @throws IOException If there is an unrecoverable error releasing the lock
     */
    public void release() throws IOException {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Attempt to release table " + (isShared ? "read" : "write") + " lock on "
          + lockName);
      }
      if (lock == null) {
        throw new IllegalStateException("Lock " + lockName + " is not locked!");
      }

      try {
        lock.release();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while releasing a lock for " + lockName);
        throw new InterruptedIOException();
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("Released table lock on " + lockName);
      }
    }

    private InterProcessLock createZKLock() {
      String lockZNode = ZKUtil.joinZNode(zkWatcher.zkLockZNode, lockName);

      ZooKeeperProtos.ZKLock data = ZooKeeperProtos.ZKLock.newBuilder().setLockName(lockName)
        .setLockOwner(ProtobufUtil.toServerName(serverName))
        .setThreadId(Thread.currentThread().getId()).setPurpose(purpose).setIsShared(isShared)
        .setCreateTime(EnvironmentEdgeManager.currentTime()).build();
      byte[] lockMetadata = toBytes(data);

      InterProcessReadWriteLock lock = new ZKInterProcessReadWriteLock(zkWatcher, lockZNode,
        METADATA_HANDLER);
      return isShared ? lock.readLock(lockMetadata) : lock.writeLock(lockMetadata);
    }
  }

  private List<String> getLockNames() throws IOException {
    List<String> lockNames;
    try {
      lockNames = ZKUtil.listChildrenNoWatch(zkWatcher, zkWatcher.zkLockZNode);
    } catch (KeeperException e) {
      LOG.error("Unexpected ZooKeeper error when listing children", e);
      throw new IOException("Unexpected ZooKeeper exception", e);
    }
    return lockNames;
  }
}
