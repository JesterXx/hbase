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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DaemonThreadFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.master.MasterMobCompactionManager;
import org.apache.hadoop.hbase.procedure.ProcedureMember;
import org.apache.hadoop.hbase.procedure.ProcedureMemberRpcs;
import org.apache.hadoop.hbase.procedure.RegionServerProcedureManager;
import org.apache.hadoop.hbase.procedure.Subprocedure;
import org.apache.hadoop.hbase.procedure.SubprocedureFactory;
import org.apache.hadoop.hbase.procedure.ZKProcedureMemberRpcs;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * This manager class handles mob compaction for table on a {@link HRegionServer}.
 */
public class RegionServerMobCompactionProcedureManager extends RegionServerProcedureManager {
  private static final Log LOG = LogFactory.getLog(RegionServerMobCompactionProcedureManager.class);

  private RegionServerServices rss;
  private ProcedureMemberRpcs memberRpcs;
  private ProcedureMember member;
  private static final String CONCURENT_MOB_COMPACTION_TASKS_KEY =
    "hbase.mob.compaction.procedure.concurrentTasks";
  private static final int DEFAULT_CONCURRENT_MOB_COMPACTION_TASKS = 10;
  public static final String MOB_COMPACTION_THREADS_KEY =
    "hbase.mob.compaction.procedure.pool.threads";
  public static final int FLUSH_REQUEST_THREADS_DEFAULT = 5;

  public static final String MOB_COMPACTION_TIMEOUT_MILLIS_KEY =
    "hbase.mob.compaction.procedure.timeout";
  public static final long MOB_COMPACTION_TIMEOUT_MILLIS_DEFAULT = 60000;

  public static final String MOB_COMPACTION_WAKE_MILLIS_KEY =
    "hbase.mob.compaction.procedure.wakefrequency";
  private static final long MOB_COMPACTION_WAKE_MILLIS_DEFAULT = 500;

  @Override
  public void initialize(RegionServerServices rss) throws KeeperException {
    this.rss = rss;
    ZooKeeperWatcher zkw = rss.getZooKeeper();
    this.memberRpcs = new ZKProcedureMemberRpcs(zkw,
      MasterMobCompactionManager.MOB_COMPACTION_PROCEDURE_SIGNATURE);

    Configuration conf = rss.getConfiguration();
    long keepAlive = conf.getLong(MOB_COMPACTION_TIMEOUT_MILLIS_KEY,
      MOB_COMPACTION_TIMEOUT_MILLIS_DEFAULT);
    int opThreads = conf.getInt(MOB_COMPACTION_THREADS_KEY, FLUSH_REQUEST_THREADS_DEFAULT);

    // create the actual mob compaction procedure member
    ThreadPoolExecutor pool = ProcedureMember.defaultPool(rss.getServerName().toString(),
      opThreads, keepAlive);
    this.member = new ProcedureMember(memberRpcs, pool, new MobCompactionSubprocedureBuilder());
  }

  /**
   * Starts accepting mob compaction requests.
   */
  @Override
  public void start() {
    LOG.debug("Start region server mob compaction procedure manager "
      + rss.getServerName().toString());
    this.memberRpcs.start(rss.getServerName().toString(), member);
  }

  /**
   * Closes <tt>this</tt> and all running tasks
   * @param force forcefully stop all running tasks
   * @throws IOException
   */
  @Override
  public void stop(boolean force) throws IOException {
    String mode = force ? "abruptly" : "gracefully";
    LOG.info("Stopping region server mob compaction procedure manager " + mode + ".");

    try {
      this.member.close();
    } finally {
      this.memberRpcs.close();
    }
  }

  @Override
  public String getProcedureSignature() {
    return MasterMobCompactionManager.MOB_COMPACTION_PROCEDURE_SIGNATURE;
  }

  /**
   * Creates a specified subprocedure to compact mob files.
   *
   * @param table the current table name.
   * @param data the arguments passed in master side.
   * @return Subprocedure to submit to the ProcedureMemeber.
   */
  public Subprocedure buildSubprocedure(TableName tableName, byte[] data) {
    // don't run the subprocedure if the parent is stop(ping)
    if (rss.isStopping() || rss.isStopped()) {
      throw new IllegalStateException("Can't start mob compaction subprocedure on RS: "
        + rss.getServerName() + ", because stopping/stopped!");
    }
    // check to see if this server is hosting any regions for the table
    List<Region> involvedRegions;
    try {
      involvedRegions = rss.getOnlineRegions(tableName);
    } catch (IOException e1) {
      throw new IllegalStateException(
        "Failed to figure out if there is region for mob compaction.", e1);
    }
    // parse the column names and if it is a major compaction
    boolean allFiles = (data[0] != (byte) 0);
    String columnName = Bytes.toString(data, 1, data.length - 1);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Launching subprocedure to compact mob files for " + tableName.getNameAsString());
    }
    ForeignExceptionDispatcher exnDispatcher = new ForeignExceptionDispatcher(
      tableName.getNameAsString());
    Configuration conf = rss.getConfiguration();
    long timeoutMillis = conf.getLong(MOB_COMPACTION_TIMEOUT_MILLIS_KEY,
      MOB_COMPACTION_TIMEOUT_MILLIS_DEFAULT);
    long wakeMillis = conf.getLong(MOB_COMPACTION_WAKE_MILLIS_KEY,
      MOB_COMPACTION_WAKE_MILLIS_DEFAULT);

    MobCompactionSubprocedurePool taskManager = new MobCompactionSubprocedurePool(rss
      .getServerName().toString(), conf);

    return new MobCompactionSubprocedure(member, exnDispatcher, wakeMillis, timeoutMillis, rss,
      involvedRegions, tableName, columnName, taskManager, allFiles);
  }

  public class MobCompactionSubprocedureBuilder implements SubprocedureFactory {
    @Override
    public Subprocedure buildSubprocedure(String name, byte[] data) {
      // The name of the procedure instance from the master is the table name.
      return RegionServerMobCompactionProcedureManager.this.buildSubprocedure(TableName.valueOf(name), data);
    }
  }

  /**
   * We use the MobCompactionSubprocedurePool, a class specific thread pool instead of
   * {@link org.apache.hadoop.hbase.executor.ExecutorService}.
   *
   * It uses a {@link java.util.concurrent.ExecutorCompletionService} which provides queuing of
   * completed tasks which lets us efficiently cancel pending tasks upon the earliest operation
   * failures.
   */
  static class MobCompactionSubprocedurePool {
    private final ExecutorCompletionService<Boolean> taskPool;
    private final ThreadPoolExecutor executor;
    private volatile boolean stopped;
    private final List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
    private final String name;

    MobCompactionSubprocedurePool(String name, Configuration conf) {
      // configure the executor service
      long keepAlive = conf.getLong(MOB_COMPACTION_TIMEOUT_MILLIS_KEY,
        MOB_COMPACTION_TIMEOUT_MILLIS_DEFAULT);
      int threads = conf.getInt(CONCURENT_MOB_COMPACTION_TASKS_KEY,
        DEFAULT_CONCURRENT_MOB_COMPACTION_TASKS);
      this.name = name;
      executor = new ThreadPoolExecutor(threads, threads, keepAlive, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(), new DaemonThreadFactory("rs(" + name
          + ")-mobCompaction-proc-pool"));
      executor.allowCoreThreadTimeOut(true);
      taskPool = new ExecutorCompletionService<Boolean>(executor);
    }

    boolean hasTasks() {
      return futures.size() != 0;
    }

    /**
     * Submit a task to the pool.
     *
     * NOTE: all must be submitted before you can safely {@link #waitForOutstandingTasks()}.
     */
    void submitTask(final Callable<Boolean> task) {
      Future<Boolean> f = this.taskPool.submit(task);
      futures.add(f);
    }

    /**
     * Wait for all of the currently outstanding tasks submitted via {@link #submitTask(Callable)}.
     * This *must* be called after all tasks are submitted via submitTask.
     *
     * @return <tt>true</tt> on success, <tt>false</tt> otherwise
     * @throws InterruptedException
     */
    boolean waitForOutstandingTasks() throws ForeignException, InterruptedException {
      LOG.debug("Waiting for local region mob compaction to finish.");

      int sz = futures.size();
      try {
        boolean success = true;
        // Using the completion service to process the futures.
        for (int i = 0; i < sz; i++) {
          Future<Boolean> f = taskPool.take();
          success = f.get() && success;
          if (!futures.remove(f)) {
            LOG.warn("unexpected future" + f);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Completed " + (i + 1) + "/" + sz + " local region mob compaction tasks.");
          }
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Completed " + sz + " local region mob compaction tasks.");
        }
        return success;
      } catch (InterruptedException e) {
        LOG.warn("Got InterruptedException in MobCompactionSubprocedurePool", e);
        if (!stopped) {
          Thread.currentThread().interrupt();
          throw new ForeignException("MobCompactionSubprocedurePool", e);
        }
        // we are stopped so we can just exit.
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof ForeignException) {
          LOG.warn("Rethrowing ForeignException from MobCompactionSubprocedurePool", e);
          throw (ForeignException) e.getCause();
        }
        LOG.warn("Got Exception in MobCompactionSubprocedurePool", e);
        throw new ForeignException(name, e.getCause());
      } finally {
        cancelTasks();
      }
      return false;
    }

    /**
     * This attempts to cancel out all pending and in progress tasks. Does not interrupt the running
     * tasks itself.
     *
     * @throws InterruptedException
     */
    void cancelTasks() throws InterruptedException {
      Collection<Future<Boolean>> tasks = futures;
      LOG.debug("cancelling " + tasks.size() + " mob compaction tasks " + name);
      for (Future<Boolean> f : tasks) {
        f.cancel(false);
      }

      // evict remaining tasks and futures from taskPool.
      futures.clear();
      while (taskPool.poll() != null) {
      }
      stop();
    }

    /**
     * Gracefully shutdown the thread pool.
     */
    void stop() {
      if (this.stopped)
        return;

      this.stopped = true;
      this.executor.shutdown();
    }
  }
}
