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
package org.apache.hadoop.hbase.regionserver;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.Strings;

public class MobFileStoreManager {

  private static final Log LOG = LogFactory.getLog(MobFileStoreManager.class);

  private Map<MobFileStoreKey, MobFileStore> stores = 
      new ConcurrentHashMap<MobFileStoreKey, MobFileStore>();
  private AtomicLong nextCleanCheckTime;
  private volatile boolean isInit = false;
  private Object lock = new Object();
  private Configuration conf;
  private FileSystem fs;
  private Path mobHome;
  // one day, in milliseconds
  private static final long CLEAN_CHECK_PERIOD = 24 * 60 * 60 * 1000;

  private static final MobFileStoreManager instance = new MobFileStoreManager();

  private MobFileStoreManager() {
    this.nextCleanCheckTime = new AtomicLong(System.currentTimeMillis() + CLEAN_CHECK_PERIOD);
  }

  public static MobFileStoreManager current() {
    return instance;
  }

  public static class MobFileStoreKey {
    private String tableName;
    private String familyName;

    public MobFileStoreKey(String tableName, String familyName) {
      this.tableName = tableName;
      this.familyName = familyName;
    }

    public String getTableName() {
      return this.tableName;
    }

    public String getFamilyName() {
      return this.familyName;
    }

    @Override
    public String toString() {
      return this.tableName + Path.SEPARATOR + familyName;
    }

    @Override
    public int hashCode() {
      return tableName.hashCode() * 127 + familyName.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (null == o) {
        return false;
      }
      if (o instanceof MobFileStoreKey) {
        MobFileStoreKey k = (MobFileStoreKey) o;

        boolean equalTable = (tableName == null ? k.tableName == null : tableName
            .equals(k.tableName));
        boolean equalFamily = (familyName == null ? k.familyName == null : familyName
            .equals(k.familyName));

        return equalTable && equalFamily;
      }
      {
        return false;
      }
    }
  }

  public void init(Configuration conf, FileSystem fs) throws IOException {
    if (!isInit) {
      synchronized (lock) {
        if (!isInit) {
          this.conf = conf;
          this.fs = fs;
          this.mobHome = MobUtils.getMobHome(conf);
          loadMobFileStores(conf, fs, mobHome);
          isInit = true;
          LOG.info("MobFileStoreManager is initialized");
        }
      }
    }
  }

  private void loadMobFileStores(Configuration conf, FileSystem fs, Path mobHome)
      throws IOException {
    FileStatus[] files = null;
    try {
      files = fs.listStatus(mobHome);
    } catch (FileNotFoundException e) {
      LOG.warn("Failed to list the files in " + mobHome, e);
      return;
    }
    if (null == files) {
      return;
    }
    for (int i = 0; i < files.length; i++) {
      if (files[i].isDirectory()) {
        loadMobFileStore(conf, fs, mobHome, files[i].getPath());
      }
    }
  }

  private void loadMobFileStore(Configuration conf, FileSystem fs, Path mobHome, Path path)
      throws IOException {
    FileStatus[] files = null;
    try {
      files = fs.listStatus(path);
    } catch (FileNotFoundException e) {
      LOG.warn("Failed to list the files in " + path, e);
      return;
    }
    if (null == files) {
      return;
    }
    String tableName = path.getName();

    for (int i = 0; i < files.length; i++) {
      if (files[i].isDirectory()) {
        Path familyPath = files[i].getPath();
        MobFileStoreKey key = new MobFileStoreKey(tableName, familyPath.getName());
        MobFileStore mobFileStore = MobFileStore.create(conf, fs, mobHome, TableName.valueOf(tableName),
            familyPath.getName());
        if (null != mobFileStore) {
          stores.put(key, mobFileStore);
        }
      }
    }
  }

  public boolean removeMobStores(List<MobFileStoreKey> keyList) throws IOException {
    if (null == keyList) {
      return true;
    }
    for (MobFileStoreKey key : keyList) {
      if (stores.containsKey(key)) {
        stores.remove(key);
      }
    }
    return true;
  }

  public boolean removeMobStore(String tableName, String familyName) throws IOException {
    MobFileStoreKey key = new MobFileStoreKey(tableName, familyName);
    if (stores.containsKey(key)) {
      stores.remove(key);
    }
    return true;
  }

  public MobFileStore getMobFileStore(String tableName, String familyName) {
    MobFileStoreKey key = new MobFileStoreKey(tableName, familyName);
    return getMobFileStore(key);
  }

  public MobFileStore getMobFileStore(MobFileStoreKey key) {
    return stores.get(key);
  }

  public List<MobFileStoreKey> getMobFileStores(String tableName) {
    Set<MobFileStoreKey> keys = stores.keySet();
    Iterator<MobFileStoreKey> iter = keys.iterator();
    if (null == iter) {
      return Collections.emptyList();
    }
    List<MobFileStoreKey> results = new ArrayList<MobFileStoreKey>();
    while (iter.hasNext()) {
      MobFileStoreKey key = iter.next();
      if (null != key && key.getTableName().equals(tableName)) {
        results.add(key);
      }
    }
    return results;
  }

  public List<MobFileStoreKey> getAllMobFileStores() {
    Set<MobFileStoreKey> keys = stores.keySet();
    Iterator<MobFileStoreKey> iter = keys.iterator();
    if (null == iter) {
      return Collections.emptyList();
    }
    List<MobFileStoreKey> results = new ArrayList<MobFileStoreKey>();
    while (iter.hasNext()) {
      MobFileStoreKey key = iter.next();
      results.add(key);
    }
    return results;
  }

  public MobFileStore createMobFileStore(String tableName, HColumnDescriptor family)
      throws IOException {
    cleanMobFileStoresIfNecessary();
    MobFileStoreKey key = new MobFileStoreKey(tableName, family.getNameAsString());
    Path homePath = new Path(mobHome, tableName + Path.SEPARATOR + family.getNameAsString());

    if (!stores.containsKey(key)) {
      MobFileStore blobStore = MobFileStore.create(conf, fs, mobHome, TableName.valueOf(tableName),
          family.getNameAsString());
      if (null == blobStore) {
        fs.mkdirs(homePath);
        blobStore = MobFileStore.create(conf, fs, homePath, family);
      }
      stores.put(key, blobStore);
    }
    return stores.get(key);
  }

  /**
   * Check the mob file stores before create blob store Clean the obsolete blob stores
   */
  private void cleanMobFileStoresIfNecessary() {
    long currentTime = System.currentTimeMillis();
    if (currentTime < nextCleanCheckTime.get()) {
      // Don't reach the next clean check slot, return directly
      return;
    }

    cleanUnusedMobFileStores();
    nextCleanCheckTime.set(currentTime + CLEAN_CHECK_PERIOD);
  }

  private void cleanUnusedMobFileStores() {
    TableDescriptors tableDescriptors = new FSTableDescriptors(fs, mobHome.getParent());
    try {
      Map<String, HTableDescriptor> htd = tableDescriptors.getAll();
      if (stores != null && stores.size() > 0) {
        for (MobFileStoreKey mobFileStoreKey : stores.keySet()) {
          String tableName = mobFileStoreKey.getTableName();

          // Check whether the latest HTableDescriptors list contains this table
          if (htd.containsKey(tableName)) {
            // If the table still exists
            String familyName = mobFileStoreKey.getFamilyName();
            HTableDescriptor table = htd.get(tableName);
            Set<byte[]> familiesKeys = table.getFamiliesKeys();

            // Judge whether the column family is still existing or not
            if (familiesKeys.contains(Bytes.toBytes(familyName))) {
              // If the family still exists
              HColumnDescriptor family = table.getFamily(Bytes.toBytes(familyName));
              // Judge if this family still enable the mob
              if (!MobUtils.isMobFamily(family)) {
                // If it does not enable mob, remove it from the stores
                removeMobStore(tableName, familyName);
              }
            } else {
              // If the family does not exist, remove it from the stores
              removeMobStore(tableName, familyName);
            }
          } else {
            // If the table does not exist, remove it from the stores
            removeMobStores(getMobFileStores(tableName));
          }
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to clean obsolete stores in the MobFileStoreManager!");
    }
  }
}
