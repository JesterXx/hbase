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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;

public class MobFile {

  private static final Log LOG = LogFactory.getLog(MobFile.class);

  private StoreFile sf;

  protected MobFile(StoreFile sf) {
    this.sf = sf;
  }

  public StoreFileScanner getScanner() throws IOException {
    List<StoreFile> sfs = new ArrayList<StoreFile>();
    sfs.add(sf);

    List<StoreFileScanner> sfScanners = StoreFileScanner.getScannersForStoreFiles(sfs, false, true,
        false, null, sf.getMaxMemstoreTS());

    if (!sfScanners.isEmpty()) {
      return sfScanners.get(0);
    }
    return null;
  }

  public Cell readKeyValue(KeyValue search, boolean cacheMobBlocks) throws IOException {
    Cell result = null;
    StoreFileScanner scanner = null;
    String msg = "";
    List<StoreFile> sfs = new ArrayList<StoreFile>();
    sfs.add(sf);
    try {
      List<StoreFileScanner> sfScanners = StoreFileScanner.getScannersForStoreFiles(sfs, cacheMobBlocks,
          true, false, null, sf.getMaxMemstoreTS());

      if (!sfScanners.isEmpty()) {
        scanner = sfScanners.get(0);
        if (true == scanner.seek(search)) {
          result = scanner.peek();
        }
      }
    } catch (IOException ioe) {
      msg = "Failed to read KeyValue!";
      if (ioe.getCause() instanceof FileNotFoundException) {
        msg += "The lob file does not exist!";
      }
      LOG.error(msg, ioe);
      result = null;
    } catch (NullPointerException npe) {
      // When delete the file during the scan, the hdfs getBlockRange will
      // throw NullPointerException, catch it and manage it.
      msg = "Failed to read KeyValue! ";
      LOG.error(msg, npe);
      result = null;
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }
    return result;
  }

  public String getName() {
    return sf.getPath().getName();
  }

  public void open() throws IOException {
    if (sf.getReader() == null) {
      sf.createReader();
    }
  }

  public void close() throws IOException {
    if (null != sf) {
      sf.closeReader(false);
      sf = null;
    }
  }

  public static MobFile create(FileSystem fs, Path path, Configuration conf, MobCacheConfig cacheConf)
      throws IOException {
    StoreFile sf = new StoreFile(fs, path, conf, cacheConf, BloomType.NONE);
    return new MobFile(sf);
  }
}
