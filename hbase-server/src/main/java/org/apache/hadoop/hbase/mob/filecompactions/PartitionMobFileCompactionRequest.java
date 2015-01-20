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
package org.apache.hadoop.hbase.mob.filecompactions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * An implementation of {@link MobFileCompactionRequest} that is used in
 * {@link PartitionMobFileCompactor}.
 */
@InterfaceAudience.Private
public class PartitionMobFileCompactionRequest extends MobFileCompactionRequest {

  protected Collection<FileStatus> delFiles;
  protected Collection<CompactedPartition> compactedPartitions;

  public PartitionMobFileCompactionRequest(Collection<CompactedPartition> compactedPartitions,
    Collection<FileStatus> delFiles) {
    this.selectionTime = EnvironmentEdgeManager.currentTime();
    this.compactedPartitions = compactedPartitions;
    this.delFiles = delFiles;
  }

  /**
   * The partition in the mob file compaction.
   * The mob files that have the same start key and date in their names belong to
   * the same partition.
   */
  protected static class CompactedPartition {
    private List<FileStatus> files = new ArrayList<FileStatus>();
    private CompactedPartitionId partitionId;

    public CompactedPartition(CompactedPartitionId partitionId) {
      this.partitionId = partitionId;
    }

    public CompactedPartitionId getPartitionId() {
      return this.partitionId;
    }

    public void addFile(FileStatus file) {
      files.add(file);
    }

    public List<FileStatus> listFiles() {
      return Collections.unmodifiableList(files);
    }
  }

  /**
   * The partition id this consists of start key and date of the mob file name. 
   */
  protected static class CompactedPartitionId {

    private String startKey;
    private String date;

    public CompactedPartitionId(String startKey, String date) {
      if (startKey == null || date == null) {
        throw new IllegalArgumentException("Neither of start key and date could be null");
      }
      this.startKey = startKey;
      this.date = date;
    }

    public String getStartKey() {
      return this.startKey;
    }

    public String getDate() {
      return this.date;
    }

    @Override
    public int hashCode() {
      int result = 17;
      result = 31 * result + startKey.hashCode();
      result = 31 * result + date.hashCode();
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof CompactedPartitionId)) {
        return false;
      }
      CompactedPartitionId another = (CompactedPartitionId) obj;
      if (!this.startKey.equals(another.startKey)) {
        return false;
      }
      if (!this.date.equals(another.date)) {
        return false;
      }
      return true;
    }

    @Override
    public String toString() {
      return new StringBuilder(startKey).append("-").append(date).toString();
    }
  }
}
