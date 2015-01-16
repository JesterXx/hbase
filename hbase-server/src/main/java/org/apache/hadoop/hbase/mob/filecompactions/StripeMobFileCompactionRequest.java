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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

public class StripeMobFileCompactionRequest extends MobFileCompactionRequest {

  protected Collection<FileStatus> delFiles;
  protected Collection<CompactedStripe> compactedStripes;

  public StripeMobFileCompactionRequest(Collection<CompactedStripe> compactedStripes,
    Collection<FileStatus> delFiles) {
    this.selectionTime = EnvironmentEdgeManager.currentTime();
    this.compactedStripes = compactedStripes;
    this.delFiles = delFiles;
  }

  protected static class CompactedStripe {
    private List<FileStatus> files = new ArrayList<FileStatus>();
    private CompactedStripeId stripeId;

    public CompactedStripe(CompactedStripeId stripeId) {
      this.stripeId = stripeId;
    }

    public CompactedStripeId getStripeId() {
      return this.stripeId;
    }

    public void addFile(FileStatus file) {
      files.add(file);
    }

    public List<FileStatus> listFiles() {
      return Collections.unmodifiableList(files);
    }
  }

  protected static class CompactedStripeId {

    private String startKey;
    private String date;

    public CompactedStripeId(String startKey, String date) {
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
      if (this.startKey != null) {
        result = 31 * result + startKey.hashCode();
      }
      if (this.date != null) {
        result = 31 * result + date.hashCode();
      }
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof CompactedStripeId)) {
        return false;
      }
      CompactedStripeId another = (CompactedStripeId) obj;
      if (this.startKey != null && !this.startKey.equals(another.startKey)) {
        return false;
      } else if (another.startKey != null) {
        return false;
      }
      if (this.date != null && !this.date.equals(another.date)) {
        return false;
      } else if (another.date != null) {
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
