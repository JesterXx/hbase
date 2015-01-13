package org.apache.hadoop.hbase.mob.nativecompaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

public class StripeMobFileCompactRequest extends MobFileCompactRequest {

  protected Collection<FileStatus> allDelFiles;
  protected Collection<CompactedStripe> compactedStripes;

  public StripeMobFileCompactRequest(Collection<CompactedStripe> compactedStripes,
      Collection<FileStatus> allDelFiles) {
    this.selectionTime = EnvironmentEdgeManager.currentTime();
    this.compactedStripes = compactedStripes;
    this.allDelFiles = allDelFiles;
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
