package org.apache.hadoop.hbase.mob.nativecompaction;

public abstract class MobFileCompactRequest {

  protected long selectionTime;
  protected CompactionType type = CompactionType.PART_FILES;

  public void setCompactionType(CompactionType type) {
    this.type = type;
  }

  protected enum CompactionType {
    PART_FILES, ALL_FILES;
  }
}
