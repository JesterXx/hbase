package org.apache.hadoop.hbase.mob.nativecompaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobFileName;
import org.apache.hadoop.hbase.mob.nativecompaction.MobFileCompactRequest.CompactionType;
import org.apache.hadoop.hbase.mob.nativecompaction.StripeMobFileCompactRequest.CompactedStripe;
import org.apache.hadoop.hbase.mob.nativecompaction.StripeMobFileCompactRequest.CompactedStripeId;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;

public class StripeMobFileCompactor implements MobFileCompactor {

  protected FileSystem fs;
  protected Configuration conf;
  protected long mergeableSize;

  public StripeMobFileCompactor(FileSystem fs, Configuration conf) {
    this.fs = fs;
    this.conf = conf;
    mergeableSize = conf.getLong(MobConstants.MOB_COMPACTION_MERGEABLE_SIZE,
      MobConstants.DEFAULT_MOB_COMPACTION_MERGEABLE_SIZE);
  }

  @Override
  public List<Path> compact(List<FileStatus> files) {
    if (files == null || files.isEmpty()) {
      return null;
    }
    StripeMobFileCompactRequest request = select(files);
    return performCompact(request);
  }

  public StripeMobFileCompactRequest select(List<FileStatus> files) {
    Collection<FileStatus> allDelFiles = new ArrayList<FileStatus>();
    Map<CompactedStripeId, CompactedStripe> filesToCompact =
      new HashMap<CompactedStripeId, CompactedStripe>();
    Iterator<FileStatus> ir = files.iterator();
    for (; ir.hasNext();) {
      FileStatus file = ir.next();
      if (StoreFileInfo.isDelFile(file.getPath())) {
        allDelFiles.add(file);
        ir.remove();
      } else if (file.getLen() < mergeableSize) {
        // add it to the merge pool
        MobFileName fileName = MobFileName.create(file.getPath().getName());
        CompactedStripeId id = new CompactedStripeId(fileName.getStartKey(), fileName.getDate());
        CompactedStripe compactedStripe = filesToCompact.get(id);
        if (compactedStripe == null) {
          compactedStripe = new CompactedStripe(id);
          compactedStripe.addFile(file);
          filesToCompact.put(id, compactedStripe);
        } else {
          compactedStripe.addFile(file);
        }
        ir.remove();
      }
    }
    StripeMobFileCompactRequest request = new StripeMobFileCompactRequest(filesToCompact.values(),
      allDelFiles);
    if (files.isEmpty()) {
      // all the files are selected
      request.setCompactionType(CompactionType.ALL_FILES);
    }
    return request;
  }

  public List<Path> performCompact(StripeMobFileCompactRequest request) {
    return null;
  }
}
