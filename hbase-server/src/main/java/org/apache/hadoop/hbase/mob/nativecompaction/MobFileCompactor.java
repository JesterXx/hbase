package org.apache.hadoop.hbase.mob.nativecompaction;

import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public interface MobFileCompactor {

  List<Path> compact(List<FileStatus> files);
}
