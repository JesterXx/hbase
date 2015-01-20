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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.mob.filecompactions.PartitionMobFileCompactionRequest.CompactedPartition;
import org.apache.hadoop.hbase.mob.filecompactions.PartitionMobFileCompactionRequest.CompactedPartitionId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestPartitionMobFileCompactionRequest {

  @Test
  public void testCompactedPartitionId() {
    String startKey1 = "startKey1";
    String startKey2 = "startKey2";
    String date1 = "date1";
    String date2 = "date2";
    CompactedPartitionId partitionId1 = new CompactedPartitionId(startKey1, date1);
    CompactedPartitionId partitionId2 = new CompactedPartitionId(startKey2, date2);
    CompactedPartitionId partitionId3 = new CompactedPartitionId(startKey1, date2);

    Assert.assertTrue(partitionId1.equals(partitionId1));
    Assert.assertFalse(partitionId1.equals(partitionId2));
    Assert.assertFalse(partitionId1.equals(partitionId3));
    Assert.assertFalse(partitionId2.equals(partitionId3));

    Assert.assertEquals(startKey1, partitionId1.getStartKey());
    Assert.assertEquals(date1, partitionId1.getDate());
  }

  @Test
  public void testCompactedPartition() {
    CompactedPartitionId partitionId = new CompactedPartitionId("startKey1", "date1");
    CompactedPartition partition = new CompactedPartition(partitionId);
    FileStatus file = new FileStatus(1, false, 1, 1024, 1, new Path("/test"));
    partition.addFile(file);
    Assert.assertEquals(file, partition.listFiles().get(0));
  }
}
