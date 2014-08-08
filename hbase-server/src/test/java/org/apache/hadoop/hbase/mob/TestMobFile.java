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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMobFile extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestMobFile.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private CacheConfig cacheConf =  new CacheConfig(TEST_UTIL.getConfiguration());
  private final String TABLE = "tableName";
  private final String FAMILY = "familyName";

  @Test
  public void testReadKeyValue() throws Exception {
    Path outputDir = new Path(new Path(this.testDir, TABLE), FAMILY);
    HFileContext meta = new HFileContextBuilder().withBlockSize(8*1024).build();
    StoreFile.Writer writer = new StoreFile.WriterBuilder(conf, cacheConf, this.fs)
            .withOutputDir(outputDir)
            .withFileContext(meta)
            .build();
    String caseName = getName();
    MobTestUtil.writeStoreFile(writer, caseName);

    MobFile mobFile = new MobFile(new StoreFile(this.fs, writer.getPath(),
        conf, cacheConf, BloomType.NONE));
    byte[] family = Bytes.toBytes(caseName);
    byte[] qualify = Bytes.toBytes(caseName);

    // Test the start key
    byte[] startKey = Bytes.toBytes("aa");  // The start key bytes
    KeyValue expectedKey =
        new KeyValue(startKey, family, qualify, Long.MAX_VALUE, Type.Put, startKey);
    KeyValue seekKey = expectedKey.createKeyOnly(false);
    KeyValue kv = KeyValueUtil.ensureKeyValue(mobFile.readCell(seekKey, false));
    assertKeyValuesEquals(expectedKey, kv);

    // Test the end key
    byte[] endKey = Bytes.toBytes("zz");  // The end key bytes
    expectedKey = new KeyValue(endKey, family, qualify, Long.MAX_VALUE, Type.Put, endKey);
    seekKey = expectedKey.createKeyOnly(false);
    kv = KeyValueUtil.ensureKeyValue(mobFile.readCell(seekKey, false));
    assertKeyValuesEquals(expectedKey, kv);

    // Test the random key
    byte[] randomKey = Bytes.toBytes(MobTestUtil.generateRandomString(2));
    expectedKey = new KeyValue(randomKey, family, qualify, Long.MAX_VALUE, Type.Put, randomKey);
    seekKey = expectedKey.createKeyOnly(false);
    kv = KeyValueUtil.ensureKeyValue(mobFile.readCell(seekKey, false));
    assertKeyValuesEquals(expectedKey, kv);

    // Test the key which is less than the start key
    byte[] lowerKey = Bytes.toBytes("a1"); // Smaller than "aa"
    expectedKey = new KeyValue(startKey, family, qualify, Long.MAX_VALUE, Type.Put, startKey);
    seekKey = new KeyValue(lowerKey, family, qualify, Long.MAX_VALUE, Type.Put, lowerKey);
    kv = KeyValueUtil.ensureKeyValue(mobFile.readCell(seekKey, false));
    assertKeyValuesEquals(expectedKey, kv);

    // Test the key which is more than the end key
    byte[] upperKey = Bytes.toBytes("z{"); // Bigger than "zz"
    seekKey = new KeyValue(upperKey, family, qualify, Long.MAX_VALUE, Type.Put, upperKey);
    kv = KeyValueUtil.ensureKeyValue(mobFile.readCell(seekKey, false));
    assertNull(kv);
  }

  /**
   * Compare two KeyValue only for their row family qualifier value
   */
  @SuppressWarnings("deprecation")
  private void assertKeyValuesEquals(KeyValue firstKeyValue,
      KeyValue secondKeyValue) {
    assertEquals(firstKeyValue.getRow(), secondKeyValue.getRow());
    assertEquals(firstKeyValue.getFamily(), secondKeyValue.getFamily());
    assertEquals(firstKeyValue.getQualifier(), secondKeyValue.getQualifier());
    assertEquals(firstKeyValue.getValue(), secondKeyValue.getValue());
  }

  @Test
  public void testGetScanner() throws Exception {
    Path outputDir = new Path(new Path(this.testDir, TABLE), FAMILY);
    HFileContext meta = new HFileContextBuilder().withBlockSize(8*1024).build();
    StoreFile.Writer writer = new StoreFile.WriterBuilder(conf, cacheConf, this.fs)
            .withOutputDir(outputDir)
            .withFileContext(meta)
            .build();
    MobTestUtil.writeStoreFile(writer, getName());

    MobFile mobFile = new MobFile(new StoreFile(this.fs, writer.getPath(),
        conf, cacheConf, BloomType.NONE));
    assertNotNull(mobFile.getScanner());
    assertTrue(mobFile.getScanner() instanceof StoreFileScanner);
  }
}
