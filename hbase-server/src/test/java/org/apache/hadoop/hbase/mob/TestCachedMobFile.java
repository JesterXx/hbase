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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestCachedMobFile extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestCachedMobFile.class);
  private CacheConfig cacheConf = new CacheConfig(conf);
  private final String TABLE = "tableName";
  private final String FAMILY = "familyName";
  private final String FAMILY1 = "familyName1";
  private final String FAMILY2 = "familyName2";
  private final long EXPECTED_REFERENCE_ZERO = 0;
  private final long EXPECTED_REFERENCE_ONE = 1;
  private final long EXPECTED_REFERENCE_TWO = 2;

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }
  
  @Test
  public void testOpenClose() throws Exception {
    String caseName = getName();
    Path outputDir = new Path(new Path(this.testDir, TABLE),
        FAMILY);
    HFileContext meta = new HFileContextBuilder().withBlockSize(8*1024).build();
    StoreFile.Writer writer = new StoreFile.WriterBuilder(conf, cacheConf, this.fs)
    .withOutputDir(outputDir)
    .withFileContext(meta)
    .build();
    MobTestUtil.writeStoreFile(writer, caseName);
    CachedMobFile cachedMobFile = new CachedMobFile(new MobFile(
        new StoreFile(this.fs, writer.getPath(), conf, cacheConf,
            BloomType.NONE)));
    
    assertEquals(EXPECTED_REFERENCE_ZERO, cachedMobFile.getReference());
    cachedMobFile.open();
    assertEquals(EXPECTED_REFERENCE_ONE, cachedMobFile.getReference());
    cachedMobFile.open();
    assertEquals(EXPECTED_REFERENCE_TWO, cachedMobFile.getReference());
    cachedMobFile.close();
    assertEquals(EXPECTED_REFERENCE_ONE, cachedMobFile.getReference());
    cachedMobFile.close();
    assertEquals(EXPECTED_REFERENCE_ZERO, cachedMobFile.getReference());
  }

  @Test
  public void testCompare() throws Exception {
    String caseName = getName();
    Path outputDir1 = new Path(new Path(this.testDir, TABLE),
        FAMILY1);
    HFileContext meta = new HFileContextBuilder().withBlockSize(8*1024).build();
    StoreFile.Writer writer1 = new StoreFile.WriterBuilder(conf, cacheConf, this.fs)
    .withOutputDir(outputDir1)
    .withFileContext(meta)
    .build();
    MobTestUtil.writeStoreFile(writer1, caseName);
    CachedMobFile cachedMobFile1 = new CachedMobFile(new MobFile(
        new StoreFile(this.fs, writer1.getPath(), conf, cacheConf,
            BloomType.NONE)));

    Path outputDir2 = new Path(new Path(this.testDir, TABLE),
        FAMILY2);
    StoreFile.Writer writer2 = new StoreFile.WriterBuilder(conf, cacheConf, this.fs)
    .withOutputDir(outputDir2)
    .withFileContext(meta)
    .build();
    MobTestUtil.writeStoreFile(writer2, caseName);
    CachedMobFile cachedMobFile2 = new CachedMobFile(new MobFile(
        new StoreFile(this.fs, writer2.getPath(), conf, cacheConf,
            BloomType.NONE)));
    cachedMobFile1.access(1);
    cachedMobFile2.access(2);
    assertEquals(cachedMobFile1.compareTo(cachedMobFile2), 1);
    assertEquals(cachedMobFile2.compareTo(cachedMobFile1), -1);
    assertEquals(cachedMobFile1.compareTo(cachedMobFile1), 0);
  }

  @Test
  public void testReadKeyValue() throws Exception {
    Path outputDir = new Path(new Path(this.testDir, TABLE), "familyname");
    HFileContext meta = new HFileContextBuilder().withBlockSize(8*1024).build();
    StoreFile.Writer writer = new StoreFile.WriterBuilder(conf, cacheConf, this.fs)
    .withOutputDir(outputDir)
    .withFileContext(meta)
    .build();
    String caseName = getName();
    MobTestUtil.writeStoreFile(writer, caseName);
    MobFile mobFile = new MobFile(new StoreFile(this.fs, writer.getPath(),
        conf, cacheConf, BloomType.NONE));
    CachedMobFile cachedMobFile = new CachedMobFile(mobFile);
    
    byte[] family = Bytes.toBytes(caseName);
    byte[] qualify = Bytes.toBytes(caseName);
    
    // Test the start key
    byte[] startKey = Bytes.toBytes("aa");  // The start key bytes
    KeyValue expectedKey =
        new KeyValue(startKey, family, qualify, Long.MAX_VALUE, Type.Put, startKey);
    KeyValue seekKey = expectedKey.createKeyOnly(false);
    KeyValue kv = KeyValueUtil.ensureKeyValue(cachedMobFile.readCell(seekKey, false));
    assertKeyValuesEquals(expectedKey, kv);
    
    // Test the end key
    byte[] endKey = Bytes.toBytes("zz");  // The end key bytes
    expectedKey = new KeyValue(endKey, family, qualify, Long.MAX_VALUE, Type.Put, endKey);
    seekKey = expectedKey.createKeyOnly(false);
    kv = KeyValueUtil.ensureKeyValue(cachedMobFile.readCell(seekKey, false));
    assertKeyValuesEquals(expectedKey, kv);
    
    // Test the random key
    byte[] randomKey = Bytes.toBytes(MobTestUtil.generateRandomString(2)); 
    expectedKey = new KeyValue(randomKey, family, qualify, Long.MAX_VALUE, Type.Put, randomKey);
    seekKey = expectedKey.createKeyOnly(false);
    kv = KeyValueUtil.ensureKeyValue(cachedMobFile.readCell(seekKey, false));
    assertKeyValuesEquals(expectedKey, kv);
    
    // Test the key which is less than the start key
    byte[] lowerKey = Bytes.toBytes("a1"); // Smaller than "aa"
    expectedKey = new KeyValue(startKey, family, qualify, Long.MAX_VALUE, Type.Put, startKey);
    seekKey = new KeyValue(lowerKey, family, qualify, Long.MAX_VALUE, Type.Put, lowerKey);
    kv = KeyValueUtil.ensureKeyValue(cachedMobFile.readCell(seekKey, false));
    assertKeyValuesEquals(expectedKey, kv);
    
    // Test the key which is more than the end key
    byte[] upperKey = Bytes.toBytes("z{"); // Bigger than "zz"
    seekKey = new KeyValue(upperKey, family, qualify, Long.MAX_VALUE, Type.Put, upperKey);
    kv = KeyValueUtil.ensureKeyValue(cachedMobFile.readCell(seekKey, false));
    assertNull(kv);
  }

  /**
   * Compare two KeyValue only for their row family qualifier value
   */
  private void assertKeyValuesEquals(KeyValue firstKeyValue,
      KeyValue secondKeyValue) {
    assertEquals(firstKeyValue.getRow(), secondKeyValue.getRow());
    assertEquals(firstKeyValue.getFamily(), secondKeyValue.getFamily());
    assertEquals(firstKeyValue.getQualifier(), secondKeyValue.getQualifier());
    assertEquals(firstKeyValue.getValue(), secondKeyValue.getValue());
  }
}
