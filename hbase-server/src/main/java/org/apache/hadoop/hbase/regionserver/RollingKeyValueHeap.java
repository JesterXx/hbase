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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.ScannerContext.NextState;

@InterfaceAudience.Private
public class RollingKeyValueHeap extends KeyValueHeap {

  protected PriorityQueue<KeyValueScanner> sortedScanners;
  protected RollingBar bar;

  public RollingKeyValueHeap(List<? extends KeyValueScanner> scanners, CellComparator comparator)
    throws IOException {
    this(scanners, new KVScannerComparator(comparator));
  }

  public RollingKeyValueHeap(List<? extends KeyValueScanner> scanners,
    KVScannerComparator comparator) throws IOException {
    super();
    this.comparator = comparator;
    sortedScanners = new PriorityQueue<KeyValueScanner>(scanners.size(), this.comparator);
    for (KeyValueScanner scanner : scanners) {
      if (scanner.peek() != null) {
        this.sortedScanners.add(scanner);
      } else {
        this.scannersForDelayedClose.add(scanner);
      }
    }
    this.heap = new PriorityQueue<KeyValueScanner>(scanners.size(), this.comparator);
    current = sortedScanners.poll();
    heap.add(current);
    if (sortedScanners.peek() != null) {
      bar = new RollingBar(sortedScanners.peek().peek());
    } else {
      bar = RollingBar.MAX;
    }
  }

  @Override
  public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
    if (this.current == null) {
      return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
    }
    InternalScanner currentAsInternal = (InternalScanner)this.current;
    boolean moreCells = currentAsInternal.next(result, scannerContext);
    Cell pee = this.current.peek();
    if (pee == null || !moreCells) {
      // add the scanner that is to be closed
      this.scannersForDelayedClose.add(this.current);
    } else {
      this.heap.add(this.current);
    }
    updateHeapInNext();
    this.current = null;
    this.current = pollRealKV();
    if (this.current == null) {
      moreCells = scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
    }
    return moreCells;
  }

  @Override
  public Cell next() throws IOException {
    if(this.current == null) {
      return null;
    }
    Cell kvReturn = this.current.next();
    Cell kvNext = this.current.peek();
    // add necessary scanners to the heap
    updateHeapInNext();
    if (kvNext == null) {
      this.scannersForDelayedClose.add(this.current);
      this.current = null;
      this.current = pollRealKV();
    } else {
      KeyValueScanner topScanner = this.heap.peek();
      // no need to add current back to the heap if it is the only scanner left
      if (topScanner != null && this.comparator.compare(kvNext, topScanner.peek()) >= 0) {
        this.heap.add(this.current);
        this.current = null;
        this.current = pollRealKV();
      }
    }
    return kvReturn;
  }

  @Override
  public boolean seek(Cell key) throws IOException {
    // 1. Move the scanners from sortedScanners to heap
    updateHeapInSeek(key);
    // 2. Seek in the heap
    return super.seek(key);
  }

  @Override
  public boolean reseek(Cell key) throws IOException {
    // 1. Move the scanners from sortedScanners to heap
    updateHeapInSeek(key);
    // 2. Seek in the heap
    return super.reseek(key);
  }

  @Override
  public boolean requestSeek(Cell key, boolean forward, boolean useBloom) throws IOException {
    // 1. Move the scanners from sortedScanners to heap
    updateHeapInSeek(key);
    // 2. Seek in the heap
    return super.requestSeek(key, forward, useBloom);
  }

  @Override
  public long getSequenceID() {
    return 0;
  }

  @Override
  public void close() {
    super.close();
    // close the scanners that are not added to heap
    KeyValueScanner scanner = null;
    while ((scanner = sortedScanners.poll()) != null) {
      scanner.close();
    }
  }

  /**
   * Adds the necessary scanners to the heap by the searched key.
   * @param key KeyValue to seek.
   */
  private void updateHeapInSeek(Cell key) {
    if (sortedScanners.size() > 0 && CellComparator.COMPARATOR.compare(bar.get(), key) <= 0) {
      addScannersToHeap(key);
      if (sortedScanners.size() > 0) {
        KeyValueScanner scanner = sortedScanners.poll();
        heap.add(scanner);
        bar.set(scanner.peek());
      } else {
        bar = RollingBar.MAX;
      }
      current = heap.peek();
    }
  }

  /**
   * Adds the necessary scanners to the heap.
   */
  private void updateHeapInNext() {
    while (sortedScanners.size() > 0) {
      KeyValueScanner heapPeak = heap.peek();
      if (heapPeak != null) {
        Cell heapPeek = heapPeak.peek();
        if (heapPeek != null && bar.compareTo(heapPeek) > 0) {
          return;
        }
      }
      KeyValueScanner scanner = sortedScanners.poll();
      heap.add(scanner);
      KeyValueScanner peekScanner = sortedScanners.peek();
      if (peekScanner != null) {
        bar.set(peekScanner.peek());
      } else {
        bar = RollingBar.MAX;
      }
    }
  }

  /**
   * Adds the necessary scanners to the heap by the searched key.
   * @param key KeyValue to seek.
   */
  private void addScannersToHeap(Cell key) {
    while (sortedScanners.size() > 0) {
      KeyValueScanner scanner = sortedScanners.peek();
      if (scanner == null) {
        return;
      }
      Cell topCell = scanner.peek();
      if (topCell != null && CellComparator.COMPARATOR.compare(topCell, key) > 0) {
        return;
      }
      scanner = sortedScanners.poll();
      heap.add(scanner);
    }
  }

  private static class RollingBar {
    private Cell cell;
    public static final RollingBar MAX = new RollingBar();

    private RollingBar() {
    }

    private RollingBar(Cell cell) {
      this.cell = cell;
    }

    public void set(Cell cell) {
      this.cell = cell;
    }

    public Cell get() {
      return cell;
    }

    public int compareTo(Cell cell) {
      if (this == MAX) {
        return 1;
      }
      return CellComparator.COMPARATOR.compare(this.cell, cell);
    }
  }

}
