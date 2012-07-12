package com.continuuity.data.operation;

import com.continuuity.api.data.ReadOperation;

public class OrderedRead implements ReadOperation {

  private final byte [] startKey;
  private final byte [] endKey;
  private final int limit;

  public OrderedRead(final byte [] key) {
    this(key, null, 1);
  }

  public OrderedRead(final byte [] startKey, final byte [] endKey) {
    this(startKey, endKey, Integer.MAX_VALUE);
  }

  public OrderedRead(final byte [] startKey, int limit) {
    this(startKey, null, limit);
  }

  public OrderedRead(final byte [] startKey, final byte [] endKey, int limit) {
    this.startKey = startKey;
    this.endKey = endKey;
    this.limit = limit;
  }
  
  public byte [] getStartKey() {
    return this.startKey;
  }

  public byte [] getEndKey() {
    return this.endKey;
  }

  public int getLimit() {
    return this.limit;
  }
}
