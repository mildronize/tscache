package net.opentsdb.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class CacheFragment {

  private static final Logger LOG = LoggerFactory.getLogger(CacheFragment.class);

  private long startTime;
  private long endTime;
  private boolean cacheState;

  public CacheFragment(long startTime, long endTime, boolean cacheState) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.cacheState = cacheState;
  }

  public boolean isInCache(){
    return cacheState;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }
}