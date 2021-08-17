package net.opentsdb.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class CacheFragment {

  private static final Logger LOG = LoggerFactory.getLogger(CacheFragment.class);

  private long startTime;

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  private long endTime;
  private boolean cacheState;

  public void setFailed(boolean failed) {
    this.failed = failed;
  }

  public boolean isFailed() {
    return failed;
  }

  // state of cache retrieval, default false
  private boolean failed = false;

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

  public boolean compareTo(CacheFragment fragment){
    if(startTime != fragment.getStartTime())
      return false;
    if(endTime != fragment.getEndTime())
      return false;
    if(cacheState != fragment.isInCache())
      return false;
    return true;
  }

  @Override
  public String toString(){
    return "[ " + startTime + " , " + endTime + " ]: " + cacheState;
  }
}