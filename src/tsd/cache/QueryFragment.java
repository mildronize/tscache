package net.opentsdb.tsd.cache;

/**
 * Created by mildronize on 4/3/2017.
 */
public class QueryFragment {
  private long start_time;
  private long end_time;
  private boolean cached;

  public long getStart_time() {
    return start_time;
  }

  public void setStart_time(long start_time) {
    this.start_time = start_time;
  }

  public long getEnd_time() {
    return end_time;
  }

  public void setEnd_time(long end_time) {
    this.end_time = end_time;
  }

  public boolean isCached() {
    return cached;
  }

  public void setCached(boolean cached) {
    this.cached = cached;
  }
}
