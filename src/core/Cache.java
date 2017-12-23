package net.opentsdb.core;

import com.stumbleupon.async.Deferred;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.TreeMap;

/**
 * Cache module
 * @since 2.0
 */

public class Cache {

  private static final Logger LOG = LoggerFactory.getLogger(Cache.class);

  public Cache() {
    LOG.debug("Create Cache object");
  }

  public ArrayList<CacheFragment> buildCacheFragments(TsdbQuery tsdbQuery){
    ArrayList<CacheFragment> cacheFragments = new ArrayList<net.opentsdb.core.CacheFragment>();
    CacheFragment cacheFragment = new CacheFragment(tsdbQuery.getStartTime(), tsdbQuery.getEndTime(), false);
    cacheFragments.add(cacheFragment);
    return cacheFragments;
  }

  public Deferred<TreeMap<byte[], Span>> findCache(CacheFragment fragment){
    return null;
  }

}