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

  // May be byte data structure for looking fast
  private ArrayList<long[]> cacheIndexes;

  // Fragment Order = Ceil(Ti/range size)
  private int startFragmentOrder;


  // in seconds
  private int rangeSize;

  // Number of hours in Range size
  public void setRangeSize(int rangeSize) {
    this.rangeSize = rangeSize * 3600;
  }

  public Cache() {
    LOG.debug("Create Cache object");

    // set range size 1 hr ( 3600 seconds )
    setRangeSize(1);

  }

  public ArrayList<CacheFragment> buildCacheFragments(TsdbQuery tsdbQuery){
    ArrayList<CacheFragment> cacheFragments = new ArrayList<CacheFragment>();

    // First miss
    if(isCacheIndexesEmpty()) {
      CacheFragment cacheFragment = new CacheFragment(tsdbQuery.getStartTime(), tsdbQuery.getEndTime(), false);
      cacheFragments.add(cacheFragment);
    }

    return cacheFragments;
  }

  public Deferred<TreeMap<byte[], Span>> findCache(CacheFragment fragment){
    // find in cache
    return null;
  }

  public Deferred<Object> storeCache(CacheFragment cacheFragment, TreeMap<byte[], Span> result){
    // save to memached

    // update cacheIndexes
    return null;
  }


  private boolean isCacheIndexesEmpty(){
    if ( true) return true;
    return false;
  }
}