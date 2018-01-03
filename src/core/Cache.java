package net.opentsdb.core;

import com.stumbleupon.async.Deferred;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
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

  // number of data per memcached key
  private int numRangeSize;

  // The period data of one row HBase ( default 1 hr (3600 second) )
  private int HBaseRowPeriod = 3600;

  public void setNumRangeSize(int numRangeSize) {
    this.numRangeSize = numRangeSize;
    rangeSize = numRangeSize * HBaseRowPeriod;
  }

  public Cache() {
    LOG.debug("Create Cache object");

    setNumRangeSize(4);
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

  // Convert TreeMap<Byte[], Span> (Raw data from hbase) into a pair of key and value, for storing in memcached
  private HashMap<String, Byte[]> serialize(TreeMap<Byte[], Span> span){
    // Assume that each span element is continuous data



    return null;
  }

  // Convert a pair of key and value from memcached into TreeMap<Byte[], Span> (Raw data from hbase)
  private TreeMap<Byte[], Span> deserialize(HashMap<String, Byte[]> cached){
    return null;
  }


  private boolean isCacheIndexesEmpty(){
    if ( true) return true;
    return false;
  }
}