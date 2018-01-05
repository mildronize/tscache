package net.opentsdb.core;

import com.stumbleupon.async.Deferred;
import com.sun.rowset.internal.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map;

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

  // Default charset for byte-String conversion
  private String charset = "US-ASCII";

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

  // Example TreeMap
//
//  Key: [B@b58523d.
//  Value: Span(4 rows,
//   [
//    RowSeq([0, 0, 1, 86, -123, 95, 16, 0, 0, 1, 0, 0, 1] (metric=level), base_time=1451581200 (Fri Jan 01 00:00:00 ICT 2016)(datapoints=1), (qualifier=[[0, 0]]), (values=[[32]]),
//   ]

  private String bytesToString(byte[] bytes) throws UnsupportedEncodingException{
    return new String(bytes, charset);
  }
  private byte[] stringToBytes(String string) throws UnsupportedEncodingException{
    return string.getBytes(charset);
  }

  // Convert TreeMap<Byte[], Span> (Raw data from hbase) into a pair of key and value, for storing in memcached
  private HashMap<String, byte[]> serialize(TreeMap<byte[], Span> span){
    // Assume that each span element is continuous data
    HashMap<String, byte[]> result = new HashMap<String, byte[]>();

    // Get first key of the span
    try {
      String key = bytesToString(span.entrySet().iterator().next().getKey());
    }catch(UnsupportedEncodingException e){
      // TODO: use errorback to handle exception
      LOG.error(e.getMessage());
    }

    int resultValueSize = 0;
    ArrayList<byte[]> tmpValues = new ArrayList<byte[]>();
    for (Map.Entry<byte[], Span> element : span.entrySet()){
      //count all result value size
      byte[] k = element.getKey();
      tmpValues.add(k);
      resultValueSize += k.length;
      // in Span, can be many RowSeq
      Span spanTmp = element.getValue();
      for(RowSeq row: spanTmp.getRows() ) {

      }

    }

    byte[] value = new byte[resultValueSize];
    // Merge all byte arrayList into one byte array
    int position = 0;
    for (byte[] tmp : tmpValues){
      System.arraycopy(tmp,0,value, position ,tmp.length);
      position += tmp.length;
    }

    // coding
    result.put(key, value);
    return result;
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