package net.opentsdb.core;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import org.hbase.async.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Cache module
 * @since 2.0
 */

public class Cache {


  private static final Logger LOG = LoggerFactory.getLogger(Cache.class);

  // May be byte data structure for looking fast
  private ArrayList<long[]> cacheIndexes = null;

  // Fragment Order = Ceil(Ti/range size)
  private int startFragmentOrder;

  private TSDB tsdb;

  // number of data(RowSeq) per memcached key
  private int numRangeSize;

  // The period data of one row HBase ( default 1 hr (3600 second) )
  private final long HBaseRowPeriod = 3600;
  private final long HBaseRowPeriodMs = HBaseRowPeriod * 1000;

  // Default charset for byte-String conversion
  private String charset = "ASCII";

  // Metadata size in byte
  private final short spanCount_numBytes = 2; // numBytes of number of Span
  private final short spanLength_numBytes = 4; // numBytes of Span
  private final short rowSeqCount_numBytes = 2;
  private final short rowSeqLength_numBytes = 4;
  private final short rowSeqKey_numBytes = 1;
  private final short rowSeqQualifier_numBytes = 2;
  private final short rowSeqValue_numBytes = 2;

  private final String memcachedHost = "memcached";
  private final int memcachedPort = 11211;
  private final int memcachedExpiredTime = 20;
  private final int memcachedVerifyingTime = 1;

  public Cache(TSDB tsdb) {
    LOG.debug("Create Cache object");
    this.tsdb = tsdb;
    this.numRangeSize = 4;
  }



  public ArrayList<CacheFragment> buildCacheFragments(TsdbQuery tsdbQuery){
    ArrayList<CacheFragment> cacheFragments = new ArrayList<CacheFragment>();

    // First miss
    if(isCacheIndexesEmpty()) {
      CacheFragment cacheFragment = new CacheFragment(tsdbQuery.getStartTime(), tsdbQuery.getEndTime(), false);
      cacheFragments.add(cacheFragment);
    }else {

    }
    // XOR operation for finding which part in cache or not?

    return cacheFragments;
  }

  public Deferred<TreeMap<byte[], Span>> findCache(CacheFragment fragment){
    // get time
    long startTime = fragment.getStartTime();
    long endTime = fragment.getEndTime();

    // calc

    // find in cache

    return null;
  }

  // -------------------------- //
  // storeCache helper functions //
  // -------------------------- //

  public int startTimeToFragmentOrder(long time){
    return (int)Math.ceil(time/(numRangeSize * HBaseRowPeriodMs));
  }

  public int endTimeToFragmentOrder(long time){
    return (int)Math.floor(time/(numRangeSize * HBaseRowPeriodMs));
  }

  public int findStartRowSeq(ArrayList<RowSeq> rowSeqs, long startTime_fo){
    LOG.debug("start findStartRowSeq");
    LOG.debug(startTime_fo+"");
    int i;
    for ( i = 0; i < rowSeqs.size(); i++) {
      long baseTime = rowSeqs.get(i).baseTime();
      LOG.debug(baseTime + " | " + rowSeqs.get(i));
      LOG.debug(baseTime + " >= " + startTime_fo + " AND " + baseTime + " < " + (startTime_fo + HBaseRowPeriod*numRangeSize));
      if (baseTime >= startTime_fo && baseTime < ( startTime_fo + HBaseRowPeriod*numRangeSize ) ){
        return i;
      }
    }
    return -1;
  }

  public Deferred<Boolean> setMemcached(MemcachedClient client, HashMap<String, byte[]> item){
    LOG.debug("setMemcached start");
    if (client == null) {
      String msg = "MemcachedClient object is null";
      LOG.error(msg);
      return Deferred.fromError(new Exception(msg));
    }
    LOG.debug("setMemcached client ready ");
    String key = item.entrySet().iterator().next().getKey();
    byte[] value = item.entrySet().iterator().next().getValue();
    LOG.debug("setMemcached data: ("+ key +") | " + Arrays.toString(value));
    OperationFuture<Boolean> future = client.set(key, memcachedExpiredTime, value);
    LOG.debug("setMemcached set!");
    try {
      return Deferred.fromResult(future.get(memcachedVerifyingTime, TimeUnit.SECONDS));
    }catch (Exception e){
        String msg = "Failed to store value for key" + key + " : " + e.getMessage();
        LOG.error(msg);
        return Deferred.fromError(new Exception(msg));
    }
  }

  public Deferred<Boolean> storeCache(CacheFragment fragment, TreeMap<byte[], Span> spans){
    // save to memached
    // find fragment order of start & end time
    final ArrayList<Deferred<Boolean>> deferreds = new ArrayList<Deferred<Boolean>>();
    MemcachedClient memcachedClientTmp = null;
    try {
      memcachedClientTmp = new MemcachedClient(new
        InetSocketAddress(memcachedHost, memcachedPort));
    }catch(IOException e){
      return Deferred.fromError(e);
    }
    LOG.debug("Connected to memcached server");
    final MemcachedClient memcachedClient = memcachedClientTmp;

    long startTime = fragment.getStartTime();
    long endTime = fragment.getEndTime();

    int start_fo = startTimeToFragmentOrder(startTime);
    int end_fo = endTimeToFragmentOrder(endTime); // Not include end fragment order

    long startTime_fo = start_fo * HBaseRowPeriod * numRangeSize;
    long endTime_fo = end_fo * HBaseRowPeriod * numRangeSize;
    // a result (TreeMap<byte[], Span>) can be more than one

    LOG.debug(start_fo + " " + end_fo + " " + startTime_fo);
    // The number of rest of RowSeq in previous Span
    int remainingRowSeq = 0;
    ArrayList<RowSeq> restRowSeq = null;
    int spanCount = 0;
    for(Map.Entry<byte[], Span> entry : spans.entrySet()) {
      LOG.debug("Span " + (spanCount + 1));
      // Group row key
      Span span = entry.getValue();  // ignore the key
      ArrayList<RowSeq> rowSeqs = span.getRows();
      LOG.debug("get rowSeqs size: " + rowSeqs.size());
      int i;
      int start_rowSeq = 0;
      // First span only for defining starting fragment order
      if (spanCount == 0){
        start_rowSeq = findStartRowSeq(rowSeqs, startTime_fo);
        if (start_rowSeq < 0 ){
          String msg = "Can't find starting rowSeq fragment order";
          LOG.error(msg);
          return Deferred.fromError(new Exception(msg));
        }
        LOG.debug("start_rowSeq index: " + start_rowSeq);
        LOG.debug("Finish findStartRowSeq");
      } else if (remainingRowSeq > 0 ){
        // This condition will happen on up to 2 spans only
        // if it remain some rowSeq in previous Span, merge `restRowSeq` and rowSeq of this Span
        ArrayList<RowSeq> tmp = new ArrayList<RowSeq>();
        tmp.addAll(restRowSeq);
        tmp.addAll(new ArrayList<RowSeq>(rowSeqs.subList( 0 , numRangeSize - remainingRowSeq)));
        start_rowSeq = numRangeSize - remainingRowSeq;
        HashMap<String, byte[]> item = serializeRowSeq(tmp);
        // send `item` to cache
        deferreds.add(setMemcached(memcachedClient, item));
      }

      // TODO: bottleneck processing
      for(i = start_rowSeq; i< rowSeqs.size(); i+= numRangeSize) {
        if (i + numRangeSize > rowSeqs.size())
          break;
        HashMap<String, byte[]> item = serializeRowSeq(rowSeqs, i, numRangeSize);
        LOG.debug("RowSeq index("+i+") : "+ item.entrySet().iterator().next().getKey());
        // send `item` to cache
        deferreds.add(setMemcached(memcachedClient, item));
      }
      remainingRowSeq = rowSeqs.size() - i;
      LOG.debug("remainingRowSeq = "+remainingRowSeq);
      if (remainingRowSeq > 0 ){
        // copy the rest of this rowseq into `restRowSeq`
        try {
          LOG.debug(rowSeqs.size() + " " + (i - numRangeSize) + " " + (i - numRangeSize + remainingRowSeq));
          restRowSeq = new ArrayList<RowSeq>(rowSeqs.subList(i - numRangeSize, i - numRangeSize + remainingRowSeq));

        }catch (IndexOutOfBoundsException e){
          return Deferred.fromError(e);
        }
      }
      // No need to define ending fragment order
      // In the Last Span if `remainingRowSeq` > 0 means we leave the rest!
      spanCount ++;
    }

    class UpdateCacheCB implements Callback<Boolean, ArrayList<Boolean>> {
      @Override
      public Boolean call(final ArrayList<Boolean> result) {
        memcachedClient.shutdown();
        // TODO: update cacheIndexes
        LOG.debug("UpdateCacheCB");
        return true;
      }
    }
    return Deferred.group(deferreds).addCallback(new UpdateCacheCB());
  }

  // Example TreeMap
//
//  Key: [B@b58523d.
//  Value: Span(4 rows,
//   [
//    RowSeq([0, 0, 1, 86, -123, 95, 16, 0, 0, 1, 0, 0, 1] (metric=level), base_time=1451581200 (Fri Jan 01 00:00:00 ICT 2016)(datapoints=1), (qualifier=[[0, 0]]), (values=[[32]]),
//   ]


  // -------------------------- //
  // Serialize helper functions //
  // -------------------------- //

  private byte[] numberToBytes(int n, int numBytes) {
    if (numBytes == 1) {
      byte[] b = new byte[1];
      b[0] = (byte) n;
      return b;
    } else if (numBytes == 2)
      return Bytes.fromShort((short) n);
    else if (numBytes == 4)
      return Bytes.fromInt(n);
    return null;
  }

  private byte[] arrayListToBytes(ArrayList<byte[]> bytes){
    int resultValueSize = 0;
    for (byte[] tmp : bytes){
      resultValueSize += tmp.length;
    }

    byte[] value = new byte[resultValueSize];
    // Merge all byte arrayList into one byte array
    int position = 0;
    for (byte[] tmp : bytes){
      System.arraycopy(tmp,0,value, position ,tmp.length);
      position += tmp.length;
    }
    return value;
  }

  private byte[] generateRowSeqBytes(RowSeq row){
    ArrayList<byte[]> tmpValues = new ArrayList<byte[]>();

    tmpValues.add(numberToBytes(row.getKey().length, rowSeqKey_numBytes));
    tmpValues.add(row.getKey());
    tmpValues.add(numberToBytes(row.getQualifiers().length, rowSeqQualifier_numBytes));
    tmpValues.add(row.getQualifiers());
    tmpValues.add(numberToBytes(row.getQualifiers().length, rowSeqValue_numBytes));
    tmpValues.add(row.getValues());

    return arrayListToBytes(tmpValues);
  }

  // group all of RowSeq into a key ( in cache )
  private HashMap<String, byte[]> serializeRowSeq(ArrayList<RowSeq> rowSeqs){
    return serializeRowSeq(rowSeqs, 0, rowSeqs.size());
  }

  // Convert TreeMap<Byte[], Span> (Raw data from hbase) into a pair of key and value, for storing in memcached
  // group all of RowSeq into a key ( in cache ) in range
  private HashMap<String, byte[]> serializeRowSeq(ArrayList<RowSeq> rowSeqs, int start, int length){
    //TODO: Optimize size of variables and speed
    // TODO: Now All Span is stored in one key *****
    LOG.debug("serializeRowSeq start");
    // Assume that each span element is continuous data
    HashMap<String, byte[]> result = new HashMap<String, byte[]>();
    String key;
    // Get first key of the span
    // Convert byte[] key into Base64 Encoding
    try {
      key = Base64.getEncoder().encodeToString(rowSeqs.get(start).getKey());
    }catch (IndexOutOfBoundsException e) {
      // TODO: use errorback to handle exception
      LOG.error(e.getMessage());
      return null;
    }
    // Perform value
    ArrayList<byte[]> tmpValues = new ArrayList<byte[]>();
    // Add Number of Span
    for(int i = start; i< length; i++) {
      byte[] tmp = generateRowSeqBytes(rowSeqs.get(i));
      tmpValues.add(numberToBytes(tmp.length, rowSeqLength_numBytes));
      tmpValues.add(tmp);
    }
    byte[] value = arrayListToBytes(tmpValues);
    result.put(key, value);
    return result;
  }

  // ---------------------------- //
  // Deserialize helper functions //
  // ---------------------------- //

  private long getNumberBytesRange(byte[] bytes, long start, long len){
    long result;
    byte[] tmp = new byte[(int)len];
    byte[] tmp2 = {0,0,0,0};
    System.arraycopy(bytes, (int)start, tmp, 0, (int)len);
    // 1 , 5A
    for (int i = tmp.length - 1 ; i >= 0 ;i--){
      tmp2[i] = tmp[i];
    }
    result = Bytes.getUnsignedInt(tmp2);
    return result;
  }

  private RowSeq bytesRangeToRowSeq(byte[] bytes, long cursor) {
    RowSeq rowSeq = new RowSeq(tsdb);
    byte[] key;
    byte[] qualifiers;
    byte[] values;
    // Get key
    long keyLength = getNumberBytesRange(bytes, cursor, rowSeqKey_numBytes);
    cursor +=  rowSeqKey_numBytes;
    key = new byte[(int)keyLength];
    System.arraycopy(bytes, (int)cursor, key, 0, (int)keyLength);
    cursor += keyLength;
    rowSeq.setKey(key);

    // Get qualifiers
    long qualifiersLength = getNumberBytesRange(bytes, cursor, rowSeqQualifier_numBytes);
    cursor +=  rowSeqQualifier_numBytes;
    qualifiers = new byte[(int)qualifiersLength];
    System.arraycopy(bytes, (int)cursor, qualifiers, 0, (int)qualifiersLength);
    cursor += qualifiersLength;
    rowSeq.setQualifiers(qualifiers);

    // Get values
    long valuesLength = getNumberBytesRange(bytes, cursor, rowSeqValue_numBytes);
    cursor +=  rowSeqValue_numBytes;
    values = new byte[(int)valuesLength];
    System.arraycopy(bytes, (int)cursor, values, 0, (int)valuesLength);
    cursor += valuesLength;
    rowSeq.setValues(values);

    return rowSeq;
  }

  private Span bytesRangeToSpan(byte[] bytes, long cursor, long len){
    Span span = new Span(tsdb);
    long rowSeqCount = getNumberBytesRange(bytes, cursor, rowSeqCount_numBytes);
    cursor += rowSeqCount_numBytes;

    for (int i = 0 ;i < rowSeqCount;i++) {
      long rowSeq_length = getNumberBytesRange(bytes, cursor, rowSeqLength_numBytes);
      cursor += rowSeqLength_numBytes;
      span.addRowSeq(bytesRangeToRowSeq(bytes, cursor));
      cursor += rowSeqLength_numBytes + rowSeq_length;
    }

    return span;
  }

  private boolean isCacheIndexesEmpty(){
    if ( cacheIndexes == null ) return true;
    if ( cacheIndexes.size() == 0) return true;
    return false;
  }
}