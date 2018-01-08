package net.opentsdb.core;

import com.stumbleupon.async.Deferred;
import org.hbase.async.Bytes;
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

  private TSDB tsdb;

  // in seconds
  private int rangeSize;

  // number of data per memcached key
  private int numRangeSize;

  // The period data of one row HBase ( default 1 hr (3600 second) )
  private int HBaseRowPeriod = 3600;

  // Default charset for byte-String conversion
  private String charset = "US-ASCII";

  // Metadata size in byte
  private final short spanCount_numBytes = 2; // numBytes of number of Span
  private final short spanLength_numBytes = 4; // numBytes of Span
  private final short rowSeqCount_numBytes = 2;
  private final short rowSeqLength_numBytes = 4;
  private final short rowSeqKey_numBytes = 1;
  private final short rowSeqQualifier_numBytes = 2;
  private final short rowSeqValue_numBytes = 2;


  public void setNumRangeSize(int numRangeSize) {
    this.numRangeSize = numRangeSize;
    rangeSize = numRangeSize * HBaseRowPeriod;
  }

  public Cache(TSDB tsdb) {
    LOG.debug("Create Cache object");
    this.tsdb = tsdb;
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

  //  ---- serialize helper function -----

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

  private byte[] generateSpanBytes(Span span){
    // Final result is Span content
    ArrayList<byte[]> tmpValues = new ArrayList<byte[]>();
    tmpValues.add(numberToBytes(span.getRows().size(), rowSeqCount_numBytes));

    for(RowSeq row: span.getRows() ) {
      byte[] tmp = generateRowSeqBytes(row);
      tmpValues.add(numberToBytes(tmp.length, rowSeqLength_numBytes));
      tmpValues.add(tmp);
    }
    return arrayListToBytes(tmpValues);
  }

  // Convert TreeMap<Byte[], Span> (Raw data from hbase) into a pair of key and value, for storing in memcached
  private HashMap<String, byte[]> serialize(TreeMap<byte[], Span> span){
    //TODO: Optimize size of variables and speed
    // TODO: Now All Span is stored in one key *****

    // Assume that each span element is continuous data
    HashMap<String, byte[]> result = new HashMap<String, byte[]>();
    String key;
    // Get first key of the span

    try {
      key = bytesToString(span.entrySet().iterator().next().getKey());
    }catch(UnsupportedEncodingException e){
      // TODO: use errorback to handle exception
      LOG.error(e.getMessage());
    }

    ArrayList<byte[]> tmpValues = new ArrayList<byte[]>();
    // Add Number of Span

    tmpValues.add(numberToBytes(span.size(),spanCount_numBytes));
    for (Map.Entry<byte[], Span> element : span.entrySet()){
      Span tmpSpan = element.getValue();
      byte[] tmp = generateSpanBytes(tmpSpan);
      tmpValues.add(numberToBytes(tmp.length, spanLength_numBytes));
      tmpValues.add(tmp);
    }

    byte[] value = arrayListToBytes(tmpValues);

    // coding
    result.put(key, value);
    return result;
  }

  //  ---- Deserialize helper function -----

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

  // Convert a pair of key and value from memcached into TreeMap<Byte[], Span> (Raw data from hbase)
  private TreeMap<byte[], Span> deserialize(HashMap<String, byte[]> cached){
    TreeMap<byte[], Span> result = new TreeMap<byte[], Span>();
    // Assume that `cached` has only one element

    byte[] cachedValue = cached.entrySet().iterator().next().getValue();

    long cursor = 0;

    // plus current
    // got number of span

    long spanCount = getNumberBytesRange(cachedValue, cursor, spanCount_numBytes);
    cursor += spanCount_numBytes;
    for (int i = 0;i < spanCount; i++){
      long span_length = getNumberBytesRange(cachedValue, cursor, spanLength_numBytes);
      // get key from first row seq of Span
      byte[] key = {(byte)0};
      result.put(key, bytesRangeToSpan(cachedValue, cursor, span_length));
      // Move cursor to Next Span
      cursor += spanLength_numBytes + span_length;
    }

    // add detail of each Span in `result`

    return result;
  }


  private boolean isCacheIndexesEmpty(){
    if ( true) return true;
    return false;
  }
}