package net.opentsdb.core;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.tsd.BadRequestException;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import org.hbase.async.Bytes;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.net.InetSocketAddress;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Cache module
 * @since 2.0
 */

public class Cache {


  private static final Logger LOG = LoggerFactory.getLogger(Cache.class);

  CacheLookupTable lookupTable = null;

  // Cache parameter

  private TSDB tsdb;

  // number of data(RowSeq) per memcached key
  private int numRangeSize;

  // The period data of one row HBase ( default 1 hr (3600 second) )
  private final long HBaseRowPeriod = 3600;
  private final long HBaseRowPeriodMs = HBaseRowPeriod * 1000;

  // Default charset for byte-String conversion
  private String charset = "ASCII";

  // Metadata size in byte
  public static final short spanCount_numBytes = 2; // numBytes of number of Span
  public static final short spanLength_numBytes = 4; // numBytes of Span
  public static final short rowSeqCount_numBytes = 2;
  public static final short ROWSEQ_LENGTH_NUMBYTES = 4;  // int
  public static final short ROWSEQ_KEY_NUMBYTES = 1; // byte
  public static final short ROWSEQ_QUALIFIER_NUMBYTES = 2; //short
  public static final short ROWSEQ_VALUE_NUMBYTES = 2; //short

  private static final short NUM_RANGE_SIZE = 16;

  private final String memcachedHost = "memcached";
  private final int memcachedPort = 11211;
  private final int memcachedExpiredTime = 0;
  private final int memcachedVerifyingTime = 1;

  public Cache(TSDB tsdb) {
    LOG.info("Create Cache object");
    this.tsdb = tsdb;
    modifyNumRangeSize(NUM_RANGE_SIZE);
    lookupTable = new CacheLookupTable();
    //LOG.info("Create Cache object with numRangeSize = " + numRangeSize);
  }

  public Cache(TSDB tsdb, int numRangeSize) {
    this(tsdb);
    modifyNumRangeSize(NUM_RANGE_SIZE);
  }

  public void modifyNumRangeSize(short numRangeSize){
    this.numRangeSize = numRangeSize;
    LOG.warn("num of range size (Cache) is changed: " + numRangeSize);
  }

  public void dropCaches(){
    lookupTable = new CacheLookupTable();
//    final MemcachedClient memcachedClient;
//    try {
//      memcachedClient = createMemcachedConnection();
//    }catch(IOException e){
//
//    }
//    LOG.debug("dropCaches: Connected to memcached server");
  }

  // ----- buildCacheFragments Helper -------

  public boolean getBitBoolean(long num, int position, int maxNumBits) throws Exception{
    // maxNumBits: max is 64
    // number of bit `num` should = maxNumBits
    if(Long.bitCount(num) > maxNumBits)
      throw new Exception("Number of bits is larger than maxNumBits!");
    if ( ((num >> ( maxNumBits - position - 1 ) ) & 1) == 1) return true;
    return false;
  }

  public ArrayList<CacheFragment> buildCacheFragmentsFromBits(ArrayList<Long> results, int startFragmentOrder, int endFragmentOrder, int indexSize) throws Exception{
    int startQueryBlockOrder = lookupTable.calcBlockOrder(startFragmentOrder);
    int endQueryBlockOrder = lookupTable.calcBlockOrder(endFragmentOrder);
    ArrayList<CacheFragment> fragments = new ArrayList<CacheFragment>();
    boolean currentCachedState; // true means in cache;
    boolean previousBit; // 0 or false means in cache;
    boolean currentBit; // 0 or false means in cache;
    long startTime_cacheFragment;
    long endTime_cacheFragment;
    int startFOBlock;
    int endFOBlock;
    int currentFO = 0;

    // First block only
    // -------------------------------------------------------------------------------
    // First CacheFragment create here
    startFOBlock = startFragmentOrder % indexSize;
    startTime_cacheFragment = fragmentOrderToStartTime(startFragmentOrder);
    previousBit = getBitBoolean(results.get(0).longValue(), startFOBlock, indexSize);
    currentCachedState = !previousBit;

    // body blocks
    // -------------------------------------------------------------------------------
    for(int i = 0; i < results.size() ; i++){
      if (i != 0 ) startFOBlock = 0;
      endFOBlock = ( i == results.size() - 1 ) ? endFragmentOrder % indexSize : indexSize;
      for(int j = startFOBlock; j < endFOBlock ; j++ ){
        currentBit = getBitBoolean(results.get(i).longValue(), j, indexSize);
        // toggle state
        if(currentBit != previousBit){
          // TODO: Test currentFO
          currentFO = (i + startQueryBlockOrder) * indexSize + ( j - 1 );
//          currentFO = i*indexSize + ( j - 1 ) + startQueryBlockOrder;
          endTime_cacheFragment = fragmentOrderToEndTime(currentFO);
//          LOG.debug("endTime_cacheFragment: "+ endTime_cacheFragment + " " + currentFO);
          fragments.add(new CacheFragment(startTime_cacheFragment, endTime_cacheFragment, currentCachedState));
          currentCachedState = !currentCachedState; // toggle state
          startTime_cacheFragment = fragmentOrderToStartTime(currentFO + 1); // next Fragment order
          previousBit = currentBit;
        }
      }
    }

    // Last block only
    // -------------------------------------------------------------------------------
    // Last CacheFragment create here
    if(currentFO <  endFragmentOrder) {
      endTime_cacheFragment = fragmentOrderToEndTime(endFragmentOrder);
      fragments.add(new CacheFragment(startTime_cacheFragment, endTime_cacheFragment, currentCachedState));
    }
    return fragments;
  }

  public ArrayList<CacheFragment> buildCacheFragments(TsdbQuery tsdbQuery){
    // Decision
    ArrayList<CacheFragment> cacheFragments;
    // First miss
    if(lookupTable.isEmpty()) {
      LOG.info("First Miss");
      cacheFragments = new ArrayList<CacheFragment>();
      cacheFragments.add(new CacheFragment(tsdbQuery.getStartTime(), tsdbQuery.getEndTime(), false));
    }else {
      // XOR operation for finding which part in cache or not?
      int start_fo = startTimeToFragmentOrder(tsdbQuery.getStartTime());
      // Including the end of fragment order
      int end_fo = endTimeToFragmentOrder(tsdbQuery.getEndTime()) - 1;

      LOG.debug("buildCacheFragments - Start FO: " + start_fo +  "( " +tsdbQuery.getStartTime()+")");
      LOG.debug("buildCacheFragments - End   FO: " + end_fo +  "( " +tsdbQuery.getEndTime()+")");

      // Build body
      ArrayList<Long> results = lookupTable.buildFragmentBits(start_fo, end_fo);
      try {
        cacheFragments = buildCacheFragmentsFromBits(results, start_fo, end_fo, lookupTable.getIndexSize());
      }catch(Exception e){
        LOG.error("buildCacheFragments: " + e.getMessage());
        return null;
      }

      // add head (not in cache) if exist
      if(tsdbQuery.getStartTime() < fragmentOrderToStartTime(start_fo)){
        CacheFragment firstFragment = cacheFragments.get(0);
        // TODO: recheck
        if(firstFragment.isInCache() == true && firstFragment.getStartTime() - 1 >= tsdbQuery.getStartTime())
          cacheFragments.add(0, new CacheFragment(
            tsdbQuery.getStartTime(),
            firstFragment.getStartTime() - 1, false));
        else
          firstFragment.setStartTime(tsdbQuery.getStartTime());
      }
      // add tail (not in cache) if exist
      if(tsdbQuery.getEndTime() > fragmentOrderToEndTime(end_fo)){
        CacheFragment lastFragment = cacheFragments.get(cacheFragments.size() - 1);
        if(lastFragment.isInCache() &&
           lastFragment.getEndTime() + 1 <= tsdbQuery.getEndTime())
          cacheFragments.add(new CacheFragment(
            lastFragment.getEndTime() + 1,
            tsdbQuery.getEndTime(), false));
        else
          lastFragment.setEndTime(tsdbQuery.getEndTime());
      }

      // Fix range
      CacheFragment lastFragment = cacheFragments.get(cacheFragments.size() - 1);
      if(lastFragment.getStartTime() == lastFragment.getEndTime()){
        lastFragment.setStartTime(lastFragment.getStartTime() - 1);
      }

      // Fix last nd range
      if(cacheFragments.size() >= 2) {
        CacheFragment lastNdFragment = cacheFragments.get(cacheFragments.size() - 2);
        if(lastNdFragment.getEndTime() == lastFragment.getStartTime()){
          lastNdFragment.setEndTime(lastNdFragment.getEndTime() - 1);
        }
      }

    }

    LOG.info("buildCacheFragments: List of CacheFragment:");
    for(final CacheFragment cf: cacheFragments){
      LOG.info("buildCacheFragments: " + cf.toString());
    }

    return cacheFragments;
  }

  // -------------------------- //
  // findCache helper functions //
  // -------------------------- //

  public String encodeKey(byte[] key) throws Exception{
    return Base64.getEncoder().encodeToString(key);
  }

  public byte[] decodeKey(String key) throws Exception{
    return Base64.getDecoder().decode(key);
  }

  public ArrayList<byte[]> processKeyCache(CacheFragment fragment, byte[] keyBytesTemplate, short metric_bytes){
    // Generate a list of keys what to get from Memcached
    ArrayList<byte[]> result = new ArrayList<byte[]>();
    LOG.debug("Key Template: " + Arrays.toString(keyBytesTemplate));
    // get a list of Fragment order that to get
    // 1 fo = 1 key

    // Retrieve from cache range
    int start_fragmentOrder = timeToFragmentOrder(fragment.getStartTime()); // use fragment order instead of start_fo
    int end_fragmentOrder = endTimeToFragmentOrder(fragment.getEndTime());
    LOG.debug("process key for getting cache: " +start_fragmentOrder+ " , " + end_fragmentOrder);
    for (int fo = start_fragmentOrder; fo <= end_fragmentOrder; fo ++){
      long timestamp = fragmentOrderToStartTime(fo);
      // we only accept positive unix epoch timestamps in seconds or milliseconds
      if (timestamp < 0 || ((timestamp & Const.SECOND_MASK) != 0 &&
        timestamp > 9999999999999L)) {
        throw new IllegalArgumentException((timestamp < 0 ? "negative " : "bad")
          + " timestamp=" + timestamp
          + " when trying to processKeyCache with keyBytesTemplate: " + Arrays.toString(keyBytesTemplate));
      }

      long base_time;

      if ((timestamp & Const.SECOND_MASK) != 0) {
        // drop the ms timestamp to seconds to calculate the base timestamp
        base_time = ((timestamp / 1000) -
          ((timestamp / 1000) % Const.MAX_TIMESPAN));
      } else {
        base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));
      }
      byte[] key = new byte[keyBytesTemplate.length];
      System.arraycopy(keyBytesTemplate, 0, key, 0 , keyBytesTemplate.length);
      Bytes.setInt(key, (int) base_time, metric_bytes);
//      LOG.debug("Getting Key: " + Arrays.toString(key));
      long time = Bytes.getUnsignedInt(key, Const.SALT_WIDTH() + tsdb.metrics.width()) * 1000;
      //LOG.debug("Getting key (FO): " +Arrays.toString(key)+  " - " + time + "  " + timeToFragmentOrder(time));
      result.add(key);
    }
    return result;
  }


  // -------------------------- //
  // storeCache helper functions //
  // -------------------------- //

  public int timeToFragmentOrder(long time){
    return (int)Math.floor((double)time/(numRangeSize * HBaseRowPeriodMs));
  }

  public int startTimeToFragmentOrder(long time){
    return (int)Math.ceil((double)(time+1)/(numRangeSize * HBaseRowPeriodMs));
  }

  public long fragmentOrderToStartTime(int fragmentOrder){
    return fragmentOrder * numRangeSize * HBaseRowPeriodMs;
  }

  public int endTimeToFragmentOrder(long time){
    // return < 0 means no fo, no need to cache
    return (int)Math.floor((double)(time)/(numRangeSize * HBaseRowPeriodMs));
  }

  public long fragmentOrderToEndTime(int fragmentOrder){
    return ( fragmentOrder + 1 ) * numRangeSize * HBaseRowPeriodMs - 1;
  }

  public int findStartRowSeq(ArrayList<RowSeq> rowSeqs, long startTime){
    LOG.debug("findStartRowSeq: Finding (" + startTime+") in rowSeqs, for finding start point of the fragment(group of rowSeq)");
    int i;
    for ( i = 0; i < rowSeqs.size(); i++) {
      long baseTime = rowSeqs.get(i).baseTime() * 1000; // convert to milliseconds
//      LOG.debug(baseTime + " >= " + startTime_fo + " AND " + baseTime + " < " + (startTime_fo + HBaseRowPeriod*numRangeSize));
      if (baseTime >= startTime && baseTime < ( startTime + HBaseRowPeriodMs * numRangeSize ) ){
        LOG.debug("findStartRowSeq: the result : " + i);
        return i;
      }
      //LOG.debug("findStartRowSeq: " + baseTime + " is not in between " + startTime + " - " + (startTime + HBaseRowPeriodMs * numRangeSize) + " | RowSeq: " + rowSeqs.get(i).size() + " datapoints");
    }
    return -1;
  }

  public Deferred<Boolean> setMemcached(MemcachedClient client, HashMap<String, byte[]> item){
//    LOG.debug("setMemcachedAsync start");
    if (client == null) {
      String msg = "MemcachedClient object is null";
      LOG.error("setMemcached: " + msg);
      return Deferred.fromError(new Exception(msg));
    }
//    LOG.debug("setMemcachedAsync client ready ");
    String key = item.entrySet().iterator().next().getKey();
    byte[] value = item.entrySet().iterator().next().getValue();
//    LOG.debug("setMemcachedAsync data: ("+ key +") | " + value.length + " btyes");

    OperationFuture<Boolean> future = client.set(key, memcachedExpiredTime, value);

//    LOG.debug("setMemcachedAsync set!");
    try {
//      byte[] result = getMemcachedAsync(client, key).joinUninterruptibly();
//      LOG.debug("Get result " + key + ": " + Arrays.toString(result));
      return Deferred.fromResult(future.get(memcachedVerifyingTime, TimeUnit.SECONDS));
    }catch (Exception e){
        String msg = "Failed to store value for key" + key + " : " + e.getMessage();
        LOG.error("setMemcached: " + msg);
        return Deferred.fromError(new Exception(msg));
    }
  }

  public MemcachedClient createMemcachedConnection() throws IOException{
    return new MemcachedClient(new InetSocketAddress(memcachedHost, memcachedPort));
  }

  public Deferred<Boolean> storeCache(CacheFragment fragment, TreeMap<byte[], Span> spans){
    // mildronize: debug
//    final FileWriter fileWriter;
//    final PrintWriter printWriter;
//    try {
//      fileWriter = new FileWriter("/var/log/opentsdb/store." + System.currentTimeMillis());
//      printWriter = new PrintWriter(fileWriter);
//    }catch (Exception e){
//      return Deferred.fromError(e);
//    }

    // save to memached
    // find fragment order of start & end time
    final ArrayList<Deferred<Boolean>> deferreds = new ArrayList<Deferred<Boolean>>();

    final MemcachedClient memcachedClient;
    try {
      memcachedClient = createMemcachedConnection();
    }catch(IOException e){
      return Deferred.fromError(e);
    }
    LOG.debug("storeCache: Connected to memcached server");

    long startTime = fragment.getStartTime();
    long endTime = fragment.getEndTime();

    final int start_fo = startTimeToFragmentOrder(startTime);
    final int end_fo = endTimeToFragmentOrder(endTime);

    long startTime_fo = fragmentOrderToStartTime(start_fo);
//    long endTime_fo = fragmentOrderToEndTime(end_fo);
    // a result (TreeMap<byte[], Span>) can be more than one

    LOG.debug("storeCache: start_fo:" +start_fo + " end_fo: " + end_fo + " startTime: " + startTime_fo);
    // The number of rest of RowSeq in previous Span
    int remainingRowSeq = 0;

    HashMap<String, byte[]> item;
    ArrayList<RowSeq> restRowSeq = null;
    int spanCount = 0;
    for(Map.Entry<byte[], Span> entry : spans.entrySet()) {
      LOG.debug("storeCache: Span " + (spanCount + 1));
      // Group row key
      Span span = entry.getValue();  // ignore the key
      ArrayList<RowSeq> rowSeqs = span.getRows();
      LOG.debug("storeCache: get rowSeqs size: " + rowSeqs.size());
      int i;
      int start_rowSeq = 0;
      // First span only for defining starting fragment order
      if (spanCount == 0){
        if (rowSeqs.size() < numRangeSize){
          LOG.info("storeCache: rowSeqs size is too small, ignore cache");
          return Deferred.fromResult(true);
        }
        start_rowSeq = findStartRowSeq(rowSeqs, startTime_fo);
        if (start_rowSeq < 0 ){
          String msg = "storeCache: Can't find starting rowSeq fragment order";
          LOG.error(msg);
          return Deferred.fromError(new Exception(msg));
        }
//        LOG.debug("Finish findStartRowSeq");
      } else if (remainingRowSeq > 0 ){
        // This condition will happen on up to 2 spans only
        // if it remain some rowSeq in previous Span, merge `restRowSeq` and rowSeq of this Span
        ArrayList<RowSeq> tmp = new ArrayList<RowSeq>();
        tmp.addAll(restRowSeq);
        tmp.addAll(new ArrayList<RowSeq>(rowSeqs.subList( 0 , numRangeSize - remainingRowSeq)));
        start_rowSeq = numRangeSize - remainingRowSeq;
        try {
          item = serializeRowSeq(tmp);
        }catch (Exception e){
          LOG.error("storeCache: " + e.getMessage());
          return Deferred.fromError(e);
        }
        // send `item` to cache
        deferreds.add(setMemcached(memcachedClient, item));
        // mildronize: debug
        //String key = item.entrySet().iterator().next().getKey();
        //byte[] value = item.entrySet().iterator().next().getValue();
        //printWriter.println(key+"#("+value.length +")#"+ Arrays.toString(value) +"$");
      }

      // TODO: bottleneck processing
      for(i = start_rowSeq; i< rowSeqs.size(); i+= numRangeSize) {
        if (i + numRangeSize > rowSeqs.size())
          break;
        try {
          item = serializeRowSeq(rowSeqs, i, numRangeSize);
        }catch (Exception e){
          LOG.error("storeCache: " + e.getMessage());
          throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
            "Can't serialize RowSeq into byte array: ");
        }
        //LOG.debug("storeCache: RowSeq index("+i+") : "+ item.entrySet().iterator().next().getKey());
        // send `item` to cache
        deferreds.add(setMemcached(memcachedClient, item));
        // mildronize: debug
        //String key = item.entrySet().iterator().next().getKey();
        //byte[] value = item.entrySet().iterator().next().getValue();
        //printWriter.println(key+"#("+value.length +")#"+ Arrays.toString(value) +"$");
      }
      remainingRowSeq = rowSeqs.size() - i;
      LOG.debug("storeCache: remainingRowSeq = "+remainingRowSeq);
      if (remainingRowSeq > 0 ){
        // copy the rest of this rowseq into `restRowSeq`
        try {
          int start_remainingRowSeq = i - numRangeSize;
          start_remainingRowSeq = start_remainingRowSeq < 0 ?0:start_remainingRowSeq;
          //LOG.debug("storeCache: rowSeqs.size():" + rowSeqs.size() + " start_remainingRowSeq: " + (start_remainingRowSeq));
          restRowSeq = new ArrayList<RowSeq>(rowSeqs.subList(start_remainingRowSeq, rowSeqs.size()));
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
        //printWriter.close();
        LOG.debug("storeCache.UpdateCacheCB.call: memcachedClient.shutdown");
        lookupTable.mark(start_fo, result.size());
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

//  public byte[] numberToBytes(int n, int numBytes) {
//    if (numBytes == 1) {
//      byte[] b = new byte[1];
//      b[0] = (byte) n;
//      return b;
//    } else if (numBytes == 2)
//      return Bytes.fromShort((short) n);
//    else if (numBytes == 4)
//      return Bytes.fromInt(n);
//    return null;
//  }
  public byte[] numberToBytes(int n , int numBytes) throws Exception{
    try {
      ByteBuffer allocated = ByteBuffer.allocate(numBytes);
      if (numBytes == 1)
        return allocated.put((byte)n).array();
      else if (numBytes == 2)
        return allocated.putShort((short)n).array();
      else if (numBytes == 4)
        return allocated.putInt(n).array();
    } catch( IllegalArgumentException e ) {
      throw new IllegalArgumentException("numBytes must be positive integer" + Arrays.toString(e.getStackTrace()));
    } catch( BufferOverflowException e ) {
      throw new IllegalArgumentException("BufferOverflowException: Try to convert " + n + " in " + numBytes + " num bytes  " + Arrays.toString(e.getStackTrace()));
    } catch( Exception e ) {
      throw new Exception(e.getClass() + " " + Arrays.toString(e.getStackTrace()));
    }
    throw new Exception("Support byte conversion only: byte, short, long");
  }

  public byte[] arrayListToBytes(ArrayList<byte[]> bytes){
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

  private byte[] generateRowSeqBytes(RowSeq row) throws Exception{
    ArrayList<byte[]> tmpValues = new ArrayList<byte[]>();

      tmpValues.add(numberToBytes(row.getKey().length, ROWSEQ_KEY_NUMBYTES));
      tmpValues.add(row.getKey());
      tmpValues.add(numberToBytes(row.getQualifiers().length, ROWSEQ_QUALIFIER_NUMBYTES));
      tmpValues.add(row.getQualifiers());
      tmpValues.add(numberToBytes(row.getValues().length, ROWSEQ_VALUE_NUMBYTES));
      tmpValues.add(row.getValues());

    return arrayListToBytes(tmpValues);
  }

  // group all of RowSeq into a key ( in cache )
  private HashMap<String, byte[]> serializeRowSeq(ArrayList<RowSeq> rowSeqs) throws Exception{
    return serializeRowSeq(rowSeqs, 0, rowSeqs.size());
  }

  // Convert TreeMap<Byte[], Span> (Raw data from hbase) into a pair of key and value, for storing in memcached
  // group all of RowSeq into a key ( in cache ) in range
  private HashMap<String, byte[]> serializeRowSeq(ArrayList<RowSeq> rowSeqs, int start, int length) throws Exception{
    //TODO: Optimize size of variables and speed
    // TODO: Now All Span is stored in one key *****
//    LOG.debug("serializeRowSeq start");
    // Assume that each span element is continuous data
    HashMap<String, byte[]> result = new HashMap<String, byte[]>();
    String key;
    // Get first key of the span
    // Convert byte[] key into Base64 Encoding
    // debug
    long time = Bytes.getUnsignedInt(rowSeqs.get(start).getKey(), Const.SALT_WIDTH() + tsdb.metrics.width()) * 1000;
//    LOG.debug("Storing Key: " + Arrays.toString(rowSeqs.get(start).getKey()));
    //LOG.debug("serializeRowSeq: Storing key : " + Arrays.toString(rowSeqs.get(start).getKey()) + " Timestamp: " + time + " FragmentOrder: " + startTimeToFragmentOrder(time));
    key = encodeKey(rowSeqs.get(start).getKey());
    // Perform value
    ArrayList<byte[]> tmpValues = new ArrayList<byte[]>();
    // Add Number of Span
    int end = start + length;
    Span span_tmp = new Span(tsdb);
    //LOG.debug("serializeRowSeq: Creating a group of RowSeq ( Size: " +length + ") " );
    for(int i = start; i< end; i++) {
      //LOG.debug("serializeRowSeq: Starting RowSeq("+ i +")");
      byte[] tmp = generateRowSeqBytes(rowSeqs.get(i));
      byte[] lenByte = numberToBytes(tmp.length, ROWSEQ_LENGTH_NUMBYTES);
      //LOG.debug("Out: " + Arrays.toString(lenByte) + " num: " + tmp.length);
      tmpValues.add(lenByte);
      tmpValues.add(tmp);
      //LOG.debug("serializeRowSeq: RowSeq("+ i +")"  );
      // For debug
//      span_tmp.addRowSeq(rowSeqs.get(i));
    }
    byte[] value = arrayListToBytes(tmpValues);
    // deserialize check
    //Span exceptedSpan = bytesRangeToSpan(value, 0);

//    if (isSpanEqual(span_tmp, exceptedSpan)){
//      LOG.info("serialize and deserialize are correct");
//    }else {
//      LOG.error("serialize and deserialize are incorrect!");
////      LOG.debug("serialize output: " + Arrays.toString(value));
//      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
//        "serialize and deserialize are incorrect");
//    }

    result.put(key, value);
    return result;
  }

  private boolean isRowSeqEqual(RowSeq a, RowSeq b){
    if(!Arrays.equals(a.getKey(), b.getKey()))
      return false;
    if(!Arrays.equals(a.getValues(), b.getValues()))
      return false;
    if(!Arrays.equals(a.getQualifiers(), b.getQualifiers()))
      return false;
    return true;
  }

  private boolean isSpanEqual(Span a, Span b){
    if (a.getRows().size() != b.getRows().size())
      return false;
    ArrayList<RowSeq> a_rows = a.getRows();
    ArrayList<RowSeq> b_rows = b.getRows();
    for (int i =0; i < a_rows.size();i++){
      if (!isRowSeqEqual(a_rows.get(i), b_rows.get(i))){
        return false;
      }
    }
    return true;
  }

  // ---------------------------- //
  // Deserialize helper functions //
  // ---------------------------- //

  public long getNumberBytesRange(byte[] bytes, long start, long len) throws Exception{
    long result;
//    byte[] tmp = new byte[(int)len];
//    byte[] long_tmp = {0,0,0,0};
////    LOG.debug(start + " - " + Arrays.toString(bytes) + " " + len);
//    System.arraycopy(bytes, (int)start, tmp, 0, (int)len);
//    // 1 , 5A
//    int j = long_tmp.length - 1;
//    for (int i = 0 ; i < tmp.length; i++){
//      long_tmp[j] = tmp[i];
//    }
//    result = Bytes.getUnsignedInt(long_tmp);

//    byte[] newBytesArray = new byte[(int)len];
//    System.arraycopy(bytes, (int)start, newBytesArray, 0, (int)len);
    try {
      ByteBuffer wrapped = ByteBuffer.wrap(bytes, (int)start, (int)len);
      if (len == 1)
        return wrapped.get();
      else if (len == 2)
        return  wrapped.getShort();
      else if (len == 4)
        return  wrapped.getInt();
    } catch( IndexOutOfBoundsException  e ) {
      throw new IndexOutOfBoundsException ( e.getMessage() + Arrays.toString(e.getStackTrace()));
    } catch( BufferUnderflowException e ) {
      throw new IllegalArgumentException("BufferUnderflowException: Try to convert " + len + " num bytes into number " + Arrays.toString(e.getStackTrace()));
    } catch( Exception e ) {
      throw new Exception(e.getClass() + " " + Arrays.toString(e.getStackTrace()));
    }
    throw new Exception("Support byte conversion only: byte, short, long");
  }

  private RowSeq bytesRangeToRowSeq(byte[] bytes, long cursor) throws Exception{
    RowSeq rowSeq = new RowSeq(tsdb);
    byte[] key;
    byte[] qualifiers;
    byte[] values;
    // Get key
    long keyLength = getNumberBytesRange(bytes, cursor, ROWSEQ_KEY_NUMBYTES);
//    LOG.debug("bytesRangeToRowSeq: " + cursor + " - " + Arrays.toString(Arrays.copyOfRange(bytes, (int)cursor, (int)(cursor+ROWSEQ_KEY_NUMBYTES))) + " " + ROWSEQ_KEY_NUMBYTES + " key Len:  " + keyLength);
    cursor += ROWSEQ_KEY_NUMBYTES;
    key = new byte[(int)keyLength];
    System.arraycopy(bytes, (int)cursor, key, 0, (int)keyLength);
    cursor += keyLength;
    rowSeq.setKey(key);

    // Get qualifiers
    long qualifiersLength = getNumberBytesRange(bytes, cursor, ROWSEQ_QUALIFIER_NUMBYTES);
    cursor += ROWSEQ_QUALIFIER_NUMBYTES;
    qualifiers = new byte[(int)qualifiersLength];
    System.arraycopy(bytes, (int)cursor, qualifiers, 0, (int)qualifiersLength);
    cursor += qualifiersLength;
    rowSeq.setQualifiers(qualifiers);

    // Get values
    long valuesLength = getNumberBytesRange(bytes, cursor, ROWSEQ_VALUE_NUMBYTES);
    cursor +=  ROWSEQ_VALUE_NUMBYTES;
    values = new byte[(int)valuesLength];
    //LOG.debug(Arrays.toString(bytes) + " " + cursor + "  " + Arrays.toString(values) + " " + valuesLength);
    System.arraycopy(bytes, (int)cursor, values, 0, (int)valuesLength);
    cursor += valuesLength;

    rowSeq.setValues(values);
    return rowSeq;
  }

  public Span bytesRangeToSpan(byte[] bytes, long cursor) throws Exception {
    Span span = new Span(tsdb);

    for (int i = 0; i < numRangeSize; i++) {
      long rowSeq_length = getNumberBytesRange(bytes, cursor, ROWSEQ_LENGTH_NUMBYTES);
      cursor += ROWSEQ_LENGTH_NUMBYTES;
      span.addRowSeq(bytesRangeToRowSeq(bytes, cursor));
      cursor += rowSeq_length;
    }

    return span;
  }

  public Deferred<byte[]> getMemcachedAsync(MemcachedClient client, String key){
//    LOG.debug("getMemcacheAsync start");
    if (client == null) {
      String msg = "MemcachedClient object is null";
      LOG.error("getMemcachedAsync: " + msg);
      return Deferred.fromError(new Exception(msg));
    }
//    LOG.debug("getMemcacheAsync client ready ");
    GetFuture<Object> future = client.asyncGet(key);
//    LOG.debug("getMemcacheAsync get!");
    try {
      return Deferred.fromResult((byte[])future.get(memcachedVerifyingTime, TimeUnit.SECONDS));
    }catch (Exception e){
      String msg = "Failed to get value for key" + key + " : " + e.getMessage();
      LOG.error("getMemcachedAsync: " + msg);
      return Deferred.fromError(new Exception(msg));
    }
  }


  // Convert a pair of key and value in Memcached into TreeMap<Byte[], Span> (Raw data from hbase)
  // Extract array of byte into a Span ( list of RowSeq)
  public Span deserializeToSpan(ArrayList<byte[]> results) throws Exception{
    //TODO: Optimize size of variables and speed
//    LOG.debug("deserializeToSpan: Starting deserializeToSpan: ");
    Span span = new Span(tsdb);
    for (final byte[] result : results){
      span.addAll(bytesRangeToSpan(result, 0).getRows());
    }
    return span;
  }

}