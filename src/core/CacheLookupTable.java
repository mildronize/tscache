package net.opentsdb.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class CacheLookupTable {

  private static final Logger LOG = LoggerFactory.getLogger(CacheLookupTable.class);

  public ArrayList<Long> getCacheIndexes() {
    return cacheIndexes;
  }

  public void setCacheIndexes(ArrayList<Long> cacheIndexes) {
    this.cacheIndexes = cacheIndexes;
  }

  // May be byte data structure for looking fast
  private ArrayList<Long> cacheIndexes= null;

  public int getIndexSize() {
    return indexSize;
  }

  // Number of bit per cacheIndex, Maximum: 64 (Size of long) default 64
  private int indexSize;

//  // Fragment Order = Ceil(Ti/range size)
//  private int startFragmentOrderBlock;
//
  private int blockOrder;

  public CacheLookupTable(){
    this.indexSize = 64;
    init();
  }

  public CacheLookupTable(int indexSize){
    this.indexSize = indexSize;
    init();
  }

  private void init(){
    ArrayList<Long> tmp = new ArrayList<Long>();
    setCacheIndexes(tmp);
  }

  public boolean isEmpty(){
    if ( cacheIndexes == null ) return true;
    if ( cacheIndexes.size() == 0) return true;
    return false;
  }

  public int calcBlockOrder(int fragmentOrder){
    return (int)(fragmentOrder / indexSize);
  }

  public int calcNumberEmptyBlock(int cacheIndexesSize, int incomingEndBlockOrder){
    int numberEmptyBlock = 0;
    if(cacheIndexesSize <= incomingEndBlockOrder){
      numberEmptyBlock = incomingEndBlockOrder - cacheIndexesSize + 1;
    }
    return numberEmptyBlock;
  }

  public int calcNumberMarkedBit(int start_fragmentOrder, int numFragment){
    int end_fragmentOrder = start_fragmentOrder + numFragment - 1;
    int incomingEndBlockOrder = calcBlockOrder(end_fragmentOrder);
    int incomingBlockOrder = calcBlockOrder(start_fragmentOrder);
    if (incomingBlockOrder == incomingEndBlockOrder){
      return numFragment;
    }else {
      return end_fragmentOrder % indexSize + 1;
    }
  }

  public void mark(int start_fragmentOrder, int numFragment) throws IndexOutOfBoundsException {
    int end_fragmentOrder = start_fragmentOrder + numFragment - 1;
    int incomingEndBlockOrder = calcBlockOrder(end_fragmentOrder);
    int incomingBlockOrder = calcBlockOrder(start_fragmentOrder);

    int numberEmptyBlock = calcNumberEmptyBlock(cacheIndexes.size(), incomingEndBlockOrder);
    for (int i = 0; i < numberEmptyBlock; i++) {
      cacheIndexes.add(emptyBlock());
    }

    int offset_head = start_fragmentOrder % indexSize;
    if(offset_head + numFragment > indexSize)
      cacheIndexes.set(incomingBlockOrder, cacheIndexes.get(incomingBlockOrder).longValue() |
        headPartialMarkedBlock(offset_head).longValue());
    // 2. Make body block with all 1 value
    for (int i = incomingBlockOrder + 1 ; i < incomingEndBlockOrder; i++){
      cacheIndexes.set(i, fulfillBlock());
    }
    // 3. Make tail block ( not fullfill)
    if (incomingBlockOrder != incomingEndBlockOrder){
      // One block incoming
      offset_head = 0;
    }
    int numberMarkedBits = calcNumberMarkedBit(start_fragmentOrder, numFragment);
    int offset_tail = indexSize - numberMarkedBits - offset_head;
    if(numberMarkedBits != 0)
      cacheIndexes.set(incomingEndBlockOrder, cacheIndexes.get(incomingEndBlockOrder).longValue() |
        tailPartialMarkedBlock(offset_tail, offset_head).longValue());
  }

  public Long emptyBlock(){
    return new Long(0L);
  }

  public Long fulfillBlock(){
    return new Long(-1L);
  }

  private Long headPartialMarkedBlock(int offset){
//    Long block = new Long(0b0000000000000000000000000000000000000000000000000000000000000000L);
    long block = 0;
    int number = indexSize - offset;
    for (int i=0;i < number  ;i++) {
      block = (block << 1) | 1;
    }
    return new Long(block);
  }

  public Long tailPartialMarkedBlock(int offset_tail, int offset_head){
    long block = 0;
    int number = indexSize - offset_tail - offset_head;
    for (int i=0;i < offset_head  ;i++) {
      block = (block << 1);
    }
    for (int i=0;i < number  ;i++) {
      block = (block << 1) | 1;
    }
    // append zero
    block = (block << offset_tail);
    return new Long(block);
  }

  // Find and create fragments section
  // ---------------------------------
  public ArrayList<Long> convertToQueryIndexes(int startFragmentOrder, int endFragmentOrder){
    ArrayList<Long> queryIndexes = new ArrayList<Long>();

    return queryIndexes;
  }

  public ArrayList<Long> findCachedBits(ArrayList<Long> queryIndexes, int startQueryBlockOrder, int endQueryBlockOrder){
    ArrayList<Long> result = new ArrayList<Long>();
    for (int i = startQueryBlockOrder ; i <= endQueryBlockOrder; i++ ){
      result.add(new Long(
        queryIndexes.get(i - startQueryBlockOrder).longValue()
          ^ cacheIndexes.get(i).longValue()
        ));
    }
    return result;
  }

  public ArrayList<Long> buildFragmentBits(int startFragmentOrder, int endFragmentOrder) {
//    ArrayList<CacheFragment> fragments = new ArrayList<CacheFragment>();
    ArrayList<Long> queryIndexes = convertToQueryIndexes(startFragmentOrder, endFragmentOrder);
    int startQueryBlockOrder = calcBlockOrder(startFragmentOrder);
    int endQueryBlockOrder = calcBlockOrder(endFragmentOrder);
    ArrayList<Long> result;
    // If length of queryIndexes > length of cacheIndexes; XOR until the end of cacheIndexes, otherwise return all 1 bits;
    int numNotInCacheBlock = queryIndexes.size() + startQueryBlockOrder - cacheIndexes.size();
    if (numNotInCacheBlock > 0)
      result = findCachedBits(queryIndexes, startQueryBlockOrder, cacheIndexes.size() - 1);
    else
      result = findCachedBits(queryIndexes, startQueryBlockOrder, endQueryBlockOrder);
    // Fill with all 1 bits; means not in cache
    for(int i = 0 ; i < numNotInCacheBlock; i++){
      result.add(fulfillBlock());
    }
    return result;
//    // build body
//    fragments = buildFragmentsFromBits(result, startQueryBlockOrder, startFragmentOrder, endFragmentOrder);
//    // Todo: add head if exist
//    // Todo: add tail if exist
//    return fragments;
  }

}
