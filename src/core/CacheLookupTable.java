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

  /* Mark bit 1 from start FO until numFragment */
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

  public Long headPartialMarkedBlock(int numLeadingZero){
//    Long block = new Long(0b0000000000000000000000000000000000000000000000000000000000000000L);
    long block = 0;
    int number = indexSize - numLeadingZero;
    for (int i=0;i < number  ;i++) {
      block = (block << 1) | 1;
    }
    return new Long(block);
  }


  public Long tailPartialMarkedBlock(int numTailingZero, int numLeadingZero){
    long block = 0;
    int number = indexSize - numTailingZero - numLeadingZero;
    for (int i=0;i < numLeadingZero  ;i++) {
      block = (block << 1);
    }
    for (int i=0;i < number  ;i++) {
      block = (block << 1) | 1;
    }
    // append zero
    block = (block << numTailingZero);
    return new Long(block);
  }

  // Find and create fragments section
  // Act like `mark` function
  // ---------------------------------
  public ArrayList<Long> convertToQueryIndexes(int startFragmentOrder, int endFragmentOrder){
    ArrayList<Long> queryIndexes = new ArrayList<Long>();
    int numFragment = endFragmentOrder - startFragmentOrder + 1;
    int endBlockOrder = calcBlockOrder(endFragmentOrder);
    int startBlockOrder = calcBlockOrder(startFragmentOrder);
    //append empty block
    for(int i = 0 ; i < startBlockOrder; i++){
      queryIndexes.add(emptyBlock());
    }
    // find head patial bit 1
    int offset_head = startFragmentOrder % indexSize;
    if(offset_head + numFragment > indexSize)
      queryIndexes.add(headPartialMarkedBlock(offset_head).longValue());
    //append body all bit 1
    for (int i = queryIndexes.size() ; i < endBlockOrder; i++){
      queryIndexes.add(fulfillBlock());
    }
    // find tail patial bit 1
    if (startBlockOrder != endBlockOrder){
      // One block incoming
      offset_head = 0;
    }
    int numberMarkedBits = calcNumberMarkedBit(startFragmentOrder, numFragment);
    int offset_tail = indexSize - numberMarkedBits - offset_head;
    if(numberMarkedBits != 0)
      queryIndexes.add(tailPartialMarkedBlock(offset_tail, offset_head).longValue());
    return queryIndexes;
  }

  public String printIndexes(ArrayList<Long> indexes){
    final StringBuilder buf = new StringBuilder(indexes.size() * (1 + 64));
    for(final Long index : indexes){
      buf.append(String.format("%64s", Long.toBinaryString(index)).replace(' ', '0'));
      buf.append(" ");
    }
    return buf.toString();
  }

  public ArrayList<Long> findCachedBits(ArrayList<Long> queryIndexes, int startBlockOrder, int endBlockOrder){
    ArrayList<Long> result = new ArrayList<Long>();
    LOG.debug("QueryIndexes: " + printIndexes(queryIndexes));
    LOG.debug("CacheIndexes: " + printIndexes(cacheIndexes));
    for (int i = startBlockOrder ; i <= endBlockOrder; i++ ){
      LOG.debug("QueryBlockOrder: " + i);
      result.add(new Long(
        queryIndexes.get(i - startBlockOrder).longValue()
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
    // If length of queryIndexes > length of cacheIndexes; XOR until the end of cacheIndexes, after that fill all 1 bits;
    int numNotInCacheBlock = queryIndexes.size() + startQueryBlockOrder - cacheIndexes.size();
    if (numNotInCacheBlock > 0)
      result = findCachedBits(queryIndexes, startQueryBlockOrder, cacheIndexes.size() - 1);
    else
      result = findCachedBits(queryIndexes, startQueryBlockOrder, endQueryBlockOrder);
    // add tail if exist
    // Fill with all 1 bits; means not in cache
    for(int i = 0 ; i < numNotInCacheBlock; i++){
      result.add(fulfillBlock());
    }
    //    Note: No head adding

    return result;

  }

}
