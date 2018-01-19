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

  public int calcNumberEmptyBlock(int cacheIndexesSize, int incomingEndBlockOrder){
    int numberEmptyBlock = 0;
    if(cacheIndexesSize <= incomingEndBlockOrder){
      numberEmptyBlock = incomingEndBlockOrder - cacheIndexesSize + 1;
    }
    return numberEmptyBlock;
  }

  public int calcNumberMarkedBit(int start_fragmentOrder, int numFragment){
    int end_fragmentOrder = start_fragmentOrder + numFragment - 1;
    int incomingEndBlockOrder = (int)(end_fragmentOrder / indexSize);
    int incomingBlockOrder = (int)(start_fragmentOrder / indexSize);

    if (incomingBlockOrder == incomingEndBlockOrder){
      return numFragment;
    }else {
      return end_fragmentOrder % indexSize + 1;
    }
  }

  public void mark(int start_fragmentOrder, int numFragment) throws IndexOutOfBoundsException {
    int end_fragmentOrder = start_fragmentOrder + numFragment - 1;
    int incomingEndBlockOrder = (int)(end_fragmentOrder / indexSize);
    int incomingBlockOrder = (int)(start_fragmentOrder / indexSize);

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

}
