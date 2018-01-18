package net.opentsdb.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class CacheLookupTable {

  private static final Logger LOG = LoggerFactory.getLogger(CacheLookupTable.class);

  // May be byte data structure for looking fast
  private ArrayList<Long> cacheIndexes= null;

  // Number of bit per cacheIndex, Maximum: 64 (Size of long)
  private final int indexSize = 64;

  // Fragment Order = Ceil(Ti/range size)
  private int startFragmentOrder;

  private int blockOrder;

  public CacheLookupTable(){
    cacheIndexes = new ArrayList<Long>();
    cacheIndexes.add(emptyBlock());
    startFragmentOrder = 0;
    blockOrder = 0;
  }

  public boolean isEmpty(){
    if ( cacheIndexes == null ) return true;
    if ( cacheIndexes.size() == 0) return true;
    return false;
  }

  public int findOrder(int fragmentOrder){
    return fragmentOrder % indexSize;
  }

  public void mark(int start_fragmentOrder, int numFragment){
    // bit 1 in cache
    // bit 0 not in cache

    //    0       1          2          3          4
    // 0 - 63, 64 - 127, 128 - 191 , 192 - 255, 256 - 319
    // 300 = 100 + 200
    int end_fragmentOrder = start_fragmentOrder + numFragment;
    // 4 = 300%64
    int incomingEndBlockOrder = (int)(end_fragmentOrder / indexSize);
    // 1 = 100 % 64
    int incomingBlockOrder = (int)(start_fragmentOrder / indexSize);
    // Insert empty blocks before and after existing cacheIndexes
    // insert empty blocks before cacheIndexs
    for (int i = 0; i < blockOrder - incomingBlockOrder; i++){
      cacheIndexes.add(0, emptyBlock());
      blockOrder--;
    }
    // Append empty blocks of cacheIndexs
    for (int i = 0; i < incomingEndBlockOrder - blockOrder + cacheIndexes.size(); i++){
      cacheIndexes.add(emptyBlock());
    }
    // Mark All 1 value of each bit into target position
    // 1. Make head block ( not fullfill)
    // 36 = 100 % 64
    int offset = start_fragmentOrder % indexSize;

    // 2. Make body block with all 1 value
    // 3. Make tail block ( not fullfill)


  }

  private Long emptyBlock(){
    return new Long(0b0000000000000000000000000000000000000000000000000000000000000000L);
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

  private Long tailPartialMarkedBlock(int offset){
    long block = 0;
    int number = indexSize - offset;
    for (int i=0;i < number  ;i++) {
      block = (block << 1) | 1;
    }
    // append zero
    block = (block << offset);
    return new Long(block);
  }

}
