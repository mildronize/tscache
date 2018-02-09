package net.opentsdb.core;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
public class TestCacheLookupTable {

  private CacheLookupTable lookupTable = null;
  private int rangeSize = 64;

  @Before
  public void setUp() throws Exception {
    lookupTable = new CacheLookupTable(64);
  }

  @Test
  public void calcNumberEmptyBlock() {
    assertEquals(1,  lookupTable.calcNumberEmptyBlock(0, 0));
    assertEquals(2,  lookupTable.calcNumberEmptyBlock(0, 1));
    assertEquals(0,  lookupTable.calcNumberEmptyBlock(1, 0));
    assertEquals(0,  lookupTable.calcNumberEmptyBlock(2, 0));
    assertEquals(1,  lookupTable.calcNumberEmptyBlock(1, 1));
    assertEquals(1,  lookupTable.calcNumberEmptyBlock(2, 2));
  }

  @Test
  public void calcNumberMarkedBit() {
    assertEquals(1, lookupTable.calcNumberMarkedBit(0, 1));
    assertEquals(64, lookupTable.calcNumberMarkedBit(0, 64));
    assertEquals(1, lookupTable.calcNumberMarkedBit(0, 65));
    assertEquals(64, lookupTable.calcNumberMarkedBit(0, 128));

    assertEquals(1, lookupTable.calcNumberMarkedBit(1, 1));
    assertEquals(63, lookupTable.calcNumberMarkedBit(1, 63));
    assertEquals(1, lookupTable.calcNumberMarkedBit(1, 64));
    assertEquals(64, lookupTable.calcNumberMarkedBit(1, 127));
    assertEquals(1, lookupTable.calcNumberMarkedBit(1, 128));

    assertEquals(2, lookupTable.calcNumberMarkedBit(64, 2));
    assertEquals(64, lookupTable.calcNumberMarkedBit(64, 64));
    assertEquals(1, lookupTable.calcNumberMarkedBit(65, 64));
    assertEquals(2, lookupTable.calcNumberMarkedBit(65, 2));
    assertEquals(8, lookupTable.calcNumberMarkedBit(66, 70));
    assertEquals(1, lookupTable.calcNumberMarkedBit(127, 1));
  }


  @Test
  public void tailPartialMarkedBlock() {
    assertEquals(new Long(0b0011111111111111111111111111111111111111111111111111111111111100L),  lookupTable.tailPartialMarkedBlock(2, 2));
    assertEquals(new Long(0b1111111111111111111111111111111111111111111111111111111111111100L),  lookupTable.tailPartialMarkedBlock(2, 0));
    assertEquals(new Long(0b0011111111111111111111111111111111111111111111111111111111111111L),  lookupTable.tailPartialMarkedBlock(0, 2));
    assertEquals(new Long(0b1000000000000000000000000000000000000000000000000000000000000000L),  lookupTable.tailPartialMarkedBlock(63, 0));
    assertEquals(new Long(0b0100000000000000000000000000000000000000000000000000000000000000L),  lookupTable.tailPartialMarkedBlock(62, 1));
  }

  @Test
  public void headPartialMarkedBlock() {
    assertEquals(new Long(0b0011111111111111111111111111111111111111111111111111111111111111L), lookupTable.headPartialMarkedBlock(2));
    assertEquals(new Long(0b1111111111111111111111111111111111111111111111111111111111111111L), lookupTable.headPartialMarkedBlock(0));
    assertEquals(new Long(0b0000000000000000000000000000000000000000000000000000000000000000L), lookupTable.headPartialMarkedBlock(64));
  }

  @Test
  public void mark_startFO_0_1() {
    int start_fo = 0;
    int numFragment = 1;
    lookupTable.mark(start_fo, numFragment);
    assertEquals(1, lookupTable.calcNumberMarkedBit(start_fo, numFragment));
    assertEquals(1,  lookupTable.getCacheIndexes().size());
    assertEquals(0b1000000000000000000000000000000000000000000000000000000000000000L, lookupTable.getCacheIndexes().get(0).longValue());
  }

  @Test
  public void mark_startFO_0_2() {
    int start_fo = 0;
    int numFragment = 63;
    lookupTable.mark(start_fo, numFragment);
    assertEquals(63, lookupTable.calcNumberMarkedBit(start_fo, numFragment));
    assertEquals(1,  lookupTable.getCacheIndexes().size());
    assertEquals(0b1111111111111111111111111111111111111111111111111111111111111110L, lookupTable.getCacheIndexes().get(0).longValue());
}
  @Test
  public void mark_startFO_0_3() {
    int start_fo = 0;
    int numFragment = 64;
    lookupTable.mark(start_fo, numFragment);
    assertEquals(64, lookupTable.calcNumberMarkedBit(start_fo, numFragment));
    assertEquals(1,  lookupTable.getCacheIndexes().size());
    assertEquals(0b1111111111111111111111111111111111111111111111111111111111111111L, lookupTable.getCacheIndexes().get(0).longValue());
  }
  @Test
  public void mark_startFO_0_4() {
    int start_fo = 0;
    int numFragment = 65;
    lookupTable.mark(start_fo, numFragment);
    assertEquals(1, lookupTable.calcNumberMarkedBit(start_fo, numFragment));
    assertEquals(2,  lookupTable.getCacheIndexes().size());
    assertEquals(0b1111111111111111111111111111111111111111111111111111111111111111L, lookupTable.getCacheIndexes().get(0).longValue());
    assertEquals(0b1000000000000000000000000000000000000000000000000000000000000000L, lookupTable.getCacheIndexes().get(1).longValue());
  }

  @Test
  public void mark_startFO_0_5() {
    int start_fo = 0;
    int numFragment = 127;
    lookupTable.mark(start_fo, numFragment);
    assertEquals(63, lookupTable.calcNumberMarkedBit(start_fo, numFragment));
    assertEquals(2,  lookupTable.getCacheIndexes().size());
    assertEquals(0b1111111111111111111111111111111111111111111111111111111111111111L, lookupTable.getCacheIndexes().get(0).longValue());
    assertEquals(0b1111111111111111111111111111111111111111111111111111111111111110L, lookupTable.getCacheIndexes().get(1).longValue());
  }

  @Test
  public void mark_startFO_0_6() {
//    lookupTable.mark(0, 128);
    int start_fo = 0;
    int numFragment = 128;
    lookupTable.mark(start_fo, numFragment);
    assertEquals(64, lookupTable.calcNumberMarkedBit(start_fo, numFragment));
    assertEquals(2,  lookupTable.getCacheIndexes().size());
    assertEquals(0b1111111111111111111111111111111111111111111111111111111111111111L, lookupTable.getCacheIndexes().get(0).longValue());
    assertEquals(0b1111111111111111111111111111111111111111111111111111111111111111L, lookupTable.getCacheIndexes().get(1).longValue());
  }

  @Test
  public void mark_startFO_0_7() {
    int start_fo = 0;
    int numFragment = 129;
    lookupTable.mark(start_fo, numFragment);
    assertEquals(1, lookupTable.calcNumberMarkedBit(start_fo, numFragment));
    assertEquals(3,  lookupTable.getCacheIndexes().size());
    assertEquals(0b1111111111111111111111111111111111111111111111111111111111111111L, lookupTable.getCacheIndexes().get(0).longValue());
    assertEquals(0b1111111111111111111111111111111111111111111111111111111111111111L, lookupTable.getCacheIndexes().get(1).longValue());
    assertEquals(0b1000000000000000000000000000000000000000000000000000000000000000L, lookupTable.getCacheIndexes().get(2).longValue());
  }

  @Test
  public void mark_8() {
    int start_fo = 1;
    int numFragment = 1;
    lookupTable.mark(start_fo, numFragment);
    assertEquals(1, lookupTable.calcNumberMarkedBit(start_fo, numFragment));
    assertEquals(1,  lookupTable.getCacheIndexes().size());
    assertEquals(0b0100000000000000000000000000000000000000000000000000000000000000L, lookupTable.getCacheIndexes().get(0).longValue());
  }

  @Test
  public void mark_9() {
    int start_fo = 1;
    int numFragment = 66;
    lookupTable.mark(start_fo, numFragment);
    assertEquals(3, lookupTable.calcNumberMarkedBit(start_fo, numFragment));
    assertEquals(2,  lookupTable.getCacheIndexes().size());
    assertEquals(0b0111111111111111111111111111111111111111111111111111111111111111L, lookupTable.getCacheIndexes().get(0).longValue());
    assertEquals(0b1110000000000000000000000000000000000000000000000000000000000000L, lookupTable.getCacheIndexes().get(1).longValue());

  }

  @Test
  public void mark_10() {
    int start_fo = 63;
    int numFragment = 1;
    lookupTable.mark(start_fo, numFragment);
    assertEquals(1, lookupTable.calcNumberMarkedBit(start_fo, numFragment));
    assertEquals(1,  lookupTable.getCacheIndexes().size());
    assertEquals(0b0000000000000000000000000000000000000000000000000000000000000001L, lookupTable.getCacheIndexes().get(0).longValue());
  }
  @Test
  public void mark_11() {
    int start_fo = 64;
    int numFragment = 1;
    lookupTable.mark(start_fo, numFragment);
    assertEquals(1, lookupTable.calcNumberMarkedBit(start_fo, numFragment));
    assertEquals(2,  lookupTable.getCacheIndexes().size());
    assertEquals(0b1000000000000000000000000000000000000000000000000000000000000000L, lookupTable.getCacheIndexes().get(1).longValue());
  }
  @Test
  public void mark_12() {
    int start_fo = 127;
    int numFragment = 1;
    lookupTable.mark(start_fo, numFragment);
    assertEquals(1, lookupTable.calcNumberMarkedBit(start_fo, numFragment));
    assertEquals(2,  lookupTable.getCacheIndexes().size());
    assertEquals(0b0000000000000000000000000000000000000000000000000000000000000001L, lookupTable.getCacheIndexes().get(1).longValue());
  }

  @Test
  public void mark_13() {
    int start_fo = 128;
    int numFragment = 1;
    lookupTable.mark(start_fo, numFragment);
    assertEquals(1, lookupTable.calcNumberMarkedBit(start_fo, numFragment));
    assertEquals(3,  lookupTable.getCacheIndexes().size());
    assertEquals(0b1000000000000000000000000000000000000000000000000000000000000000L, lookupTable.getCacheIndexes().get(2).longValue());
  }

  // next
  @Test
  public void mark_14() {
    ArrayList<Long> cachesIndexes = new ArrayList<Long>();
    cachesIndexes.add(lookupTable.emptyBlock());
    cachesIndexes.add(lookupTable.emptyBlock());
    cachesIndexes.add(lookupTable.emptyBlock());
    lookupTable.setCacheIndexes(cachesIndexes);
    int start_fo = 65;
    int numFragment = 2;
    lookupTable.mark(start_fo, numFragment);
    assertEquals(2, lookupTable.calcNumberMarkedBit(start_fo, numFragment));
    assertEquals(3,  lookupTable.getCacheIndexes().size());
    assertEquals(0b0110000000000000000000000000000000000000000000000000000000000000L, lookupTable.getCacheIndexes().get(1).longValue());
  }

  @Test
  public void mark_16() {
    ArrayList<Long> cachesIndexes = new ArrayList<Long>();
    cachesIndexes.add(lookupTable.emptyBlock());
    cachesIndexes.add(lookupTable.emptyBlock());
    cachesIndexes.add(lookupTable.emptyBlock());
    lookupTable.setCacheIndexes(cachesIndexes);
    int start_fo = 65;
    int numFragment = 64;
    lookupTable.mark(start_fo, numFragment);
    assertEquals(1, lookupTable.calcNumberMarkedBit(start_fo, numFragment));
    assertEquals(3,  lookupTable.getCacheIndexes().size());
    assertEquals(0b0111111111111111111111111111111111111111111111111111111111111111L, lookupTable.getCacheIndexes().get(1).longValue());
    assertEquals(0b1000000000000000000000000000000000000000000000000000000000000000L, lookupTable.getCacheIndexes().get(2).longValue());

  }

  @Test
  public void mark_15() {
    ArrayList<Long> cachesIndexes = new ArrayList<Long>();
    cachesIndexes.add(lookupTable.emptyBlock());
    cachesIndexes.add(lookupTable.emptyBlock());
    cachesIndexes.add(lookupTable.emptyBlock());
    lookupTable.setCacheIndexes(cachesIndexes);
    int start_fo = 66;
    int numFragment = 70;
    lookupTable.mark(start_fo, numFragment);
    assertEquals(8, lookupTable.calcNumberMarkedBit(start_fo, numFragment));
    assertEquals(3,  lookupTable.getCacheIndexes().size());
    assertEquals(0b0011111111111111111111111111111111111111111111111111111111111111L, lookupTable.getCacheIndexes().get(1).longValue());
    assertEquals(0b1111111100000000000000000000000000000000000000000000000000000000L, lookupTable.getCacheIndexes().get(2).longValue());
  }
  @Test
  public void convertToQueryIndexes_1_min(){
    ArrayList<Long> actual = lookupTable.convertToQueryIndexes(0, 0);
    assertEquals(1, actual.size());
    assertEquals(0b1000000000000000000000000000000000000000000000000000000000000000L, actual.get(0).longValue());
  }

  @Test
  public void convertToQueryIndexes_1(){
    ArrayList<Long> actual = lookupTable.convertToQueryIndexes(0, 3);
    assertEquals(1, actual.size());
    assertEquals(0b1111000000000000000000000000000000000000000000000000000000000000L, actual.get(0).longValue());
  }
  @Test
  public void convertToQueryIndexes_1_full(){
    ArrayList<Long> actual = lookupTable.convertToQueryIndexes(0, 63);
    assertEquals(1, actual.size());
    assertEquals(0b1111111111111111111111111111111111111111111111111111111111111111L, actual.get(0).longValue());
  }

  @Test
  public void convertToQueryIndexes_2(){
    ArrayList<Long> actual = lookupTable.convertToQueryIndexes(0, 64);
    assertEquals(2, actual.size());
    assertEquals(0b1111111111111111111111111111111111111111111111111111111111111111L, actual.get(0).longValue());
    assertEquals(0b1000000000000000000000000000000000000000000000000000000000000000L, actual.get(1).longValue());
  }

  @Test
  public void convertToQueryIndexes_2_full(){
    ArrayList<Long> actual = lookupTable.convertToQueryIndexes(0, 127);
    assertEquals(2, actual.size());
    assertEquals(0b1111111111111111111111111111111111111111111111111111111111111111L, actual.get(0).longValue());
    assertEquals(0b1111111111111111111111111111111111111111111111111111111111111111L, actual.get(1).longValue());
  }

  @Test
  public void convertToQueryIndexes_3(){
    ArrayList<Long> actual = lookupTable.convertToQueryIndexes(0, 128);
    assertEquals(3, actual.size());
    assertEquals(0b1111111111111111111111111111111111111111111111111111111111111111L, actual.get(0).longValue());
    assertEquals(0b1111111111111111111111111111111111111111111111111111111111111111L, actual.get(1).longValue());
    assertEquals(0b1000000000000000000000000000000000000000000000000000000000000000L, actual.get(2).longValue());
  }

  @Test
  public void convertToQueryIndexes_lead_zero(){
    ArrayList<Long> actual = lookupTable.convertToQueryIndexes(1, 63);
    assertEquals(1, actual.size());
    assertEquals(0b0111111111111111111111111111111111111111111111111111111111111111L, actual.get(0).longValue());
  }

  @Test
  public void convertToQueryIndexes_lead_and_tail_zero_1(){
    ArrayList<Long> actual = lookupTable.convertToQueryIndexes(1, 3);
    assertEquals(1, actual.size());
    assertEquals(0b0111000000000000000000000000000000000000000000000000000000000000L, actual.get(0).longValue());
  }

  @Test
  public void convertToQueryIndexes_lead_and_tail_zero_2(){
    ArrayList<Long> actual = lookupTable.convertToQueryIndexes(1, 126);
    assertEquals(2, actual.size());
    assertEquals(0b0111111111111111111111111111111111111111111111111111111111111111L, actual.get(0).longValue());
    assertEquals(0b1111111111111111111111111111111111111111111111111111111111111110L, actual.get(1).longValue());
  }


}
