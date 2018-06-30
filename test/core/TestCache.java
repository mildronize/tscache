package net.opentsdb.core;

import net.opentsdb.utils.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Arrays;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DateTime.class })
public final class TestCache extends BaseTsdbTest {



  @Test
  public void calcEndFOBlock() throws Exception {
    assertEquals(tsdb.cache.calcEndFOBlock(0, 2, 70,64), 63);
    assertEquals(tsdb.cache.calcEndFOBlock(1, 3, 80,64), 63);
    assertEquals(tsdb.cache.calcEndFOBlock(1, 2, 70,64), 6);
    assertEquals(tsdb.cache.calcEndFOBlock(2, 3, 80,64), 16);

  }

  @Test
  public void getBitBoolean() throws Exception {
    assertEquals(tsdb.cache.getBitBoolean(0b0100000000000000000000000000000000000000000000000000000000000000L, 2, 64), false);
    assertEquals(tsdb.cache.getBitBoolean(0b0100000000000000000000000000000000000000000000000000000000000000L, 1, 64), true);
    assertEquals(tsdb.cache.getBitBoolean(0b0100000000000000000000000000000000000000000000000000000000000000L, 0, 64), false);
    assertEquals(tsdb.cache.getBitBoolean(0b010L, 61, 64), false);
    assertEquals(tsdb.cache.getBitBoolean(0b010L, 62, 64), true);
    assertEquals(tsdb.cache.getBitBoolean(0b010L, 63, 64), false);

    assertEquals(tsdb.cache.getBitBoolean(0b0100000000L, 2, 10), false);
    assertEquals(tsdb.cache.getBitBoolean(0b0100000000L, 1, 10), true);
    assertEquals(tsdb.cache.getBitBoolean(0b0100000000L, 0, 10), false);
    assertEquals(tsdb.cache.getBitBoolean(0b010L, 7, 10), false);
    assertEquals(tsdb.cache.getBitBoolean(0b010L, 8, 10), true);
    assertEquals(tsdb.cache.getBitBoolean(0b010L, 9, 10), false);

  }

  @Test
  public void getBitBooleanWithError() {
    try {
      tsdb.cache.getBitBoolean(0b11111L, 0, 3);
      fail("Expected an Exception to be thrown");
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Number of bits is larger than maxNumBits!"));
    }

  }

  @Test
  public void startTimeToFragmentOrder(){
    /*
                       0 -> FO = 0
        0 001 - 7200 000 -> FO = 1
    7200 001 - 14400 000 -> FO = 2
     */

    Cache c = new Cache(tsdb, 2);
    assertEquals(1, c.startTimeToFragmentOrder(0));
    assertEquals(1, c.startTimeToFragmentOrder(3600000));
    assertEquals(1, c.startTimeToFragmentOrder(7199000));
    assertEquals(2, c.startTimeToFragmentOrder(7200000));
    assertEquals(2, c.startTimeToFragmentOrder(7200001));
    assertEquals(0, c.fragmentOrderToStartTime(0));
    assertEquals(7200000, c.fragmentOrderToStartTime(1));
    assertEquals(14400000, c.fragmentOrderToStartTime(2));
  }

  @Test
  public void endTimeToFragmentOrder(){
    /*
            0 -  7199 999 -> FO = 0
     7200 000 - 14399 999 -> FO = 1
    14400 000 - 21599 999 -> FO = 2
    21600 000 - 28799 999 -> FO = 3
     */

    Cache c = new Cache(tsdb, 2);
    assertEquals(0, c.endTimeToFragmentOrder(0));
    assertEquals(0, c.endTimeToFragmentOrder(3600000));
    assertEquals(0, c.endTimeToFragmentOrder(7199000));
    assertEquals(0, c.endTimeToFragmentOrder(7199999));
    assertEquals(1, c.endTimeToFragmentOrder(7200000));
    assertEquals(1, c.endTimeToFragmentOrder(14399000));
    assertEquals(1, c.endTimeToFragmentOrder(14399999));
    assertEquals(2, c.endTimeToFragmentOrder(14400000));
    assertEquals(2, c.endTimeToFragmentOrder(21599000));
    assertEquals(2, c.endTimeToFragmentOrder(21599999));
    assertEquals(3, c.endTimeToFragmentOrder(21600000));
    assertEquals(3, c.endTimeToFragmentOrder(28799999));

    Cache c256 = new Cache(tsdb, 256);
    assertEquals(344, c256.endTimeToFragmentOrder(317675657000L));
    assertEquals(345, c256.endTimeToFragmentOrder(318675657000L));

  }

  @Test
  public void fragmentOrderToEndTime(){
    Cache c = new Cache(tsdb, 2);
    assertEquals(7199999, c.fragmentOrderToEndTime(0));
    assertEquals(14399999, c.fragmentOrderToEndTime(1));
    assertEquals(21599999, c.fragmentOrderToEndTime(2));
    assertEquals(28799999, c.fragmentOrderToEndTime(3));
  }

//  @Test
//  public void buildFragmentsFromBits_start_fo_one() throws Exception{
//    Cache c = tsdb.cache;
//    ArrayList<Long> results = new ArrayList<Long>();
//    // 0 or false means in cache;
//    results.add(new Long(0b0111111111111111111111111111111111111111111111111111111111111111L));
//    results.add(new Long(0b1111111111111111111111111111111111111111111111111111111111111111L));
//    results.add(new Long(0b1111111111111111111111111111111111111111111111111111111111111111L));
//    results.add(new Long(0b1111111111111111111111111111111111111111111111111111111111111111L));
//    results.add(new Long(0b1111111111111111111111111111111111111111111111111111111111111111L));
//    results.add(new Long(0b1111111111111111111111111111111111111111111111111111111111111111L));
//    results.add(new Long(0b1111111111111111111111101000000000000000000000000000000000000000L));
//    // end                                             e
//    ArrayList<CacheFragment> expected = new ArrayList<CacheFragment>();
//    expected.add(new CacheFragment(c.fragmentOrderToStartTime(0),c.fragmentOrderToEndTime(53), true));
//    expected.add(new CacheFragment(c.fragmentOrderToStartTime(54),c.fragmentOrderToEndTime(63), false));
//
//    ArrayList<CacheFragment> actual = c.buildCacheFragmentsFromBits(results, 1, 343,64);
//    assertEquals(expected.size(), actual.size());
//
//    for(int i =0;i< expected.size();i++){
//      System.out.println(i);
//      assertTrue(expected.get(i).compareTo(actual.get(i)));
//    }
//  }


  @Test
  public void buildFragmentsFromBits_head_body_tail() throws Exception{
    Cache c = tsdb.cache;
    ArrayList<Long> results = new ArrayList<Long>();
    // 0 or false means in cache;
    // start                    s
    results.add(new Long(0b0000000000000000000000000000000000000000000000000000001111111111L));
    results.add(new Long(0b0000000011111111111111111110000000000000000000000000000000000000L));
    results.add(new Long(0b0000000011111111111111111110000000000000000000000000000000000000L));
    // end                               e
    ArrayList<CacheFragment> expected = new ArrayList<CacheFragment>();
    expected.add(new CacheFragment(c.fragmentOrderToStartTime(0),c.fragmentOrderToEndTime(53), true));
    expected.add(new CacheFragment(c.fragmentOrderToStartTime(54),c.fragmentOrderToEndTime(63), false));

    expected.add(new CacheFragment(c.fragmentOrderToStartTime(64),c.fragmentOrderToEndTime(71), true));
    expected.add(new CacheFragment(c.fragmentOrderToStartTime(72),c.fragmentOrderToEndTime(90), false));
    expected.add(new CacheFragment(c.fragmentOrderToStartTime(91),c.fragmentOrderToEndTime(135), true));

    expected.add(new CacheFragment(c.fragmentOrderToStartTime(136),c.fragmentOrderToEndTime(137), false));

    ArrayList<CacheFragment> actual = c.buildCacheFragmentsFromBits(results, 0, 137,64);
    assertEquals(expected.size(), actual.size());

    for(int i =0;i< expected.size();i++){
      System.out.println(i);
      assertTrue(expected.get(i).compareTo(actual.get(i)));
    }
  }

  @Test
  public void buildFragmentsFromBits_head_body_tail_2() throws Exception{
    Cache c = tsdb.cache;
    ArrayList<Long> results = new ArrayList<Long>();
    // 0 or false means in cache;
    // start                    s
    results.add(new Long(0b0000000000000000000000000000000000000000000000000000001111111111L));
    // end                                                                                   e
    ArrayList<CacheFragment> expected = new ArrayList<CacheFragment>();
    expected.add(new CacheFragment(c.fragmentOrderToStartTime(0),c.fragmentOrderToEndTime(53), true));
    expected.add(new CacheFragment(c.fragmentOrderToStartTime(54),c.fragmentOrderToEndTime(60), false));

    ArrayList<CacheFragment> actual = c.buildCacheFragmentsFromBits(results, 0, 60,64);
    assertEquals(expected.size(), actual.size());

    for(int i =0;i< expected.size();i++){
      System.out.println(i);
      assertTrue(expected.get(i).compareTo(actual.get(i)));
    }
  }

  @Test
  public void buildFragmentsFromBits_start_w_zero() throws Exception{
    Cache c = tsdb.cache;
    ArrayList<Long> results = new ArrayList<Long>();
    // 0 or false means in cache;
    // start                    s
    results.add(new Long(0b0000000000000000000000000000000000000000000000000000001111111111L));
    // end                                                                                      e
    ArrayList<CacheFragment> expected = new ArrayList<CacheFragment>();
    expected.add(new CacheFragment(c.fragmentOrderToStartTime(0),c.fragmentOrderToEndTime(53), true));
    expected.add(new CacheFragment(c.fragmentOrderToStartTime(54),c.fragmentOrderToEndTime(63), false));

    ArrayList<CacheFragment> actual = c.buildCacheFragmentsFromBits(results, 0, 63,64);
    assertEquals(expected.size(), actual.size());

    for(int i =0;i< expected.size();i++){
      System.out.println(i);
      assertTrue(expected.get(i).compareTo(actual.get(i)));
    }
  }

  @Test
  public void buildFragmentsFromBits_start_wo_zero() throws Exception{
    Cache c = tsdb.cache;
    ArrayList<Long> results = new ArrayList<Long>();
    // 0 or false means in cache;
    // start                         s
    results.add(new Long(0b0000000000000000000000000000000000000000000000000000001111111111L));
    // end                                                                                      e
    ArrayList<CacheFragment> expected = new ArrayList<CacheFragment>();
    expected.add(new CacheFragment(c.fragmentOrderToStartTime(4),c.fragmentOrderToEndTime(53), true));
    expected.add(new CacheFragment(c.fragmentOrderToStartTime(54),c.fragmentOrderToEndTime(63), false));

    ArrayList<CacheFragment> actual = c.buildCacheFragmentsFromBits(results, 4, 63,64);
    assertEquals(expected.size(), actual.size());

    for(int i =0;i< expected.size();i++){
      System.out.println(i);
      assertTrue(expected.get(i).compareTo(actual.get(i)));
    }
  }

//  @Test
//  public void bytesRangeToSpan(){
//    byte[] raw_data = {0, 0, 0, 25, 13, 0, 0, 1, 86, -123, 109, 32, 0, 0, 1, 0, 0, 1, 0, 4, 0, 0, 112, -128, 0, 3, 31, 30, 0, 0, 0, 0, 25, 13, 0, 0, 1, 86, -123, 123, 48, 0, 0, 1, 0, 0, 1, 0, 4, 0, 0, 112, -128, 0, 3, 39, 31, 0};
//    try{
//      Span span = tsdb.cache.bytesRangeToSpan(raw_data, 0);
//    }catch(Exception e){
//      e.printStackTrace();
//    }
//    assertEquals(0,0);
//  }

  @Test
  public void getNumberBytesRange_start_4_num_1() throws Exception{
    byte[] bytes = {0, 0, 0, 25, 13, 0, 0, 1, 86, -123, 109, 32, 0, 0, 1, 0, 0, 1, 0, 4, 0, 0, 112, -128, 0, 3, 31, 30, 0, 0, 0, 0, 25, 13, 0, 0, 1, 86, -123, 123, 48, 0, 0, 1, 0, 0, 1, 0, 4, 0, 0, 112, -128, 0, 3, 39, 31, 0};
    long keyLength = tsdb.cache.getNumberBytesRange(bytes, 4, Cache.ROWSEQ_KEY_NUMBYTES);
    assertEquals(13, keyLength);
  }

  @Test
  public void getNumberBytesRange_start_0_num_4() throws Exception{
    byte[] bytes = {0, 0, 0, 25, 13, 0, 0, 1, 86, -123, 109, 32, 0, 0, 1, 0, 0, 1, 0, 4, 0, 0, 112, -128, 0, 3, 31, 30, 0, 0, 0, 0, 25, 13, 0, 0, 1, 86, -123, 123, 48, 0, 0, 1, 0, 0, 1, 0, 4, 0, 0, 112, -128, 0, 3, 39, 31, 0};
    long keyLength = tsdb.cache.getNumberBytesRange(bytes, 0, Cache.ROWSEQ_LENGTH_NUMBYTES);
    assertEquals(25, keyLength);
  }

  @Test
  public void getNumberBytesRange_start_53_num_2() throws Exception{
    byte[] bytes = {0, 0, 0, 25, 13, 0, 0, 1, 86, -123, 109, 32, 0, 0, 1, 0, 0, 1, 0, 4, 0, 0, 112, -128, 0, 3, 31, 30, 0, 0, 0, 0, 25, 13, 0, 0, 1, 86, -123, 123, 48, 0, 0, 1, 0, 0, 1, 0, 4, 0, 0, 112, -128, 0, 3, 39, 31, 0};
    long num = tsdb.cache.getNumberBytesRange(bytes, 53, Cache.ROWSEQ_QUALIFIER_NUMBYTES);
    assertEquals(3, num);
  }


}