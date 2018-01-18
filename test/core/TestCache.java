package net.opentsdb.core;

import net.opentsdb.utils.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DateTime.class })
public final class TestCache extends BaseTsdbTest {

    //private TsdbQuery query = null;

    @Before
    public void beforeLocal() throws Exception {
      //query = new TsdbQuery(tsdb);
    }

  @Test
  public void test_findStartRowSeq() {
//    tsdb.cache.findStartRowSeq();
  }

  @Test
  public void startTimeToFragmentOrder(){
    assertEquals(tsdb.cache.startTimeToFragmentOrder(3000), 0);
  }


}