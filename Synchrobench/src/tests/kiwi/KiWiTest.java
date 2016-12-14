package kiwi;

import org.junit.Test;

import static org.junit.Assert.*;
import contention.benchmark.Parameters;
import util.IntegerGenerator;

/**
 * Created by dbasin on 12/7/15.
 */
public class KiWiTest {

    KiWiMap kiwi;
    private void reset()
    {
        Chunk.MAX_ITEMS = 4;
        kiwi = new KiWiMap();

        for(int j = 0; j < 10; ++j) {
            for (int i = 0; i < 3; ++i) {
                kiwi.put(j, i);
            }
        }

    }

    @Test
    public void testGet() throws Exception {
        reset();
        assertTrue(kiwi.get(Integer.valueOf(1)).equals(2));

    }

    @Test
    public void testDistribution() throws Exception {
        IntegerGenerator distr = Parameters.KeyDistribution.Churn10Perc.createGenerator(0,2000000);
        long sum = 0;
        double sqSum = 0;
        int size = 10000;
        for(int i = 0; i < size; ++i)
        {
            int val = distr.nextInt();
            sum += val;
            sqSum += (long)val*val;
        }

        long avg = sum/size;
        long stdev = (long)Math.sqrt((sqSum/size) - avg*avg);

        System.out.println("Avg: " + avg);
        System.out.println("Stdev: " + stdev);


    }

    @Test
    public void testScan() throws Exception {
        Integer[] arr = new Integer[8];

        //kiwi.getRange(arr,)
    }
}