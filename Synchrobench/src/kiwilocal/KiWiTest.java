package kiwilocal;

import contention.benchmark.Parameters;
import kiwilocal.Chunk;
import kiwilocal.KiWiMap;
import org.junit.Test;
import sun.misc.Unsafe;
import util.IntegerGenerator;

import java.lang.reflect.Constructor;

import static org.junit.Assert.*;

/**
 * Created by dbasin on 12/7/15.
 */
public class KiWiTest {
    private static final Unsafe unsafe;
    /** static constructor - access and create a new instance of Unsafe */
    static
    {
        try
        {
            Constructor<Unsafe> unsafeConstructor = Unsafe.class.getDeclaredConstructor();
            unsafeConstructor.setAccessible(true);
            unsafe = unsafeConstructor.newInstance();
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

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
    public void testZipfian() throws Exception{
        Parameters.distribution = Parameters.KeyDistribution.Zipfian;

        IntegerGenerator g = Parameters.distribution.createGenerator(0, 100000);

        //IntegerGenerator g = Parameters.KeyDistribution.Uniform.createGenerator(0, 10000);
        long sum = 0;
        double sumSq = 0;
        int count = 10000;
        for(int i = 0; i < count; ++i)
        {
            int next = g.nextInt();
            sum+= next;
            sumSq += (double)next*next;
        }

        double avg = (double)sum/count;
        long stdev = (long)Math.sqrt(sumSq/count - avg*avg);
        System.out.println("The exp of zipf: " + sum/count);
        System.out.println("The variance: " + stdev);

    }
    @Test
    public void testPut() throws Exception {

    }

    @Test
    public void testScan() throws Exception {
        Integer[] arr = new Integer[8];

        //kiwi.getRange(arr,)
    }

    @Test
    public void testUnsafe() throws Exception{
        int[] arr = new int[10];
        int y = 2;
        int x = 1;
        arr[2] = x;
        arr[3] = y;

        long l = (((long)y) << 32) | (x & 0xffffffffL);

        assertTrue(unsafe.compareAndSwapLong(arr,Unsafe.ARRAY_INT_BASE_OFFSET + (2) * Unsafe.ARRAY_INT_INDEX_SCALE, l, 5L<<32));

        assertTrue(arr[3]== 5);

        long val =  unsafe.getLong(arr,(long)Unsafe.ARRAY_INT_BASE_OFFSET + (2) * (long)Unsafe.ARRAY_INT_INDEX_SCALE);
        assertTrue(val == 5L<<32);

    }
}