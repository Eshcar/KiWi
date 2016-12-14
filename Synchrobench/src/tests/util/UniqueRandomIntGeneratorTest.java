package util;

import junit.framework.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by dbasin on 7/7/16.
 */
public class UniqueRandomIntGeneratorTest {


    @Test
    public void hasNext() throws Exception {
        UniqueRandomIntGenerator ug = new UniqueRandomIntGenerator(0,10);
        Set<Integer> values = new HashSet<>();

        while(ug.hasNext())
        {
            Integer nextVal = ug.next();

            Assert.assertFalse(values.contains(nextVal));

            values.add(nextVal);
        }

        Assert.assertEquals(values.size(),10);
        Assert.assertTrue(values.contains(0));
    }

    @Test
    public void reset() throws Exception {
        UniqueRandomIntGenerator ug = new UniqueRandomIntGenerator(0,10);
        Set<Integer> values = new HashSet<>();

        while(ug.hasNext()) ug.next();

        ug.reset();

        while(ug.hasNext())
        {
            Integer nextVal = ug.next();

            Assert.assertFalse(values.contains(nextVal));

            values.add(nextVal);
        }

        Assert.assertEquals(values.size(),10);
        Assert.assertTrue(values.contains(0));

    }
}