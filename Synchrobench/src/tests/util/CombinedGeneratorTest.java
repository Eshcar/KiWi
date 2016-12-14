package util;

import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by dbasin on 7/7/16.
 */
public class CombinedGeneratorTest {

    @Test
    public void next() throws Exception {
        List<Integer> prefixes = Arrays.asList(1500234, 50001, 289384);
        CombinedGenerator cg = new CombinedGenerator(prefixes);
        Set<Integer> genVals = new HashSet<>();

        while (cg.hasNext()) {
            int value = cg.next();
            Assert.assertFalse(genVals.contains(value));
            genVals.add(value);
        }

        Assert.assertEquals(genVals.size(), 30);

        Set<Integer> reducedVals = new HashSet<>();
        for (Integer v : genVals) {
            int rv = v.intValue() >> cg.getShift();
            reducedVals.add(rv);
        }

        Assert.assertEquals(reducedVals.size(), 3);
        Assert.assertTrue(reducedVals.contains(1500234));
        Assert.assertTrue(reducedVals.contains(50001));
        Assert.assertTrue(reducedVals.contains(289384));

    }

    @Test
    public void reset() throws Exception {
        List<Integer> prefixes = Arrays.asList(1500234, 50001, 289384);
        CombinedGenerator cg = new CombinedGenerator(prefixes);
        Set<Integer> genVals = new HashSet<>();

        while (cg.hasNext()) cg.next();

        cg.reset();

        while (cg.hasNext()) {
            int value = cg.next();
            Assert.assertFalse(genVals.contains(value));
            genVals.add(value);
        }

        Assert.assertEquals(genVals.size(), 30);

        Set<Integer> reducedVals = new HashSet<>();
        for (Integer v : genVals) {
            int rv = v.intValue() >> cg.getShift();
            reducedVals.add(rv);
        }

        Assert.assertEquals(reducedVals.size(), 3);

    }
}