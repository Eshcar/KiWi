package kiwi;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by dbasin on 12/2/15.
 */

public class ItemsIteratorsTests {

    ArrayList<Chunk<Integer,Integer>> chunks;
    private static final int NUM_OF_CHUNKS = 10;
    private int maxKey = 0;


    public void fillChunk(Chunk<Integer, Integer> c, int numOfItems)
    {

        for(int i = 0; i < numOfItems; ++i)
        {
            //c.allocate(maxKey, maxKey);
            c.appendItem(maxKey,maxKey,maxKey);
            maxKey++;
        }
    }

    public void resetState(int numOfItems, int numOfChunks)
    {
        chunks = new ArrayList<>(numOfChunks);
        Chunk<Integer, Integer> prev = new ChunkInt(0,null);
        fillChunk(prev,numOfItems);

        chunks.add(prev);

        for(int i = 1; i < numOfChunks; ++i)
        {
            Chunk<Integer, Integer> c = new ChunkInt(i*10,null);

            fillChunk(c,numOfItems);

            prev.next.set(c,false);
            chunks.add(c);
            prev = c;
        }
    }


    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testMultiChunkIterate() throws Exception {
        resetState(5, 3);
        MultiChunkIterator<Integer,Integer> iter = new MultiChunkIterator<>(chunks);
        int expected = 0;


        while(iter.hasNext())
        {
            iter.next();
            assertTrue(iter.getKey() == expected);
            assertTrue(iter.getValue() == expected);
            assertTrue(iter.getVersion() == expected);
            expected++;
        }
    }

    @Test
    public void testItemsIterator() throws Exception {
        resetState(3,1);
        Chunk<Integer,Integer>.ItemsIterator iter = chunks.get(0).itemsIterator();
        int expectedKey = 0;
        int expectedValue = 0;

        while(iter.hasNext())
        {
            iter.next();
            assertTrue(iter.getKey() == expectedKey);
            assertTrue(iter.getValue() == expectedValue);
            expectedKey++;
            expectedValue++;

        }
    }

    @Test
    public void testSimpleCloneAndIterate() throws Exception {
        resetState(3,1);
        Chunk<Integer,Integer>.ItemsIterator iter = chunks.get(0).itemsIterator();
        int expectedKey = 0;
        int expectedValue = 0;

        Chunk<Integer,Integer>.ItemsIterator copy = iter.cloneIterator();

        while(iter.hasNext())
        {
            iter = iter.cloneIterator();
            iter.next();
            assertTrue(iter.getKey() == expectedKey);
            assertTrue(iter.getValue() == expectedValue);
            expectedKey++;
            expectedValue++;

            iter = iter.cloneIterator();
        }

        copy.next();

        assertTrue(copy.getKey() == 0);
    }

    @Test
    public void testMultipleCloneAndIterate() throws Exception {
        resetState(3,4);
        MultiChunkIterator<Integer,Integer> iter = new MultiChunkIterator<>(chunks);
        int expectedKey = 0;
        int expectedValue = 0;

        MultiChunkIterator<Integer,Integer> copy = iter.cloneIterator();

        while(iter.hasNext())
        {
            iter = iter.cloneIterator();
            iter.next();
            assertTrue(iter.getKey() == expectedKey);
            assertTrue(iter.getValue() == expectedValue);
            expectedKey++;
            expectedValue++;

            iter = iter.cloneIterator();
        }

        copy.next();

        assertTrue(copy.getKey() == 0);
    }

    @Test
    public void emptyIteratorTest() throws Exception
    {
        Chunk<Integer, Integer> chunk = new ChunkInt(0,null);
        Chunk<Integer, Integer>.ItemsIterator iter = chunk.itemsIterator();

        assertFalse(iter.hasNext());
    }

    @Test
    public void emptyMultiChunkIterTest()
    {
        List<Chunk<Integer,Integer>> chunks = new LinkedList<>();

        try {
            MultiChunkIterator<Integer, Integer> iter = new MultiChunkIterator<>(chunks);
            assertTrue(false);

        } catch(IllegalArgumentException e)
        {

        }

    }


}

