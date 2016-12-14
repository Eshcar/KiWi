package kiwi;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by dbasin on 11/24/15.
 */
public class RebalancerTest {


    ArrayList<Chunk<Integer,Integer>> chunks;
    private static final int NUM_OF_CHUNKS = 10;
    private int maxKey = 0;
    private ChunkIterator<Integer,Integer> ci;

    public class TestChunkIterator<K extends Comparable<K>,V> implements ChunkIterator<K,V>{

        @Override
        public Chunk<K, V> getNext(Chunk<K, V> chunk) {
            if(chunk == null) return null;
            return chunk.next.getReference();
        }

        @Override
        public Chunk<K, V> getPrev(Chunk<K, V> chunk) {
            Chunk<K,V> prev = null;

            for(Chunk c : RebalancerTest.this.chunks)
            {
                if(c.next.getReference() == chunk) prev = c;
            }

            return prev;
        }
    }

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

        ci = new TestChunkIterator<Integer, Integer>();
    }

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testEngageChunks() throws Exception {
        resetState(Chunk.MAX_ITEMS/6,5);

        Chunk c = chunks.get(0);
        KiWi.RebalanceSize =3;

       Rebalancer<Integer,Integer> r = new Rebalancer<>(c,ci);

        // here should engage 3 first chunks
        r.engageChunks();

        assertTrue(chunks.get(0).isEngaged(r));
        assertTrue(chunks.get(1).isEngaged(r));
        assertTrue(chunks.get(2).isEngaged(r));

        assertFalse(chunks.get(3).isEngaged(r));
    }

    @Test
    public void testEngageChunksMiddleRange()
    {
        resetState(Chunk.MAX_ITEMS/6,5);

        Chunk c = chunks.get(1);
        KiWi.RebalanceSize = 3;

        Rebalancer<Integer,Integer> r = new Rebalancer<>(c,ci);

        r.engageChunks();

        assertFalse(chunks.get(0).isEngaged(r));

        assertTrue(chunks.get(1).isEngaged(r));
        assertTrue(chunks.get(2).isEngaged(r));
        assertTrue(chunks.get(3).isEngaged(r));

        assertFalse(chunks.get(4).isEngaged(r));


    }

    @Test
    public void testEngageChunksMiddleRangeWithContention()
    {
        resetState(Chunk.MAX_ITEMS/6,5);

        Chunk c = chunks.get(2);
        KiWi.RebalanceSize = 3;

        Rebalancer<Integer,Integer> r = new Rebalancer<>(chunks.get(2),ci);

        Rebalancer<Integer,Integer> r2 = new Rebalancer<>(chunks.get(4),ci);
        chunks.get(4).engage(r2);

        r.engageChunks();

        assertFalse(chunks.get(0).isEngaged(r));

        assertTrue(chunks.get(1).isEngaged(r));
        assertTrue(chunks.get(2).isEngaged(r));
        assertTrue(chunks.get(3).isEngaged(r));

        assertFalse(chunks.get(4).isEngaged(r));


    }

    @Test
    public void testEngageChunksMiddleRangeWithContentionBothSides()
    {
        resetState(Chunk.MAX_ITEMS/6,5);

        Chunk c = chunks.get(2);
        KiWi.RebalanceSize = 3;

        Rebalancer<Integer,Integer> r = new Rebalancer<>(chunks.get(2),ci);

        Rebalancer<Integer,Integer> r2 = new Rebalancer<>(chunks.get(4),ci);
        Rebalancer<Integer,Integer> r3 = new Rebalancer<>(chunks.get(1),ci);

        chunks.get(4).engage(r2);
        chunks.get(1).engage(r3);

        r.engageChunks();

        assertFalse(chunks.get(0).isEngaged(r));
        assertFalse(chunks.get(1).isEngaged(r));

        assertTrue(chunks.get(2).isEngaged(r));
        assertTrue(chunks.get(3).isEngaged(r));

        assertFalse(chunks.get(4).isEngaged(r));


    }

    @Test
    public void testEngageChunksHelping()
    {
        resetState(Chunk.MAX_ITEMS/6,5);

        KiWi.RebalanceSize = 3;
        Rebalancer<Integer,Integer> r = new Rebalancer<>(chunks.get(2),ci);

        Rebalancer<Integer,Integer> r2 = new Rebalancer<>(chunks.get(2),ci);
        chunks.get(2).engage(r2);

        r.engageChunks();

        assertFalse(chunks.get(0).isEngaged(r2));
        assertFalse(chunks.get(1).isEngaged(r2));

        assertTrue(chunks.get(2).isEngaged(r2));
        assertTrue(chunks.get(3).isEngaged(r2));
        assertTrue(chunks.get(4).isEngaged(r2));
    }


    @Test
    public void testCompactMergeChunks()
    {
        resetState(4, 2);
        Rebalancer<Integer,Integer> r = new Rebalancer<>(chunks.get(0),new TestChunkIterator<Integer,Integer>());
        r.engageChunks();
        assertTrue(r.isEngaged());

        r.freeze();
        assertTrue(r.isFreezed());

        //TreeSet<Integer> scans = new TreeSet<>();
        ArrayList<ThreadData.ScanData> scans = new ArrayList<>();

        r.compact(new ScanIndex<Integer>(scans, 2, 0, maxKey));

        List<Chunk<Integer,Integer>> compacted = r.getCompactedChunks();

        assertTrue(compacted.size() == 1);
    }

    @Test
    public void testCompactSplitChunks()
    {
        resetState((int)(Chunk.MAX_ITEMS*0.9), 1);
        Rebalancer<Integer,Integer> r = new Rebalancer<>(chunks.get(0),new TestChunkIterator<Integer,Integer>());
        r.engageChunks();
        assertTrue(r.isEngaged());

        r.freeze();
        assertTrue(r.isFreezed());


        ArrayList<ThreadData.ScanData> scans = new ArrayList<>();

        r.compact(new ScanIndex<Integer>(scans, 2, 0, maxKey));


        List<Chunk<Integer,Integer>> compacted = r.getCompactedChunks();

        assertTrue(compacted.size() == 2);
    }

    @Test
    public void testCompactCompressMultiChunk()
    {
        Chunk<Integer, Integer> c1 = new ChunkInt();

        c1.appendItem(0,1,3);
        c1.appendItem(0,2,3);
        c1.appendItem(0,3,2);

        c1.appendItem(1,4,2);

        c1.appendItem(2,5,4);
        c1.appendItem(2,6,3);
        c1.appendItem(2,7,2);



        Chunk<Integer, Integer> c2 = new ChunkInt();
        c1.next.set(c2,false);

        c2.appendItem(3,8,2);
        c2.appendItem(3,9,2);

        chunks = new ArrayList<>();
        chunks.add(c1);
        chunks.add(c2);

        Rebalancer<Integer,Integer> r = new Rebalancer<Integer, Integer>(c1, new TestChunkIterator<Integer, Integer>());
        r.engageChunks();

        assertTrue(r.isEngaged());

        r.freeze();
        assertTrue(r.isFreezed());

        ArrayList<ThreadData.ScanData> scans = new ArrayList<>();

        r.compact(new ScanIndex<Integer>(scans, 2, 0, maxKey));


        List<Chunk<Integer,Integer>> compacted = r.getCompactedChunks();

        assertTrue(compacted.size() == 1);
        Chunk<Integer,Integer>.ItemsIterator iter = compacted.get(0).itemsIterator();

        assertTrue(iter.hasNext());

        iter.next();
        assertTrue(iter.getKey() == 0);
        assertTrue(iter.getVersion() == 3);
        assertTrue(iter.hasNext());

        iter.next();
        assertTrue(iter.getKey() == 1);
        assertTrue(iter.getVersion() == 2);

        assertTrue(iter.hasNext());

        iter.next();
        assertTrue(iter.getKey() == 2);
        assertTrue(iter.getVersion() == 4);
        assertTrue(iter.getValue() == 5);

        assertTrue(iter.hasNext());

        iter.next();
        assertTrue(iter.getKey() == 3);
        assertTrue(iter.getVersion() == 2);

    }



    @Test
    public void testCompactCompressMultiChunkWithVersions()
    {
        Chunk<Integer, Integer> c1 = new ChunkInt();

        c1.appendItem(0,1,3);
        c1.appendItem(0,2,3);
        c1.appendItem(0,3,2);

        c1.appendItem(1,4,2);

        c1.appendItem(2,5,4);
        c1.appendItem(2,6,3);
        c1.appendItem(2,7,2);



        Chunk<Integer, Integer> c2 = new ChunkInt();
        c1.next.set(c2,false);

        c2.appendItem(3,8,2);
        c2.appendItem(3,9,2);

        chunks = new ArrayList<>();
        chunks.add(c1);
        chunks.add(c2);

        Rebalancer<Integer,Integer> r = new Rebalancer<Integer, Integer>(c1, new TestChunkIterator<Integer, Integer>());
        r.engageChunks();

        assertTrue(r.isEngaged());

        r.freeze();
        assertTrue(r.isFreezed());


//        int[] scans = new int[1];
//        scans[0] = 2;

        ArrayList<ThreadData.ScanData> scans = new ArrayList<>();
        scans.add( new ThreadData.ScanData(0,Integer.MAX_VALUE-1));
        scans.get(0).version.set(2);

        r.compact(new ScanIndex<Integer>(scans, 3, 0, Integer.MAX_VALUE));

        List<Chunk<Integer,Integer>> compacted = r.getCompactedChunks();

        assertTrue(compacted.size() == 1);
        Chunk<Integer,Integer>.ItemsIterator iter = compacted.get(0).itemsIterator();

        assertTrue(iter.hasNext());

        iter.next();
        assertTrue(iter.getKey() == 0);
        assertTrue(iter.getVersion() == 3);


        assertTrue(iter.hasNext());

        iter.next();
        assertTrue(iter.getKey() == 0);
        assertTrue(iter.getVersion() == 2);

        assertTrue(iter.hasNext());

        iter.next();
        assertTrue(iter.getKey() == 1);
        assertTrue(iter.getVersion() == 2);

        assertTrue(iter.hasNext());

        iter.next();
        assertTrue(iter.getKey() == 2);
        assertTrue(iter.getVersion() == 4);
        assertTrue(iter.getValue() == 5);

        assertTrue(iter.hasNext());

        iter.next();
        assertTrue(iter.getKey() == 2);
        assertTrue(iter.getVersion() == 2);
        assertTrue(iter.getValue() == 7);

        assertTrue(iter.hasNext());

        iter.next();
        assertTrue(iter.getKey() == 3);
        assertTrue(iter.getVersion() == 2);

    }


    @Test
    public void testCompactCompressMultiChunkWithDeletes()
    {
        Chunk<Integer, Integer> c1 = new ChunkInt();

        c1.appendItem(0,1,3);
        c1.appendItem(0,2,3);
        c1.appendItem(0,null,2);
        c1.appendItem(0,3,2);

        c1.appendItem(1,null,2);
        c1.appendItem(1,4,2);

        c1.appendItem(2,5,4);
        c1.appendItem(2,6,3);
        c1.appendItem(2,7,2);



        Chunk<Integer, Integer> c2 = new ChunkInt();
        c1.next.set(c2,false);

        c2.appendItem(3,8,2);
        c2.appendItem(3,9,2);

        chunks = new ArrayList<>();
        chunks.add(c1);
        chunks.add(c2);

        Rebalancer<Integer,Integer> r = new Rebalancer<Integer, Integer>(c1, new TestChunkIterator<Integer, Integer>());
        r.engageChunks();

        assertTrue(r.isEngaged());

        r.freeze();
        assertTrue(r.isFreezed());


        ArrayList<ThreadData.ScanData> scans = new ArrayList<>();
        scans.add(new ThreadData.ScanData(0,Integer.MAX_VALUE -1));
        scans.get(0).version.set(2);

        r.compact(new ScanIndex<Integer>(scans, 3, 0, Integer.MAX_VALUE));

        //r.compact(new ScanIndex<Integer>(scans, 3));

        List<Chunk<Integer,Integer>> compacted = r.getCompactedChunks();

        assertTrue(compacted.size() == 1);
        Chunk<Integer,Integer>.ItemsIterator iter = compacted.get(0).itemsIterator();

        assertTrue(iter.hasNext());

        iter.next();
        assertTrue(iter.getKey() == 0);
        assertTrue(iter.getVersion() == 3);

        assertTrue(iter.hasNext());

        iter.next();
        assertTrue(iter.getKey() == 2);
        assertTrue(iter.getVersion() == 4);
        assertTrue(iter.getValue() == 5);

        assertTrue(iter.hasNext());

        iter.next();
        assertTrue(iter.getKey() == 2);
        assertTrue(iter.getVersion() == 2);
        assertTrue(iter.getValue() == 7);

        assertTrue(iter.hasNext());

        iter.next();
        assertTrue(iter.getKey() == 3);
        assertTrue(iter.getVersion() == 2);

    }


    @Test
    public void testCompactFullChunk()
    {
        Chunk.MAX_ITEMS = 2;
        Chunk<Integer, Integer> c1 = new ChunkInt();
        c1.appendItem(0,1,3);
        c1.appendItem(0,2,3);

        chunks = new ArrayList<>();
        chunks.add(c1);

        Rebalancer<Integer,Integer> r = new Rebalancer<Integer, Integer>(c1, new TestChunkIterator<Integer, Integer>());
        r.engageChunks();

        assertTrue(r.isEngaged());

        r.freeze();
        assertTrue(r.isFreezed());

        //TreeSet<Integer> scans = new TreeSet<>();
        //scans.add(2);

        ArrayList<ThreadData.ScanData> scans= new ArrayList<>();
        scans.add(new ThreadData.ScanData(0, 10));
        scans.get(0).version.set(2);

        r.compact(new ScanIndex<Integer>(scans, 3,0, 10));

        List<Chunk<Integer,Integer>> compacted = r.getCompactedChunks();

        assertTrue(compacted.size() == 1);
        //ItemsIterator<Integer,Integer> iter = compacted.get(0).itemsIterator();

    }

    @Test
    public void testKiWiFill()
    {
        KiWiMap kmap = new KiWiMap();
        Chunk.MAX_ITEMS = 1024;
        int itemsToInsert = Chunk.MAX_ITEMS + 450;
        for(int i = 0; i < itemsToInsert; ++i)
        {
            kmap.put((Integer)i,(Integer) i);
        }

        DebugStats ds = kmap.kiwi.calcChunkStatistics();

        assertTrue(ds.itemCount == itemsToInsert);

    }
}