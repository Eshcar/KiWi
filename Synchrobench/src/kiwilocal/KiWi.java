package kiwilocal;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import kiwilocal.*;
import kiwilocal.ThreadData.PutData;
import kiwilocal.ThreadData.ScanData;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class KiWi<K extends Comparable<? super K>, V> implements ChunkIterator<K,V>
{

	/*************** Constants ***************/
	public static int MAX_THREADS = 32;
	public static final int PAD_SIZE = 640;
	public static int RebalanceSize = 2;

	/*************** Members ***************/
	private final ConcurrentSkipListMap<K , Chunk<K, V>>	skiplist;		// skiplist of chunks for fast navigation
	protected AtomicInteger 							version;		// current version to add items with
	private final boolean								withScan;		// support scan operations or not (scans add thread-array)
	//private final ThreadData[]		threadArray;	// thread-data for each thread
	private final PutData<K,V>[]	putArray;
	private final ScanData[]		scanArray;


	/*************** Constructors ***************/
	public KiWi(Chunk<K,V> head)
	{
		this(head, false);
	}

	@SuppressWarnings("unchecked")
	public KiWi(Chunk<K,V> head, boolean withScan)
	{
		this.skiplist = new ConcurrentSkipListMap<>();
		this.version = new AtomicInteger(2);	// first version is 2 - since 0 means NONE, and -1 means FREEZE

		// TODO use unsafe for version?

		this.skiplist.put(head.minKey, head);	// add first chunk (head) into skiplist
		this.withScan = withScan;

		if (withScan) {
			//this.threadArray = new ThreadData[MAX_THREADS];
			this.putArray = new PutData[MAX_THREADS * (PAD_SIZE + 1)];
			this.scanArray = new ScanData[MAX_THREADS * (PAD_SIZE + 1)];
		}
		else {
			//this.threadArray = null;
			this.putArray = null;
			this.scanArray = null;
		}
	}

	/*************** Methods ***************/

	private int pad(int idx)
	{
		return (PAD_SIZE + idx*PAD_SIZE);
	}

	public V get(K key)
	{
		PutData<K,V> pd = null;

		if (withScan)
		{
			// help concurrent put operations (helpPut) set a version
			pd = helpPutInGet(key);
		}

		// find chunk matching key
		Chunk<K,V> c = skiplist.floorEntry(key).getValue();
		c = iterateChunks(c, key);

		// find item matching key inside chunk
		return c.find(key, pd);
	}

	public void put(K key, V val)
	{
		// find chunk matching key
		Chunk<K,V> c = skiplist.floorEntry(key).getValue();

		// repeat until put operation is successful
		while (true)
		{
			// the chunk we have might have been in part of split so not accurate
			// we need to iterate the chunks to find the correct chunk following it
			c = iterateChunks(c, key);

			// if chunk is infant chunk (has a parent), we can't add to it
			// we need to help finish compact for its parent first, then proceed
			{
				Chunk<K,V> parent = c.creator;
				if (parent != null) {
					if (rebalance(parent) == null)
						return;
				}
			}

			// allocate space in chunk for key & value
			// this also writes key&val into the allocated space
			int oi = c.allocate(key, val);

			// if failed - chunk is full, compact it & retry
			if (oi < 0)
			{
				c = rebalance(c);
				if (c == null)
					return;
				continue;
			}

			if (withScan)
			{
				// publish put operation in thread array
				// publishing BEFORE setting the version so that other operations can see our value and help
				// this is in order to prevent us from adding an item with an older version that might be missed by others (scan/get)
				publishPut(new PutData<>(c, oi));
			}

			// try to update the version to current version, but use whatever version is successfuly set
			// reading & setting version AFTER publishing ensures that anyone who sees this put op has
			// a version which is at least the version we're setting, or is otherwise setting the version itself
			long myVersion = buildVersion(c);
			myVersion = c.setInitialVersion(oi, myVersion);

			// if chunk is frozen, clear published data, compact it and retry
			// (when freezing, version is set to FREEZE)
			if (myVersion == Chunk.FREEZE_VERSION)
			{
				// clear thread-array item if needed
				if (withScan)
					publishPut(null);

				c = rebalance(c);
				if (c == null)
					return;
				continue;
			}

			// allocation is done (and published) and has a version
			// all that is left is to insert it into the chunk's linked list
			c.addToList(oi, key);

			// delete operation from thread array - and done
			if (withScan)
				publishPut(null);

			if(shouldRebalance(c))
				rebalance(c);

			break;
		}
	}

	private long buildVersion(Chunk<K, V> c) {
		int high = version.get();
		int low = c.localVersion.get();

		return combineVersion(low,high);
	}

	private boolean shouldRebalance(Chunk<K, V> c) {

		// if no merges allowed -- should not rebalance
		if(RebalanceSize < 2) return false;

		// perform actual check only in 5% of puts
		if(ThreadLocalRandom.current().nextInt(100) < 95) return false;

		// if another thread already runs rebalance -- skip it
		if(!c.isEngaged(null)) return false;
		Chunk<K,V> next = c.next.getReference();
		if(next == null) return false;
		int compacted =c.getStatistics().getCompactedCount() + next.getStatistics().getCompactedCount();
		int total = c.getStatistics().getFilledCount() + next.getStatistics().getFilledCount();
		int threshold = c.MAX_ITEMS/2;


		if( total > c.MAX_ITEMS && compacted <  threshold) {
			return true;
		}else {
			return false;
		}
	}

	public Iterator<V> scan(K min, K max)
	{
		if (withScan)
		{

			// find chunk matching min key, to start iterator there
			Chunk<K,V> c = skiplist.floorEntry(min).getValue();
			c = iterateChunks(c, min);

			// get current version and increment version (atomically) for this scan
			// all items beyond my version are ignored by this scan
			// the newVersion() method is used to ensure my version is published correctly,
			// so concurrent split ops will not compact items with this version (ensuring linearizability)
			long myVer = newVersion(c,min, max);

			// help pending put ops set a version - and get in a sorted map for use in the scan iterator
			// (so old put() op doesn't suddently set an old version this scan() needs to see,
			//  but after the scan() passed it)
			SortedMap<K,PutData<K,V>> items = helpPutInScan(myVer, min, max);

			// create a new ScanIterator for this scan and return it
			// the iterator will contain the actual scan logic
			return new ScanIterator<>(min, max, myVer, c, items);
		}
		else
		{
			//TODO implement non-atomic scan
			return null;
		}
	}

	@Override
	public Chunk<K,V> getNext(Chunk<K,V> chunk)
	{
		return chunk.next.getReference();
	}

	@Override
	public Chunk<K,V> getPrev(Chunk<K,V> chunk)
	{

		Map.Entry<K, Chunk<K, V>> kChunkEntry = skiplist.lowerEntry(chunk.minKey);
		if(kChunkEntry == null) return null;
		Chunk<K,V> prev = kChunkEntry.getValue();

		while(true)
		{
			Chunk<K,V> next = prev.next.getReference();
			if(next == chunk) break;
			if(next == null) {
				prev = null;
				break;
			}

			prev = next;
		}

		return prev;

	}


	private long newGlobalVersion()
	{
		int highVer = version.getAndIncrement();
		return combineVersion(Integer.MAX_VALUE,highVer);
	}

	private long newLocalVersion(Chunk<K,V> c)
	{
		int verLow = c.localVersion.getAndIncrement();
		int verHigh = version.get();

		return combineVersion(verLow, verHigh);
	}

	/** fetch-and-add for the version counter. in a separate method because scan() ops need to use
	 * thread-array for this, to make sure concurrent split/compaction ops are aware of the scan() */
	private long newVersion(Chunk<K,V> c, K min, K max)
	{


		// create new ScanData and publish it - in it the scan's version will be stored
		ScanData sd = new ScanData();
		Chunk<K,V> next = c.next.getReference();
		if(next == null || next.minKey.compareTo(max) < 0)
		{
			sd.chunk = null;
		} else
		{
			sd.chunk = c;
		}

		publishScan(sd);

		// increment global version counter and get latest
		long myVer = Chunk.NONE_VERSION;



		if(sd.isMultiChunk())
			myVer = newGlobalVersion();
		else
			myVer = newLocalVersion(c);


		// try to set it as this scan's version - return whatever is successfuly set
		if (sd.version.compareAndSet(Chunk.NONE_VERSION, myVer))
			return myVer;
		else
			return sd.version.get();
	}

	private static long combineVersion(int verLow, int verHigh) {
		return (((long)verHigh) << 32) | (verLow & 0xffffffffL);
	}

	/** finds and returns the chunk where key should be located, starting from given chunk */
	private Chunk<K,V> iterateChunks(Chunk<K,V> c, K key)
	{
		// find chunk following given chunk (next)
		Chunk<K,V> next = c.next.getReference();

		// found chunk might be in split process, so not accurate
		// since skiplist isn't updated atomically in split/compcation, our key might belong in the next chunk
		// next chunk might itself already be split, we need to iterate the chunks until we find the correct one
		while ((next != null) && (next.minKey.compareTo(key) <= 0))
		{
			c = next;
			next = c.next.getReference();
		}

		return c;
	}

	private long[] getScansArray(long myVersion)
	{
		long[] scans = new long[MAX_THREADS];

		for(int i = 0; i < MAX_THREADS; ++i)
		{
			ScanData currScan = scanArray[pad(i)];
			if(currScan ==  null)
				continue;

			if(currScan.version.get() == Chunk.NONE_VERSION)
			{
				// TODO: understand if we need to increment here
				long ver = Chunk.NONE_VERSION;
				if(currScan.isMultiChunk())
				{
					ver = newGlobalVersion();
				} else
				{
					ver = newLocalVersion(currScan.chunk);
				}

				currScan.version.compareAndSet(Chunk.NONE_VERSION, ver);
			}

			// read the scan version (which is now set)
			long verScan = currScan.version.get();
			if (verScan < myVersion)
			{
				scans[i] = verScan;
			}
		}

		Arrays.sort(scans);

		// mirror the array
		for( int i = 0; i < scans.length/2; ++i )
		{
			long temp = scans[i];
			scans[i] = scans[scans.length - i - 1];
			scans[scans.length - i - 1] = temp;
		}

		return scans;
	}
	private Chunk<K,V> rebalance(Chunk<K,V> chunk)
	{
		Rebalancer<K,V> rebalancer = new Rebalancer<>(chunk, this);

		rebalancer = rebalancer.engageChunks();

		// freeze all the engaged range.
		// When completed, all update (put, next pointer update) operations on the engaged range
		// will be redirected to help the rebalance procedure
		rebalancer.freeze();

		// before starting compaction -- check if another thread has completed this stage
		if(!rebalancer.isCompacted()) {
			ScanIndex<K> index = updateAndGetPendingScans(combineVersion(0,version.get()));
			rebalancer.compact(index);
		}

		// the children list may be generated by another thread
		List<Chunk<K,V>> engaged = rebalancer.getEngagedChunks();
		List<Chunk<K,V>> compacted = rebalancer.getCompactedChunks();


		connectToChunkList(engaged, compacted);
		updateIndex(engaged, compacted);

		return compacted.get(0);
	}

	private ScanIndex<K> updateAndGetPendingScans(long currVersion) {
		// TODO: implement versions selection by key
		return new ScanIndex(getScansArray(currVersion), currVersion);
	}

	private void updateIndex(List<Chunk<K,V>> engagedChunks, List<Chunk<K,V>> compacted)
	{
		Iterator<Chunk<K,V>> iterEngaged = engagedChunks.iterator();
		Iterator<Chunk<K,V>> iterCompacted = compacted.iterator();

		Chunk<K,V> firstEngaged = iterEngaged.next();
		Chunk<K,V> firstCompacted = iterCompacted.next();

		skiplist.replace(firstEngaged.minKey,firstEngaged, firstCompacted);

		// update from infant to normal
		firstCompacted.creator = null;

		// remove all old chunks from index.
		// compacted chunks are still accessible through the first updated chunk
		while(iterEngaged.hasNext())
		{
			Chunk<K,V> engagedToRemove = iterEngaged.next();
			skiplist.remove(engagedToRemove.minKey,engagedToRemove);
		}

		// for simplicity -  naive lock implementation
		// can be implemented without locks using versions on next pointer in  skiplist

		while(iterCompacted.hasNext())
		{
			Chunk<K,V> compactedToAdd = iterCompacted.next();

			synchronized (compactedToAdd)
			{
				skiplist.putIfAbsent(compactedToAdd.minKey,compactedToAdd);
				compactedToAdd.creator = null;
			}
		}
	}

	private void connectToChunkList(List<Chunk<K, V>> engaged, List<Chunk<K, V>> children) {

		updateLastChild(engaged,children);

		Chunk<K,V> firstEngaged = engaged.get(0);

		// replace in linked list - we now need to find previous chunk to our chunk
		// and CAS its next to point to c1, which is the same c1 for all threads who reach this point.
		// since prev might be marked (in compact itself) - we need to repeat this until successful
		while (true)
		{
			// start with first chunk (i.e., head)
			Chunk<K,V> curr = skiplist.firstEntry().getValue();	// TODO we can store&update head for a little efficiency
			Chunk<K,V> prev = null;

			// iterate until found chunk or reached end of list
			while ((curr != firstEngaged) && (curr != null))
			{
				prev = curr;
				curr = curr.next.getReference();
			}

			// chunk is head or not in list (someone else already updated list), so we're done with this part
			if ((curr == null) || (prev == null))
				break;

			// if prev chunk is marked - it is deleted, need to help split it and then continue
			if (prev.next.isMarked())
			{
				rebalance(prev);
				continue;
			}

			// try to CAS prev chunk's next - from chunk (that we split) into c1
			// c1 is the old chunk's replacement, and is already connected to c2
			// c2 is already connected to old chunk's next - so all we need to do is this replacement
			if ((prev.next.compareAndSet(firstEngaged, children.get(0), false, false)) ||
					(!prev.next.isMarked()))
				// if we're successful, or we failed but prev is not marked - so it means someone else was successful
				// then we're done with loop
				break;
		}

	}

	private void updateLastChild(List<Chunk<K, V>> engaged, List<Chunk<K, V>> children) {
		Chunk<K,V> lastEngaged = engaged.get(engaged.size()-1);
		Chunk<K,V> nextToLast =  lastEngaged.markAndGetNext();
		Chunk<K,V> lastChild = children.get(children.size() -1);

		lastChild.next.compareAndSet(null, nextToLast, false, false);
	}

	/** publish data into thread array - use null to clear **/
	private void publishScan(ScanData data)
	{
		// get index of current thread
		// since thread IDs are increasing and changing, we assume threads are created one after another (sequential IDs).
		// thus, (ThreadID % MAX_THREADS) will return a unique index for each thread in range [0, MAX_THREADS)
		int idx = (int) (Thread.currentThread().getId() % MAX_THREADS);

		// publish into thread array
		scanArray[pad(idx)] = data;

		//TODO store fence here?
	}
	/** publish data into thread array - use null to clear **/
	private void publishPut(PutData<K,V> data)
	{
		// get index of current thread
		// since thread IDs are increasing and changing, we assume threads are created one after another (sequential IDs).
		// thus, (ThreadID % MAX_THREADS) will return a unique index for each thread in range [0, MAX_THREADS)
		int idx = (int) (Thread.currentThread().getId() % MAX_THREADS);

		// TODO verify the assumption about sequential IDs

		// publish into thread array
		putArray[pad(idx)] = data;

		//TODO store fence here?
	}

	/** this method is used by scan operations (ONLY) to help pending put operations set a version
	 * @return sorted map of items matching key range of any currently-pending put operation */
	private SortedMap<K,PutData<K,V>> helpPutInScan(long myVersion, K min, K max)
	{
		SortedMap<K,PutData<K,V>> items = new TreeMap<>();

		// go over thread data of all threads
		for (int i = 0; i < MAX_THREADS; ++i)
		{
			// make sure data is for a Put operatio
			PutData<K,V> currPut = putArray[pad(i)];
			if (currPut == null)
				continue;

			// if put operation's key is not in key range - skip it
			K currKey = currPut.chunk.readKey(currPut.orderIndex);
			if ((currKey.compareTo(min) < 0) || (currKey.compareTo(max) > 0))
				continue;

			// read the current version of the item
			long currVer = currPut.chunk.getVersionAbs(currPut.orderIndex);

			// if empty, try to set to my version
			if (currVer == Chunk.NONE)
				currVer = currPut.chunk.setInitialVersion(currPut.orderIndex, myVersion);

			// if item is frozen or beyond my version - skip it
			if ((currVer == Chunk.FREEZE_VERSION) || (currVer > myVersion))
				continue;

			// get found item matching current key
			PutData<K,V> item = (PutData<K,V>) items.get(currKey);

			// is there such an item we previously found? check if we need to replace it
			if (item != null)
			{
				// get its version
				long itemVer = item.chunk.getVersionAbs(item.orderIndex);

				// existing item is newer - don't replace
				if (itemVer > currVer)
				{
					continue;
				}
				// if same versions - continue checking (otherwise currVer is newer so will replace item)
				else if (itemVer == currVer)
				{
					// both in same chunk - position determines newest
					if (item.chunk == currPut.chunk)
					{
						// same chunk & version but items's index is larger - don't replace
						if (item.orderIndex > currPut.orderIndex)
							continue;
					}
					// in different chunks - check which chunk has a child (i.e., split so irrelevant)
					else
					{
						// if chunk has child1, it probably isn't newer- don't replace
						if (currPut.chunk.isRebalanced())
							continue;
					}
				}

				// if we've reached here then curr is newer than item, and we replace it
				items.put(currKey, currPut);
			}
		}

		return items;
	}

	/** this method is used by get operations (ONLY) to help pending put operations set a version
	 * @return newest item matching myKey of any currently-pending put operation */
	private PutData<K,V> helpPutInGet(K myKey)
	{

		// marks the most recent put that was found in the thread-array
		PutData<K,V> newestPut = null;
		long newestVer = Chunk.NONE_VERSION;

		// go over thread data of all threads
		for (int i = 0; i < MAX_THREADS; ++i)
		{
			// make sure data is for a Put operation
			PutData<K,V> currPut = putArray[pad(i)];
			if (currPut == null)
				continue;

			// if put operation's key is not same as my key - skip it
			K currKey = currPut.chunk.readKey(currPut.orderIndex);
			if (currKey.compareTo(myKey) != 0)
				continue;

			// read the current version of the item
			long currVer = currPut.chunk.getVersionAbs(currPut.orderIndex);

			// if empty, try to set to my version
			if (currVer == Chunk.NONE_VERSION) {
				Chunk<K,V> c = currPut.chunk;

				currVer = currPut.chunk.setInitialVersion(currPut.orderIndex, buildVersion(currPut.chunk));
			}

			// if item is frozen - skip it
			if (currVer == Chunk.FREEZE_VERSION)
				continue;

			// current item has newer version than newest item - replace
			if (currVer > newestVer)
			{
				newestVer = currVer;
				newestPut = currPut;
			}
			// same version for both item - check according to chunk
			else if (currVer == newestVer)
			{
				// both in same chunk - position determines newest
				if (currPut.chunk == newestPut.chunk)
				{
					// same chunk & version but current's index is larger - it is newer
					if (currPut.orderIndex > newestPut.orderIndex)
						newestPut = currPut;
				}
				// in different chunks - check which chunk has a child (i.e., split so irrelevant)
				else
				{
					// if stored newest chunk has child1 - use other chunk's item
					if (newestPut.chunk.isRebalanced())
						newestPut = currPut;
				}
			}
		}

		// return item if its chunk.child1 is null, otherwise return null
		if ((newestPut == null) || (newestPut.chunk.isRebalanced()))
			return null;
		else
			return newestPut;
	}

	public void compactAllSerial()
	{
		Chunk<K,V> c  = skiplist.firstEntry().getValue();
		while(c!= null)
		{
			c = rebalance(c);
			c =c.next.getReference();
		}
	}

	public int debugCountKeys()
	{
		int keys = 0;
		Chunk<K,V> chunk = skiplist.firstEntry().getValue();

		while (chunk != null)
		{
			keys += chunk.debugCountKeys();
			chunk = chunk.next.getReference();
		}
		return keys;
	}
	public int debugCountDups()
	{
		int dups = 0;
		Chunk<K,V> chunk = skiplist.firstEntry().getValue();
		
		while (chunk != null)
		{
			dups += chunk.debugCountDups();
			chunk = chunk.next.getReference();
		}
		return dups;
	}
	public void debugPrint()
	{
		Chunk<K,V> chunk = skiplist.firstEntry().getValue();
		
		while (chunk != null)
		{
			System.out.print("[ ");
			chunk.debugPrint();
			System.out.print("]\t");
			
			chunk = chunk.next.getReference();
		}
		System.out.println();
	}

}
