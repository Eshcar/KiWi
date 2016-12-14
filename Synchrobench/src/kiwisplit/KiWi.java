package kiwisplit;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import kiwisplit.ThreadData.PutData;
import kiwisplit.ThreadData.ScanData;

public class KiWi<K extends Comparable<? super K>, V>
{
	/*************** Constants ***************/
	public static int MAX_THREADS = 32;
	public static final int PAD_SIZE = 640;
	
	/*************** Members ***************/
	private final ConcurrentSkipListMap<K, Chunk<K, V>>	skiplist;		// skiplist of chunks for fast navigation
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
			pd = helpPutInGet(version.get(), key);
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
					if (compact(parent) == null)
						return;
				}
			}

			// allocate space in chunk for key & value
			// this also writes key&val into the allocated space
			int oi = c.allocate(key, val);
			
			// if failed - chunk is full, compact it & retry
			if (oi < 0)
			{
				c = compact(c);
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
			int myVersion = c.setVersion(oi, this.version.get());
			
			// if chunk is frozen, clear published data, compact it and retry
			// (when freezing, version is set to FREEZE)
			if (myVersion == Chunk.FREEZE_VERSION)
			{
				// clear thread-array item if needed
				if (withScan)
					publishPut(null);

				c = compact(c);
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
			break;
		}
	}

	public Iterator<V> scan(K min, K max)
	{
		if (withScan)
		{
			// get current version and increment version (atomically) for this scan
			// all items beyond my version are ignored by this scan
			// the newVersion() method is used to ensure my version is published correctly,
			// so concurrent split ops will not compact items with this version (ensuring linearizability)
			int myVer = newVersion();
			
			// help pending put ops set a version - and get in a sorted map for use in the scan iterator
			// (so old put() op doesn't suddently set an old version this scan() needs to see,
			//  but after the scan() passed it)
			SortedMap<K,PutData<K,V>> items = helpPutInScan(myVer, min, max);
			
			// find chunk matching min key, to start iterator there
			Chunk<K,V> c = skiplist.floorEntry(min).getValue();
			c = iterateChunks(c, min);
			
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
	
	/** fetch-and-add for the version counter. in a separate method because scan() ops need to use
	 * thread-array for this, to make sure concurrent split/compaction ops are aware of the scan() */
	private int newVersion()
	{
		// create new ScanData and publish it - in it the scan's version will be stored
		ScanData sd = new ScanData();
		publishScan(sd);
		
		// increment global version counter and get latest
		int myVer = version.getAndIncrement();
		
		// try to set it as this scan's version - return whatever is successfuly set
		if (sd.version.compareAndSet(Chunk.NONE, myVer))
			return myVer;
		else
			return sd.version.get();
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
	
	private TreeSet<Integer> getScans(int myVersion)
	{
		TreeSet<Integer> scans = new TreeSet<>();
		
		// go over thread data of all threads
		for (int i = 0; i < MAX_THREADS; ++i)
		{
			// make sure data is for a Scan operation
			ScanData currScan = scanArray[pad(i)];
			if (currScan == null)
				continue;
			
			// if scan was published but didn't yet CAS its version - help it
			if (currScan.version.get() == Chunk.NONE)
			{
				int ver = version.getAndIncrement();
				currScan.version.compareAndSet(Chunk.NONE, ver);
			}
			
			// read the scan version (which is now set)
			int verScan = currScan.version.get();
			if (verScan < myVersion)
			{
				scans.add(verScan);
			}
		}
		
		return scans;
	}
	
	/** Splits and compacts the given chunk, resulting in two new chunks (or one if compacted enough) 
	 * @return the first of the new chunks created after splitting the given chunk **/
	private Chunk<K,V> compact(Chunk<K,V> chunk)
	{
		// try to get first child chunk
		Chunk<K,V> c1 = null; //chunk.child1.get();
		
		// if it doesn't exist - split chunk to it
		if (c1 == null)
		{
			// get current version that we compact versions < my version
			int myVersion = version.get();

			// get concurrent scans to avoid compacting their versions
			TreeSet<Integer> scans = null;
			if (withScan)
				scans = getScans(myVersion);
			else
				scans = new TreeSet<>();

			// split chunk into the new chunks
			c1 = chunk.split(myVersion, scans);
		
			// try to set my c1 as the 1st child of chunk
			// after this point only one child exists,
			// and all concurrent split/compact ops will try to insert same c1 into list
			if (! chunk.child1.compareAndSet(null, c1))
			{
				// if we failed setting to our c1, somebody else succeeded - just update c1 to it
				c1 = chunk.child1.get();
			}
		}
		
		// also update c2 to next of c1
		// note: if we're slow then c1.next might have changed (c2 was split, etc.)
		// this does not disrupt us because of the next CAS for chunk's 2nd child (child2)
		Chunk<K,V> c2 = c1.next.getReference();
		
		// c2 might be null (split resulted in 1 chunk only), so set it to c1
		if (c2 == null)
			c2 = c1;
		
		// try to set c2 we have as the 2nd child of chunk
		// first thread to get to this point and succeed CAS will necessarily place the correct c2
		// threads after it will fail - so we'll have both updated correctly
		if (! chunk.child2.compareAndSet(null, c2))
			c2 = chunk.child2.get();
		
		// -- all threads that reach this point (for the same chunk) have the SAME c1 and c2
		// -- we now need to replace chunk with c1&c2 in the linked list, and then in the skiplist

		// replace in linked list - we now need to find previous chunk to our chunk
		// and CAS its next to point to c1, which is the same c1 for all threads who reach this point.
		// since prev might be marked (in compact itself) - we need to repeat this until successful
		while (true)
		{
			// start with first chunk (i.e., head)
			Chunk<K,V> curr = skiplist.firstEntry().getValue();	// TODO we can store&update head for a little efficiency
			Chunk<K,V> prev = null;
			
			// iterate until found chunk or reached end of list
			while ((curr != chunk) && (curr != null))
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
				compact(prev);
				continue;
			}
			
			// try to CAS prev chunk's next - from chunk (that we split) into c1
			// c1 is the old chunk's replacement, and is already connected to c2
			// c2 is already connected to old chunk's next - so all we need to do is this replacement
			if ((prev.next.compareAndSet(chunk, c1, false, false)) ||
				(!prev.next.isMarked()))
				// if we're successful, or we failed but prev is not marked - so it means someone else was successful
				// then we're done with loop
				break;
		}

		// -- replace chunk in skiplist
		// c1 is the "smaller" new chunk and should have the same key as original chunk
		// so chunk is replaced with it
		skiplist.replace(chunk.minKey, chunk, c1);
		
		// clear parent to allow garbage collection of old chunk (the frozen parent chunk)
		// this also signifies that chunks are no longer INFANT and can be put into
		c1.creator = null;
		
		// add c2 to skiplist if it's not the same chunk as c1
		// (need to make sure split actually resulted in 2 chunks and not only 1)
		if (c2 != c1)
		{
			// clear parent (c2 not INFANT)
			c2.creator = null;
			
			// c2 is new so is added according to the pivot (which is the minKey of c2)
			// it is added atomically (putIfAbsent) to avoid race conditions where c2 is split and then we put an old value
			skiplist.putIfAbsent(c2.minKey, c2);
		}
		
		return c1;
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

		// publish into thread array
		putArray[pad(idx)] = data;
		
		//TODO store fence here?
	}
	
	/** this method is used by scan operations (ONLY) to help pending put operations set a version
	 * @return sorted map of items matching key range of any currently-pending put operation */
	private SortedMap<K,PutData<K,V>> helpPutInScan(int myVersion, K min, K max)
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
			int currVer = currPut.chunk.getVersion(currPut.orderIndex);
			
			// if empty, try to set to my version
			if (currVer == Chunk.NONE)
				currVer = currPut.chunk.setVersion(currPut.orderIndex, myVersion);
			
			// if item is frozen or beyond my version - skip it
			if ((currVer == Chunk.FREEZE_VERSION) || (currVer > myVersion))
				continue;

			// get found item matching current key
			PutData<K,V> item = (PutData<K,V>) items.get(currKey);
			
			// is there such an item we previously found? check if we need to replace it
			if (item != null)
			{
				// get its version
				int itemVer = item.chunk.getVersion(item.orderIndex);
				
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
						if (currPut.chunk.child1.get() != null)
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
	private PutData<K,V> helpPutInGet(int myVersion, K myKey)
	{
		// marks the most recent put that was found in the thread-array
		PutData<K,V> newestPut = null;
		int newestVer = Chunk.NONE;
		
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
			int currVer = currPut.chunk.getVersion(currPut.orderIndex);
			
			// if empty, try to set to my version
			if (currVer == Chunk.NONE)
				currVer = currPut.chunk.setVersion(currPut.orderIndex, myVersion);
			
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
					if (newestPut.chunk.child1.get() != null)
						newestPut = currPut;
				}
			}
		}
		
		// return item if its chunk.child1 is null, otherwise return null
		if ((newestPut == null) || (newestPut.chunk.child1.get() != null)) 
			return null;
		else
			return newestPut;
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
