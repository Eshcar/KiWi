package kiwisplit;

import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;

import kiwisplit.ThreadData.PutData;
import sun.misc.Unsafe;

/**
 * Created by msulamy on 7/27/15.
 */
public abstract class Chunk<K extends Comparable<? super K>,V>
{	
	/***************	Constants			***************/
	protected static final int		NONE = 0;	// constant for "no version", "no index", etc. MUST BE 0!
	protected static final int		FREEZE_VERSION	= 1;
	
	// order_size(4) = next + version + key + data
	private static final int		ORDER_SIZE = 4;		// # of fields in each item of order array
	private static final int		OFFSET_NEXT = 0;
	private static final int		OFFSET_VERSION = 1;	// POSITIVE means item is linked, otherwise might not be linked yet
	protected static final int		OFFSET_KEY = 2;
	protected static final int		OFFSET_DATA = 3;
	
	// location of the first (head) node - just a next pointer
	private static final int		HEAD_NODE = 0;
	// index of first item in order-array, after head (not necessarily first in list!)
	private static final int		FIRST_ITEM = 1;
	
	public static int				MAX_ITEMS = 4500;
	public static boolean			ALLOW_DUPS = true;
	//private static final int		MIN_ITEMS = 0;

	/***************	Members				***************/
	private static final Unsafe					unsafe;
	private final int[]							orderArray;	// array is initialized to 0, i.e., NONE - this is important!
	protected final byte[]						dataArray;

	private final AtomicInteger					orderIndex;	// points to next free index of order array
	private final AtomicInteger					dataIndex;	// points to next free index of data array 

	public K									minKey;		// minimal key that can be put in this chunk
	public AtomicMarkableReference<Chunk<K,V>>	next;
	public Chunk<K,V>							creator;	// in split/compact process, represents parent of split (can be null!)
	public AtomicReference<Chunk<K,V>>			child1;		// in split/compact process, represents 1st child of this chunk
	public AtomicReference<Chunk<K,V>>			child2;		// in split/compact process, represents 2nd child of this chunk
	//TODO use unsafe for child?
	private int									sortedCount;// # of sorted items at order-array's beginning (resulting from split)
	
	//private final AtomicBoolean					isFrozen = new AtomicBoolean(false);

	/***************	Constructors		***************/
	/**
	 * Create a new chunk
	 * @param minKey	minimal key to be placed in chunk, used by KiWi
	 * @param dataItemSize	expected avg. size (in BYTES!) of items in data-array. can be an estimate
	 */
	public Chunk(K minKey, int dataItemSize, Chunk<K,V> creator)
	{
		// allocate space for head item (only "next", starts pointing to NONE==0)
		this.orderIndex = new AtomicInteger(FIRST_ITEM);
		this.dataIndex = new AtomicInteger(FIRST_ITEM);	// index 0 in data is "NONE"

		// allocate space for MAX_ITEMS, and add FIRST_ITEM (size of head) for order array
		//this.orderArray = new AtomicIntegerArray(MAX_ITEMS * ORDER_SIZE + FIRST_ITEM);	// initialized to 0, i.e., NONE
		this.orderArray = new int[MAX_ITEMS * ORDER_SIZE + FIRST_ITEM];
		this.dataArray = new byte[MAX_ITEMS * dataItemSize + 1];

		this.child1 = new AtomicReference<>(null);
		this.child2 = new AtomicReference<>(null);
		this.next = new AtomicMarkableReference<>(null, false);
		this.minKey = minKey;
		this.creator = creator;
		this.sortedCount = 0;		// no sorted items at first

		// TODO allocate space for minKey inside chunk (pointed by skiplist)?
	}
	
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
	
	/***************	Abstract Methods	***************/
	public abstract int				allocate(K key, V data);
	public abstract K				readKey(int orderIndex);
	public abstract V				readData(int orderIndex, int dataIndex);
	/** should CLONE minKey as needed */
	protected abstract Chunk<K,V>	newChunk(K minKey);

	/***************	Methods				***************/
	
	/** gets the data for the given item, or 'null' if it doesn't exist */
	public V getData(int orderIndex)
	{
		// get index of data in data-array (abs- it might be negative)
		int di = get(orderIndex, OFFSET_DATA);
		
		// if no data for item - return null
		if (di == NONE)
			return null;
		else
			return readData(orderIndex, di);
	}
	
	/** gets the field of specified offset for given item */
	protected int get(int item, int offset)
	{
		return orderArray[item+offset];
	}
	/** sets the field of specified offset to 'value' for given item */
	protected void set(int item, int offset, int value)
	{
		orderArray[item+offset] = value;
	}
	/** performs CAS from 'expected' to 'value' for field at specified offset of given item */
	private boolean cas(int item, int offset, int expected, int value)
	{
		return unsafe.compareAndSwapInt(orderArray,
			Unsafe.ARRAY_INT_BASE_OFFSET + (item + offset) * Unsafe.ARRAY_INT_INDEX_SCALE,
			expected, value);
	}
	
	/** binary search for largest-entry smaller than 'key' in sorted part of order-array.
	 * @return the index of the entry from which to start a linear search -
	 * if key is found, its previous entry is returned! */
	private int binaryFind(K key)
	{
		// if there are no sorted keys,or the first item is already larger than key -
		// return the head node for a regular linear search
		if ((sortedCount == 0) || (readKey(FIRST_ITEM).compareTo(key) >= 0))
			return HEAD_NODE;
		
		// TODO check last key to avoid binary search?
		
		int start = 0;
		int end = sortedCount;
		
		while (end - start > 1)
		{
			int curr = start + (end - start) / 2;
			
			if (readKey(curr * ORDER_SIZE + FIRST_ITEM).compareTo(key) >= 0)
				end = curr;
			else
				start = curr;
		}
		
		return start * ORDER_SIZE + FIRST_ITEM;
	}
	
	/** compacts and splits this chunk into new chunks, and returns the first.
	 * they are not "published" so can not be modified unless shared -
	 * use next pointers from returned chunk, untill null, to iterate over all created chunks */
	public Chunk<K,V> split(int myVersion, TreeSet<Integer> scans)
	{
		//if (!isFrozen.compareAndSet(false, true))
		//	return null;
		
		// freeze this chunk and get number of items in it
		int numItems = this.freeze();
		
		// create a first new chunk ("c1") and start adding items to it
		Chunk<K,V> c1 = newChunk(this.minKey);
		Chunk<K,V> destChunk = c1;
		
		// copy half of the items into created chunk c1
		// start iterating from first item
		int srcCurr = get(HEAD_NODE, OFFSET_NEXT);
		K prevKey = null;
		
		// remember prev node in destChunk, to easily add to its linked list
		int prevInDest = HEAD_NODE;
		int numAdded = 0;

		// iterate over scan versions, used in compaction
		Iterator<Integer> iterScans = null;
		int currScan = NONE;

		// iterate over this chunk's linked list
		while (srcCurr != NONE)
		{
			// get key & version for current item (updated prev to last key)
			K srcKey = readKey(srcCurr);
			int srcVer = getVersion(srcCurr);
			
			// determines whether we should add the current item, according to the following conditions
			// the other items we compact, as they are duplicates which we don't need
			boolean shouldAdd = false;
			boolean canSplit = false;	// can we split chunks on this item? (only if different than prev item)

			// if this item is different than previous (no prev, or prev has different key),
			// add it to new chunk, and reset scans iterator for it
			if ((prevKey == null) || (prevKey.compareTo(srcKey) != 0))
			{
				shouldAdd = true;
				canSplit = true;
				iterScans = scans.descendingIterator();
				currScan = (iterScans.hasNext() ? iterScans.next() : NONE);
			}
			// otherwise - if it is larger-or-equal to my version, or lesser-or-equal to current scan version - add it
			else if ((srcVer >= myVersion) || (srcVer <= currScan))
			{
				shouldAdd = true;
			}
			
			if (shouldAdd)
			{
				// progress scans iterator as needed
				while (srcVer <= currScan)
				{
					// we found an item with some version and are about to add it
					// this item might match several scans (i.e., be newest-version item lower than scan's version)
					// scans set is sorted so we proceed to the next-lower scan version until a scan version which does
					// not see the current item is found
					currScan = (iterScans.hasNext() ? iterScans.next() : NONE);
				}

				// if we added at least half of the items - split to new chunk
				// provided that we can split sfaely (canSplit is set if key is different from prevKey)
				if ((numAdded*2 >= numItems) && (canSplit))
				{
					Chunk<K,V> c2 = newChunk(srcKey);
					destChunk.next.set(c2, false);
					destChunk = c2;
					numAdded = 0;
					prevInDest = HEAD_NODE;
				}

				// get data of current item
				V srcVal = getData(srcCurr);
				
				// -- copy current item into destination chunk:
				// only add existing values
				// if data is null - it means no value (a deleted entry), no need to copy it
				if (srcVal != null)
				{
					// allocate space for item
					int oiDest = destChunk.allocate(srcKey, srcVal);
					
					// update to item's version (since allocation gives NONE version)
					// version is positive so item is marked as linked
					destChunk.set(oiDest, OFFSET_VERSION, srcVer);
					
					// updated dest chunk's linked list with new item
					destChunk.set(prevInDest, OFFSET_NEXT, oiDest);
					prevInDest = oiDest;
					
					// increase count of sorted items in dest chunk - for current item added
					++destChunk.sortedCount;
				}
			}
	
			// move to next item in this chunk's list (the frozen original chunk)
			srcCurr = get(srcCurr, OFFSET_NEXT);
			prevKey = srcKey;
			
			// count item as added no matter if it was actually added or not
			// half of original items go to 1 chunk, and half go to 2nd chunk
			++numAdded;
		}
		
		// set the last created chunk's next to this chunk's next
		destChunk.next.set(this.markAndGetNext(), false);
		return c1;
	}
	
	/** marks this chunk's next pointer so this chunk is marked as deleted
	 * @return the next chunk pointed to once marked (will not change) */
	public Chunk<K,V> markAndGetNext()
	{
		// new chunks are ready, we mark frozen chunk's next pointer so it won't change
		// since next pointer can be changed by other split operations we need to do this in a loop - until we succeed
		while (true)
		{
			// if chunk is marked - that is ok and its next pointer will not be changed anymore
			// return whatever chunk is set as next
			if (next.isMarked())
			{
				return next.getReference();
			}
			// otherwise try to mark it
			else
			{
				// read chunk's current next
				Chunk<K,V> savedNext = next.getReference();
				
				// try to mark next while keeping the same next chunk - using CAS
				// if we succeeded then the next pointer we remembered is set and will not change - return it
				if (next.compareAndSet(savedNext, savedNext, false, true))
					return savedNext;
			}
		}
	}
	
	/** freezes chunk so no more changes can be done in it. also marks pending items as frozen
	 * @return number of items in this chunk */
	private int freeze()
	{
		int numItems = 0;
		
		// mark all items as frozen (set version to FREEZE_VERSION)
		// this will cause pending put ops to fail, help split, and retry
		for (int idx = FIRST_ITEM; idx < orderArray.length; idx += ORDER_SIZE)
		{
			int version = getVersion(idx);
			
			// if item is frozen, ignore it - so only handle non-frozen items
			if (version != FREEZE_VERSION)
			{
				// if item has no version, try to freeze it
				if (version == NONE)
				{
					// set version to FREEZE so put op knows to restart
					// if succeded - item will not be in this chunk, we can continue to next item
					if ((cas(idx, OFFSET_VERSION, NONE, FREEZE_VERSION)) ||
						(getVersion(idx) == FREEZE_VERSION))
					{
						continue;
					}
				}

				// if we reached here then item has a version - we need to help by adding item to chunk's list
				// we need to help the pending put operation add itself to the list before proceeding
				// to make sure a frozen chunk is actually frozen - all items are fully added
				addToList(idx, readKey(idx));
				++numItems;
			}
		}
		return numItems;
	}
	
	/** finds and returns the index of the first item that is equal or larger-than the given min key
	 * with max version that is equal or less-than given version.
	 * returns NONE if no such key exists */
	public int findFirst(K minKey, int version)
	{
		// binary search sorted part of order-array to quickly find node to start search at
		// it finds previous-to-key so start with its next
		int curr = get(binaryFind(minKey), OFFSET_NEXT);
		
		// iterate until end of list (or key is found)
		while (curr != NONE)
		{
			K key = readKey(curr);

			// if item's key is larger or equal than min - we've found a matching key
			if (key.compareTo(minKey) >= 0)
			{
				// check for valid version
				if (getVersion(curr) <= version)
				{
					return curr;
				}
			}
			
			curr = get(curr, OFFSET_NEXT);
		}
		
		return NONE;
	}
	
	/** returns the index of the first item in this chunk with a version <= version */
	public int getFirst(int version)
	{
		int curr = get(HEAD_NODE, OFFSET_NEXT);
		
		// iterate over all items
		while (curr != NONE)
		{
			// if current item is of matching version - return it
			if (getVersion(curr) <= version)
			{
				return curr;
			}
			
			// proceed to next item
			curr = get(curr, OFFSET_NEXT);
		}
		return NONE;
	}
	
	public int findNext(int curr, int version, K key)
	{
		curr = get(curr, OFFSET_NEXT);
		
		while (curr != NONE)
		{
			K currKey = readKey(curr);
			
			// if in a valid version, and a different key - found next item
			if ((currKey.compareTo(key) != 0) && (getVersion(curr) <= version))
			{
				return curr;
			}
			
			// otherwise proceed to next
			curr = get(curr, OFFSET_NEXT);
		} 
		
		return NONE;
	}
	
	/** finds and returns the value for the given key, or 'null' if no such key exists */
	public V find(K key, PutData<K,V> item)
	{
		// binary search sorted part of order-array to quickly find node to start search at
		// it finds previous-to-key so start with its next
		int curr = get(binaryFind(key), OFFSET_NEXT);

		// iterate until end of list (or key is found)
		while (curr != NONE)
		{
			// compare current item's key to searched key
			int cmp = readKey(curr).compareTo(key);
			
			// if item's key is larger - we've exceeded our key
			// it's not in chunk - no need to search further
			if (cmp > 0)
				return null;
			// if keys are equal - we've found the item
			else if (cmp == 0)
				return chooseNewer(curr, item);
			// otherwise- proceed to next item
			else
				curr = get(curr, OFFSET_NEXT);
		}
		
		return null;
	}
	
	private V chooseNewer(int item, PutData<K,V> pd)
	{
		// if pd is empty or in different chunk, then item is definitely newer
		// it's true since put() publishes after finding a chunk, and get() finds chunk only after reading thread-array
		// so get() definitely sees the same chunks put() sees, or NEWER chunks
		if ((pd == null) || (pd.chunk != this))
			return getData(item);
		
		// if same chunk then regular comparison (version, then orderIndex)
		int itemVer = getVersion(item);
		int dataVer = getVersion(pd.orderIndex);

		if (itemVer > dataVer)
			return getData(item);
		else if (dataVer > itemVer)
			return getData(pd.orderIndex);
		else
			// same version - return latest item by order in order-array
			return getData(Math.max(item, pd.orderIndex));
	}
	
	/** add the given item (allocated in this chunk) to the chunk's linked list
	 * @param orderIndex index of item in order-array
	 * @param key given for convenience */
	public void addToList(int orderIndex, K key)
	{
		int prev, curr;
		
		// retry adding to list until successful
		// no items are removed from list - so we don't need to restart on failures
		// so if we CAS some node's next and fail, we can continue from it
		// --retry so long as version is negative (which means item isn't in linked-list)
		while (get(orderIndex, OFFSET_VERSION) < 0)
		{
			// remember next pointer in entry we're trying to add
			int savedNext = get(orderIndex, OFFSET_NEXT);
			
			// start iterating from quickly-found node (by binary search) in sorted part of order-array
			curr = binaryFind(key);
			//TODO can be improved - no need to restart search each time

			// iterate items until key's position is found
			while (true)
			{
				prev = curr;
				curr = get(prev, OFFSET_NEXT);	// index of next item in list
				
				// if no item, done searching - add to end of list
				if (curr == NONE)
					break;
				
				// if found item we're trying to insert - already inserted by someone else, so we're done
				if (curr == orderIndex)
					return;
					//TODO also update version to positive?
				
				// compare current item's key to ours
				int cmp = readKey(curr).compareTo(key);
				
				// if current item's key is larger, done searching - add between prev and curr
				if (cmp > 0)
					break;

				// if same key - check according to version and location in array
				if (cmp == 0)
				{
					// if duplicate values aren't allowed - do not add value
					if (!ALLOW_DUPS) {
						return;
					}

					int verMine = getVersion(orderIndex);
					int verNext = getVersion(curr);
					
					// if current item's version is smaller, done searching - larger versions are first in list
					if (verNext < verMine)
						break;
					
					// same versions but i'm later in chunk's array - i'm first in list
					if ((verNext == verMine) && (orderIndex > curr))
						break;
				}
			}
			
			// try to CAS update my next to current item ("curr" variable)
			// using CAS from saved next since someone else might help us
			// and we need to avoid race conditions with other put ops and helpers
			if (cas(orderIndex, OFFSET_NEXT, savedNext, curr))
			{
				// try to CAS curr's next to point from "next" to me
				// if successful - we're done, exit loop. Otherwise retry (return to "while true" loop)
				if (cas(prev, OFFSET_NEXT, curr, orderIndex))
				{
					// if some CAS failed we restart, if both successful - we're done
					// update version to positive (getVersion() always returns positive number) to mark item is linked
					set(orderIndex, OFFSET_VERSION, getVersion(orderIndex));
					break;
				}
			}
		}
	}
	
	/** base allocate method for use in allocation by implementing classes
	 * @return index of allocated order-array item (can be used to get data-array index) */
	protected int baseAllocate(int dataSize)
	{
		// increment order array to get new index in it
		int oi = orderIndex.getAndAdd(ORDER_SIZE);
		if (oi+ORDER_SIZE > orderArray.length)
			return -1;
		
		// if there's data - allocate room for it
		// otherwise DATA field of order-item is left as NONE
		if (dataSize > 0)
		{
			// increment data array to get new index in it
			int di = dataIndex.getAndAdd(dataSize);
			if (di+dataSize > dataArray.length)
				return -1;
	
			// write base item data location (offset of data-array) to order-array
			// since NONE==0, item's version and next are already set to NONE
			set(oi, OFFSET_DATA, di);
		}
		
		// return index of allocated order-array item
		return oi;
	}
	
	/** gets the current version of the given order-item */
	public int getVersion(int orderIndex)
	{
		return Math.abs(get(orderIndex, OFFSET_VERSION));
	}
	/** tries to set (CAS) the version of order-item to specified version
	 * @return whatever version is successfuly set (by this thread or another)	 */
	public int setVersion(int orderIndex, int version)
	{		
		// try to CAS version from NO_VERSION to desired version
		if (cas(orderIndex, OFFSET_VERSION, NONE, -version))
			return version;
		// if failed (someone else's CAS succeeded) - read version again and return it
		else
			return getVersion(orderIndex);
	}
	
	public int debugCountKeys()
	{
		int curr = get(HEAD_NODE, OFFSET_NEXT);
		int prev = curr;
		int keys = 0;
		
		if (curr != NONE)
		{
			keys = 1;
			curr = get(curr,OFFSET_NEXT);
			
			while (curr != NONE)
			{
				if (readKey(curr).compareTo(readKey(prev)) != 0)
					++keys;
				prev = curr;
				curr = get(curr,OFFSET_NEXT);
			}
		}
		return keys;
	}
	public int debugCountDups()
	{
		int curr = get(HEAD_NODE, OFFSET_NEXT);
		int prev = curr;
		int dups = 0;
		
		if (curr != NONE)
		{
			curr = get(curr,OFFSET_NEXT);
			
			while (curr != NONE)
			{
				if (readKey(curr).compareTo(readKey(prev)) == 0)
					++dups;
				prev = curr;
				curr = get(curr,OFFSET_NEXT);
			}
		}
		return dups;
	}
	public void debugPrint()
	{
		System.out.print(this.minKey + " :: ");
		int curr = get(HEAD_NODE, OFFSET_NEXT);
		
		while (curr != NONE)
		{
			System.out.print("(" + readKey(curr) + "," + getData(curr) + "," + curr + ") ");
			curr = get(curr, OFFSET_NEXT);
		}
	}
}
