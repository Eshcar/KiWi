package contention.benchmark;

import contention.abstractions.CompositionalMap;
import util.IntegerGenerator;
import util.Pair;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * The loop executed by each thread of the map 
 * benchmark.
 * 
 * @author Vincent Gramoli
 * 
 */
public class ThreadLoop implements Runnable {

	private final int avgKeysReturned;
	private final IntegerGenerator coinGenerator;
	/** The instance of the running benchmark */
	public CompositionalMap<Integer, Integer> bench;
	/** The stop flag, indicating whether the loop is over */
	protected volatile boolean stop = false;
	/** The pool of methods that can run */
	protected Method[] methods;
	/** The number of the current thread */
	protected final short myThreadNum;

	/** The counters of the thread successful operations */
	public long numAdd = 0;
	public long numRemove = 0;
	public long numAddAll = 0;
	public long numRemoveAll = 0;
	public long numSize = 0;
	public long numContains = 0;
	/** The counter of the false-returning operations */
	public long failures = 0;
	/** The counter of the thread operations */
	public long total = 0;
	/** The counter of aborts */
	public long aborts = 0;
	/** The random number */
	IntegerGenerator keysGenerator;
	Random rand = new Random();
	
	public long scanTimeTotal = 0;
	public long scanCount = 0;
	public long scanKeysCount = 0;
	public long scanTimeMax = 0;
	public long scanTimeSquareSum = 0;

	public long putTimeTotal = 0;
	public long putCount = 0;
	public long putTimeMax = 0;
	public long putTimeSquareSum = 0;

	public long getTimeTotal = 0;
	public long getTimeSquareSum = 0;
	public long getCount;

	public long nodesTraversed;
	public long structMods;

	public int minRangeSize;
	public int maxRangeSize;

	public int scanThreads = 0;
	public int putThreads = 0;
	 

	/**
	 * The distribution of methods as an array of percentiles
	 * 
	 * 0%        cdf[0]        cdf[2]                     100%
	 * |--writeAll--|--writeSome--|--readAll--|--readSome--|
	 * |-----------write----------|--readAll--|--readSome--| cdf[1]
	 */
	int[] cdf = new int[3];

	public ThreadLoop(short myThreadNum,
			CompositionalMap<Integer, Integer> bench, Method[] methods) {
		this.myThreadNum = myThreadNum;
		this.bench = bench;
		this.methods = methods;
		this.minRangeSize = Parameters.minRangeSize;
		this.maxRangeSize = Parameters.maxRangeSize;
		this.keysGenerator = Parameters.distribution.createGenerator(0,Parameters.range);
		this.coinGenerator = Parameters.KeyDistribution.Uniform.createGenerator(0,Parameters.range);

		this.avgKeysReturned = Math.max((minRangeSize+maxRangeSize)/4, 1);

		/* initialize the method boundaries */
		assert (Parameters.numWrites >= Parameters.numWriteAlls);

		int wsRange = (int)((double)Parameters.range*(Parameters.numWrites + Parameters.numSnapshots)/100);
		int writesRange = (int)(wsRange/(1+ Parameters.numSnapshots/((double)avgKeysReturned*Parameters.numWrites)));

		cdf[0] = (int)(((double)Parameters.numWriteAlls/100) * Parameters.range);
		cdf[1] = cdf[0] + writesRange;
		cdf[2] = cdf[1] + wsRange - writesRange;


		if(Parameters.rangeQueries){
			int numThreads = Parameters.numThreads; 
			if (numThreads == 1){
				return; 
			}
			if(myThreadNum < getScanThreadsNum() ){
				//do snapshot
				cdf[0]= 0;
				cdf[1]= 0;
				cdf[2]= Parameters.range;

				scanThreads=1;
			}else{
				//do update
				cdf[0]= 0;
				cdf[1]= Parameters.range;
				cdf[2]= 0;

				putThreads=1;
			}
		}

	}

	private int getScanThreadsNum() {
		return Math.max(1,Parameters.numSnapshots*Parameters.numThreads/100);
	}


	public void stopThread() {
		stop = true;
	}

	public void printDataStructure() {
		System.out.println(bench.toString());
	}

	public void run() {
		Integer [] rangeResult = new Integer[maxRangeSize*4 + 1];

		while (!stop) {
			int newInt = keysGenerator.nextInt();
			
			int coin = coinGenerator.nextInt();
			if (coin < cdf[0]) { // 1. should we run a writeAll operation?

				// init a collection
				Map<Integer, Integer> hm = new HashMap<Integer, Integer>(newInt, newInt);
				hm.put(newInt / 2, newInt / 2); // accepts duplicate

				try {
					if (bench.keySet().removeAll(hm.keySet()))
						numRemoveAll++; 
					else failures++; 
				} catch (Exception e) {
					System.err.println("Unsupported writeAll operations! Leave the default value of the numWriteAlls parameter (0).");
				}
				 

			} else if (coin < cdf[1]) { // 2. should we run a writeSome
										// operation?

				long putTime = System.nanoTime();
				
				if (2 * (coin - cdf[0]) < cdf[1] - cdf[0]) { // add
				//if (true) {
					if ((bench.put((int) newInt, (int) newInt)) == null) {
						numAdd++;
					} else {
						failures++;
					}
				} else { // remove
					if ((bench.remove((int) newInt)) != null) {
						numRemove++;
					} else{
						failures++;
					}
				}
				
				putTime = System.nanoTime() - putTime;
				putTimeTotal += putTime;
				putTimeSquareSum += putTime*putTime;

				++putCount;
				putTimeMax = Math.max(putTime, putTimeMax);


			} else if (coin < cdf[2]) { // 3. should we run a readAll operation?

				Pair<Integer, Integer> interval = keysGenerator.nextInterval();
				int min = interval.getLeft();
				int max = interval.getRight();

				long scanTime = System.nanoTime();

				int numKeys = bench.getRange(rangeResult,min,max);
				scanTime = System.nanoTime() - scanTime;

				scanTimeTotal += scanTime;
				scanTimeSquareSum += scanTime*scanTime;

				++scanCount;
				scanTimeMax = Math.max(scanTime, scanTimeMax);
				
				if (Parameters.countKeysInRange) {
					numSize += numKeys;
					scanKeysCount += numKeys;
					//total += numKeys-1; // since total is incremented later
				}
				else {
					++numSize;
				}

			} else { // 4. then we should run a readSome operation

				long readTime = System.nanoTime();
				if (bench.get((int) newInt) != null)
					numContains++;
				else
					failures++;

				readTime = System.nanoTime() - readTime;
				getCount++;
				getTimeTotal += readTime;
				getTimeSquareSum += readTime*readTime;
			}
			total++;

			//assert total == failures + numContains + numSize + numRemove
			//		+ numAdd + numRemoveAll + numAddAll;
		}
		// System.out.println(numAdd + " " + numRemove + " " + failures);
		//this.getCount = CompositionalMap.counts.get().getCount;
		this.nodesTraversed = CompositionalMap.counts.get().nodesTraversed;
		this.structMods = CompositionalMap.counts.get().structMods;
		System.out.println("Thread #" + myThreadNum + " finished.");
	}

}
