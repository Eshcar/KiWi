package contention.benchmark;

import contention.abstractions.CompositionalIntSet;
import contention.abstractions.CompositionalMap;
import contention.abstractions.CompositionalSortedSet;
import contention.abstractions.MaintenanceAlg;
import kiwi.Rebalancer;
import util.CombinedGenerator;
import util.UniqueRandomIntGenerator;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Synchrobench-java, a benchmark to evaluate the implementations of 
 * high level abstractions including Map and Set.
 * 
 * @author Vincent Gramoli
 * 
 */
public class Test {

	public static final String VERSION = "11-17-2014";
	
	public enum Type {
	    INTSET, MAP, SORTEDSET
	}

	/** The array of threads executing the benchmark */
	private Thread[] threads;
	/** The array of runnable thread codes */
	private ThreadLoop[] threadLoops;
	private ThreadSetLoop[] threadLoopsSet;
	private ThreadSortedSetLoop[] threadLoopsSSet;
	/** The observed duration of the benchmark */
	private double elapsedTime;
	private double fillTime;
	/** The throughput */
	private double throughput = 0;

	/** The total number of operations for all threads */
	private long total = 0;
	/** The total number of successful operations for all threads */
	private int numAdd = 0;
	private int numRemove = 0;
	private int numAddAll = 0;
	private int numRemoveAll = 0;
	private int numSize = 0;
	private int numContains = 0;
	/** The total number of failed operations for all threads */
	private int failures = 0;
	
	private long scanTimeMax = 0;
	private long scanTimeTotal = 0;
	private int scanCount=0;
	private long scanKeysCount =0;
	private long scanTimeSquareSum = 0;

	private long putTimeMax = 0;
	private long putTimeTotal = 0;
	private int putCount;
	public long putTimeSquareSum = 0;

	public long readTimeTotal = 0;
	public long readCount = 0;
	public long readTimeSquareSum = 0;

	public int putThreads = 0;
	public int scanThreads = 0;
	
	/** The total number of aborts */
	//private int aborts = 0;
	/** The instance of the benchmark */
	private Type benchType = null;
	private CompositionalIntSet setBench = null;
	private CompositionalSortedSet<Integer> sortedBench = null;
	private CompositionalMap<Integer, Integer> mapBench = null;
	ConcurrentHashMap<Integer, Integer> map = null;
	/** The instance of the benchmark */
	/** The benchmark methods */
	private Method[] methods;

	private long nodesTraversed;
	public long structMods;
	private long getCount;
	private long getTimeTotal = 0;
	private long getTimeSquareSum = 0;


	/** The thread-private PRNG */
	final private static ThreadLocal<Random> s_random = new ThreadLocal<Random>() {
		@Override
		protected synchronized Random initialValue() {
			return new Random();
		}
	};

	public interface Filler
	{
		void fill(final int range, final long size);
	}

	private class FillerFactory
	{
		public Filler createFiller()
		{
			switch (Parameters.fillType)
			{
				case Random:
					return new RandomFillerNoDups();
				case Ordered:
					return new OrderedFiller();
				case DropHalf:
					return new DropHalfFiller();
				case Drop90:
					return new Drop90Filler();

				default:
					return new RandomFiller();
			}
		}
	}

	private class RandomFillerNoDups implements Filler
	{

		@Override
		public void fill(final int range, final long size) {
			int[] keys = new int[range];
			int usedIdx = keys.length -1;

			// init random source array
			for(int i = 0; i < keys.length; ++i)
			{
				keys[i] = i;
			}

			// fill full data structure
			for(int i = 0; i < size; ++i)
			{
				int nextIdx = s_random.get().nextInt(usedIdx +1);
				int nextKey = keys[nextIdx];

				int tmp = keys[usedIdx];
				keys[usedIdx] = nextKey;
				keys[nextIdx] = tmp;

				usedIdx--;

				mapBench.put((Integer)nextKey, (Integer)nextKey);
			}

		}
	}
	private class RandomFiller implements Filler
	{

		@Override
		public void fill(final int range, final long size) {
			for (long i = size; i > 0;) {
				Integer v = s_random.get().nextInt(range);
				switch(benchType) {
					case INTSET:
						if (setBench.addInt(v)) {
							i--;
						}
						break;
					case MAP:
						if (mapBench.putIfAbsent((Integer) v, (Integer) v) == null) {
							i--;
						}
						break;
					case SORTEDSET:
						if (sortedBench.add((Integer) v)) {
							i--;
						}
						break;
					default:
						System.err.println("Wrong benchmark type");
						System.exit(0);
				}
			}
		}
	}

	private class OrderedFiller implements Filler{

		@Override
		public void fill(final int range, final long size) {

			if(range < size ) throw new IllegalStateException("Keys range is less than data structure size");
			int ratio = (int)(range/size);
			int currKey = 0;

			for(long i = 0; i < size; ++i)
			{
				mapBench.put((Integer)currKey, (Integer)currKey);
				currKey+= 1+s_random.get().nextInt(ratio);
			}
		}
	}

	private class Drop90Filler implements  Filler {

		@Override
		public void fill(int range, long size)
		{
			ArrayList<Integer> prefixValues = new ArrayList<>((int)size);
			UniqueRandomIntGenerator ug = new UniqueRandomIntGenerator(0,range);

			for(int i =0 ; i < size; ++i)
			{
				assert ug.hasNext();

				prefixValues.add(ug.next());
			}

			// fill random values
			CombinedGenerator cg = new CombinedGenerator(prefixValues);

			while(cg.hasNext())
			{
				int val = cg.next();
				mapBench.put(val,val);
			}


			// delete 90% of vals
			cg.reset();

			final int bitmask = createBitMask(cg.getShift());
			Set<Integer> removedKeys = new HashSet<>();

			while(cg.hasNext())
			{
				int key =  cg.next();
				if((key & bitmask) != 0)
				{
					assert !removedKeys.contains(key);

					removedKeys.add(key);

					mapBench.remove(key);
				}
			}

			int count = removedKeys.size();

			assert count == size*9;
		}

		private int createBitMask(int shift)
		{
			int bitmask = 0;
			for(int i = 0; i < shift; ++i)
			{
				bitmask = bitmask << 1;
				bitmask = bitmask + 1;
			}
			return bitmask;
		}
	}

	private class DropHalfFiller implements Filler{


		protected void put(Integer key, Integer val)
		{
			mapBench.put(key, val);
		}

		protected void remove(Integer key)
		{
			mapBench.remove(key);
		}


		@Override
		public void fill(final int range, final long size) {
			if(range < size) throw new IllegalStateException("Keys range is less than data structure size");

			int[] keys = new int[range];
			int usedIdx = keys.length -1;

			// init random source array
			for(int i = 0; i < keys.length; ++i)
			{
				keys[i] = i;
			}

			// fill full data structure
			for(int i = 0; i < range; ++i)
			{
				int nextIdx = s_random.get().nextInt(usedIdx +1);
				int nextKey = keys[nextIdx];

				int tmp = keys[usedIdx];
				keys[usedIdx] = nextKey;
				keys[nextIdx] = tmp;

				usedIdx--;

				put((Integer)nextKey, (Integer)nextKey);
			}


			usedIdx = keys.length -1;

			// remove random keys
			for(int i = 0; i < range - size; ++i)
			{
				int nextIdx = s_random.get().nextInt(usedIdx +1);
				int nextKey = keys[nextIdx];

				int tmp = keys[usedIdx];
				keys[usedIdx] = nextKey;
				keys[nextIdx] = tmp;

				usedIdx--;

				remove((Integer)nextKey);

			}
		}

	}

	private class DropHalfProfiledFiller extends DropHalfFiller
	{
		int opsPerSnapshot;
		int totalOps;

		private void checkForSnapshot()
		{
			if(mapBench instanceof kiwi.KiWiMap)
			{
				if(totalOps % opsPerSnapshot == 0)
				{
					((kiwi.KiWiMap)mapBench).calcChunkStatistics();
				}
			}

		}
		@Override
		protected void put(Integer key, Integer val)
		{
			super.put(key,val);
			totalOps++;

			checkForSnapshot();
		}

		@Override
		protected void remove(Integer key)
		{
			super.remove(key);
			totalOps++;

			checkForSnapshot();
		}

		@Override
		public void fill(final int range, final long size)
		{
			opsPerSnapshot = (int)size/20;
			totalOps = 0;

			super.fill(range, size);
		}
	}

	public void fill(final int range, final long size) {
		for (long i = size; i > 0;) {
			Integer v = s_random.get().nextInt(range);
			switch(benchType) {
			case INTSET:
				if (setBench.addInt(v)) {
					i--;
				}
				break;
			case MAP:
				if (mapBench.putIfAbsent((Integer) v, (Integer) v) == null) {
					i--;
				}	
				break;
			case SORTEDSET:
				if (sortedBench.add((Integer) v)) {
					i--;
				}	
				break;
			default:
				System.err.println("Wrong benchmark type");
				System.exit(0);
			}
		}
	}


	/**
	 * Initialize the benchmark
	 * 
	 * @param benchName
	 *            the class name of the benchmark
	 * @return the instance of the initialized corresponding benchmark
	 */
	@SuppressWarnings("unchecked")
	public void instanciateAbstraction(
			String benchName) {
		try {
			Class<CompositionalMap<Integer, Integer>> benchClass = (Class<CompositionalMap<Integer, Integer>>) Class
					.forName(benchName);
			Constructor<CompositionalMap<Integer, Integer>> c = benchClass
					.getConstructor();
			methods = benchClass.getDeclaredMethods();
			
			if (CompositionalIntSet.class.isAssignableFrom((Class<?>) benchClass)) {
				setBench = (CompositionalIntSet)c.newInstance();
				benchType = Type.INTSET;
			} else if (CompositionalMap.class.isAssignableFrom((Class<?>) benchClass)) {
				mapBench = (CompositionalMap<Integer, Integer>) c.newInstance();
				benchType = Type.MAP;
			} else if (CompositionalSortedSet.class.isAssignableFrom((Class<?>) benchClass)) {
				sortedBench = (CompositionalSortedSet<Integer>) c.newInstance();
				benchType = Type.SORTEDSET;
			}
			
		} catch (Exception e) {
			System.err.println("Cannot find benchmark class: " + benchName + ". Exception: " + e.toString());
			System.exit(-1);
		}
	}
	

	/**
	 * Creates as many threads as requested
	 * 
	 * @throws InterruptedException
	 *             if unable to launch them
	 */
	private void initThreads() throws InterruptedException {
		switch(benchType) {
		case INTSET:
			threadLoopsSet = new ThreadSetLoop[Parameters.numThreads];
			threads = new Thread[Parameters.numThreads];
			for (short threadNum = 0; threadNum < Parameters.numThreads; threadNum++) {
				threadLoopsSet[threadNum] = new ThreadSetLoop(threadNum, setBench, methods);
				threads[threadNum] = new Thread(threadLoopsSet[threadNum]);
			}
			break;
		case MAP:
			threadLoops = new ThreadLoop[Parameters.numThreads];
			threads = new Thread[Parameters.numThreads];
			for (short threadNum = 0; threadNum < Parameters.numThreads; threadNum++) {
				threadLoops[threadNum] = new ThreadLoop(threadNum, mapBench, methods);
				threads[threadNum] = new Thread(threadLoops[threadNum]);
			}
			break;
		case SORTEDSET:
			threadLoopsSSet = new ThreadSortedSetLoop[Parameters.numThreads];
			threads = new Thread[Parameters.numThreads];
			for (short threadNum = 0; threadNum < Parameters.numThreads; threadNum++) {
				threadLoopsSSet[threadNum] = new ThreadSortedSetLoop(threadNum, sortedBench, methods);
				threads[threadNum] = new Thread(threadLoopsSSet[threadNum]);
			}
			break;
		}
	}

	/**
	 * Constructor sets up the benchmark by reading parameters and creating
	 * threads
	 * 
	 * @param args
	 *            the arguments of the command-line
	 */
	public Test(String[] args) throws InterruptedException {
		printHeader();
		try {
			parseCommandLineParameters(args);
		} catch (Exception e) {
			System.err.println("Cannot parse parameters.");
			e.printStackTrace();
		}
		instanciateAbstraction(Parameters.benchClassName);
	}

	/**
	 * Execute the main thread that starts and terminates the benchmark threads
	 * 
	 * @throws InterruptedException
	 */
	private void execute(int milliseconds, boolean maint)
			throws InterruptedException {
		long startTime;

		if(milliseconds == 0) return;

		FillerFactory factory = new FillerFactory();

		startTime = System.currentTimeMillis();

		factory.createFiller().fill(Parameters.range, Parameters.size);

		fillTime = ((double)System.currentTimeMillis() - startTime)/1000.0;
/*
		if(threadLoops[0].bench instanceof kiwi.KiWiMap)
		{
			kiwi.KiWiMap kmap = ((kiwi.KiWiMap)threadLoops[0].bench);
			//kmap.compactAllSerial();
			System.out.println("After fill statistics");
			kmap.calcChunkStatistics();
		}
*/

		Thread.sleep(5000);

		startTime = System.currentTimeMillis();
		for (Thread thread : threads)
			thread.start();
		try {
			Thread.sleep(milliseconds);
		} finally {
			switch(benchType) {
			case INTSET:
				for (ThreadSetLoop threadLoop : threadLoopsSet)
					threadLoop.stopThread();
				break;
			case MAP:
				for (ThreadLoop threadLoop : threadLoops)
					threadLoop.stopThread();
				break;
			case SORTEDSET:
				for (ThreadSortedSetLoop threadLoop : threadLoopsSSet)
					threadLoop.stopThread();
				break;
			}
		}
		for (Thread thread : threads)
			thread.join();




		long endTime = System.currentTimeMillis();
		elapsedTime = ((double) (endTime - startTime)) / 1000.0;
/*
		if(threadLoops[0].bench instanceof kiwi.KiWiMap) {
			kiwi.KiWiMap kmap = ((kiwi.KiWiMap) threadLoops[0].bench);
			//kmap.compactAllSerial();
			System.out.println("After run chunks statistics not compacted:");
			kmap.calcChunkStatistics();

			System.out.println("After first compaction: ");
			kmap.compactAllSerial();
			kmap.calcChunkStatistics();

		}
		*/
	}

	public void clear() {
		switch(benchType) {
		case INTSET:
			setBench.clear();
			break;
		case MAP:
			mapBench.clear();
			break;
		case SORTEDSET:
			sortedBench.clear();
			break;
		}
	}

	public static void main(String[] args) throws InterruptedException {
		runTest(args);
	}
	public static CompositionalMap<Integer,Integer> runTest(String[] args) throws InterruptedException
	{
		boolean firstIteration = true;
		Test test = new Test(args);
		test.printParams();

		// warming up the JVM
		if (Parameters.warmUp != 0) {
			try {
				test.initThreads();
			} catch (Exception e) {
				System.err.println("Cannot launch operations.");
				e.printStackTrace();
			}
			test.execute(Parameters.warmUp * 1000, true);
			// give time to the JIT
			Thread.sleep(1000);
			if (Parameters.detailedStats)
				test.recordPreliminaryStats();
			test.clear();
			test.resetStats();
		}

		// running the bench
		for (int i = 0; i < Parameters.iterations; i++) {
			if (!firstIteration) {
				// give time to the JIT
				Thread.sleep(1000);
				test.resetStats();
				test.clear();
				org.deuce.transaction.estmstats.Context.threadIdCounter.set(0);
			}
			try {
				test.initThreads();
			} catch (Exception e) {
				System.err.println("Cannot launch operations.");
				e.printStackTrace();
			}
			test.execute(Parameters.numMilliseconds, false);

			if (test.setBench instanceof MaintenanceAlg) {
				((MaintenanceAlg) test.setBench).stopMaintenance();
				test.structMods += ((MaintenanceAlg) test.setBench)
						.getStructMods();
			}
			if (test.mapBench instanceof MaintenanceAlg) {
				((MaintenanceAlg) test.mapBench).stopMaintenance();
				test.structMods += ((MaintenanceAlg) test.mapBench)
						.getStructMods();
			}
			if (test.sortedBench instanceof MaintenanceAlg) {
				((MaintenanceAlg) test.sortedBench).stopMaintenance();
				test.structMods += ((MaintenanceAlg) test.sortedBench)
						.getStructMods();
			}

			test.printBasicStats();
			if (Parameters.detailedStats)
				test.printDetailedStats();
			firstIteration = false;
		}
		return test.mapBench;
	}

	/* ---------------- Input/Output -------------- */

	/**
	 * Parse the parameters on the command line
	 */
	private void parseCommandLineParameters(String[] args) throws Exception {
		int argNumber = 0;

		while (argNumber < args.length) {
			String currentArg = args[argNumber++];

			try {
				if (currentArg.equals("--help") || currentArg.equals("-h")) {
					printUsage();
					System.exit(0);
				} else if (currentArg.equals("--verbose")
						|| currentArg.equals("-v")) {
					Parameters.detailedStats = true;
				} else {
					String optionValue = args[argNumber++];
					if (currentArg.equals("--thread-nums")
							|| currentArg.equals("-t")) {
						Parameters.numThreads = Integer.parseInt(optionValue);
						kiwi.KiWi.MAX_THREADS = Integer.parseInt(optionValue);
						kiwilocal.KiWi.MAX_THREADS = Integer.parseInt(optionValue);
						kiwisplit.KiWi.MAX_THREADS = Integer.parseInt(optionValue);
					}
					else if (currentArg.equals("--duration")
							|| currentArg.equals("-d")) {
						Parameters.numMilliseconds = Integer
								.parseInt(optionValue);
					}
					else if (currentArg.equals("--updates")
							|| currentArg.equals("-u")) {
						Parameters.numWrites = Integer.parseInt(optionValue);
					}
					else if (currentArg.equals("--writeAll")
							|| currentArg.equals("-a")) {
						Parameters.numWriteAlls = Integer.parseInt(optionValue);
					}
					else if (currentArg.equals("--snapshots")
							|| currentArg.equals("-s")) {
						Parameters.numSnapshots = Integer.parseInt(optionValue);
					}
					else if (currentArg.equals("--size")
							|| currentArg.equals("-i")) {
						Parameters.size = Integer.parseInt(optionValue);
					}
					else if (currentArg.equals("--range")
							|| currentArg.equals("-r")) {
						Parameters.range = Integer.parseInt(optionValue);
					}
					else if (currentArg.equals("--Warmup")
							|| currentArg.equals("-W")) {
						Parameters.warmUp = Integer.parseInt(optionValue);
					}
					else if (currentArg.equals("--benchmark")
							|| currentArg.equals("-b")) {
						Parameters.benchClassName = optionValue;
					}
					else if (currentArg.equals("--iterations")
							|| currentArg.equals("-n")) {
						Parameters.iterations = Integer.parseInt(optionValue);
					}
					else if (currentArg.equals("--range")
							|| currentArg.equals("-R")) {
						Parameters.rangeQueries = Boolean.parseBoolean(optionValue);
					}
					else if (currentArg.equals("--countKeys")
							|| currentArg.equals("-K")) {
						Parameters.countKeysInRange = Boolean.parseBoolean(optionValue);
					}
					else if (currentArg.equals("--maxRangeSize")
							|| currentArg.equals("-MR")) {
						Parameters.maxRangeSize = Integer.parseInt(optionValue);
					}
					else if (currentArg.equals("--minRangeSize")
							|| currentArg.equals("-mR")) {
						Parameters.minRangeSize = Integer.parseInt(optionValue);
					}
					else if (currentArg.equals("--kiwiItems")
							|| currentArg.equals("-it")) {
						kiwi.Chunk.MAX_ITEMS = Integer.parseInt(optionValue);
						kiwilocal.Chunk.MAX_ITEMS = Integer.parseInt(optionValue);
						kiwisplit.Chunk.MAX_ITEMS = Integer.parseInt(optionValue);
					}
					else if (currentArg.equals(("-rebProb")))
					{
						Parameters.rebalanceProbPerc = Double.parseDouble(optionValue);
					}
					else if (currentArg.equals("-rebRatio"))
					{
						Parameters.sortedRebalanceRatio = Double.parseDouble(optionValue);
					}
					else if (currentArg.equalsIgnoreCase("-fillType"))
					{
						if(optionValue.equalsIgnoreCase("ordered")) {
							Parameters.fillType = Parameters.FillType.Ordered;
						} else if(optionValue.equalsIgnoreCase("drophalf")){
							Parameters.fillType = Parameters.FillType.DropHalf;
						}
						else if(optionValue.equalsIgnoreCase("drop90")){
							Parameters.fillType = Parameters.FillType.Drop90;
						}
						else
						{
							Parameters.fillType = Parameters.FillType.Random;
						}
					}
					else if(currentArg.equalsIgnoreCase("-afterMergeRatio"))
					{
						Rebalancer.MAX_AFTER_MERGE_PART = Double.parseDouble(optionValue);
					}
					else if (currentArg.equals("--rebalanceSize")
							|| currentArg.equals("-rbSz")){
						kiwi.KiWiMap.RebalanceSize = Integer.parseInt(optionValue);
						kiwilocal.KiWiMap.RebalanceSize = Integer.parseInt(optionValue);

					}
					else if (currentArg.equals("--kiwiDistribution")
							|| currentArg.equals("-kDistr")) {
						if (optionValue.equalsIgnoreCase("zipfian")) {
							Parameters.distribution = Parameters.KeyDistribution.Zipfian;
						} else if(optionValue.equalsIgnoreCase("Churn10Perc")) {
							Parameters.distribution = Parameters.KeyDistribution.Churn10Perc;

						} else if(optionValue.equals("Churn5Perc"))
						{
							Parameters.distribution = Parameters.KeyDistribution.Churn5Perc;
						}
						else if(optionValue.equals("Churn1Perc"))
						{
							Parameters.distribution = Parameters.KeyDistribution.Churn1Perc;
						}
						else if (optionValue.equalsIgnoreCase("shifted"))
						{
							Parameters.distribution = Parameters.KeyDistribution.ShiftedUniform;
						}
						else
						{
							Parameters.distribution = Parameters.KeyDistribution.Uniform;
						}
					}
				/*	else if (currentArg.equals("--kiwiDups")
							|| currentArg.equals("-kd")) {
						Chunk.ALLOW_DUPS = Boolean.parseBoolean(optionValue);
					}
					else if (currentArg.equals("--kiwiPool")
							|| currentArg.equals("-kp")) {
						ChunkInt.setPoolSize(Integer.parseInt(optionValue));
					}
					*/
					else if (currentArg.equals("--large-range")){
						Parameters.rangeQueries = Boolean.parseBoolean(optionValue);
						if (Boolean.parseBoolean(optionValue)){
							Parameters.minRangeSize = 1000;
							Parameters.maxRangeSize = 2000;
						}
					}
				}
			} catch (IndexOutOfBoundsException e) {
				System.err.println("Missing value after option: " + currentArg
						+ ". Ignoring...");
			} catch (NumberFormatException e) {
				System.err.println("Number expected after option:  "
						+ currentArg + ". Ignoring...");
			}
		}
		assert (Parameters.range >= Parameters.size);
		if (Parameters.range != 2 * Parameters.size)
			System.err
					.println("Note that the value range is not twice "
							+ "the initial size, thus the size expectation varies at runtime.");
	}

	/**
	 * Print a 80 character line filled with the same marker character
	 * 
	 * @param ch
	 *            the marker character
	 */
	private void printLine(char ch) {
		StringBuffer line = new StringBuffer(79);
		for (int i = 0; i < 79; i++)
			line.append(ch);
		System.out.println(line);
	}

	/**
	 * Print the header message on the standard output
	 */
	private void printHeader() {
		String header = "Synchrobench-java\n"
				+ "A benchmark-suite to evaluate synchronization techniques";
		printLine('-');
		System.out.println(header);
		printLine('-');
		System.out.println();
	}

	/**
	 * Print the benchmark usage on the standard output
	 */
	private void printUsage() {
		String syntax = "Usage:\n"
				+ "java synchrobench.benchmark.Test [options] [-- stm-specific options]\n\n"
				+ "Options:\n"
				+ "\t-v            -- print detailed statistics (default: "
				+ Parameters.detailedStats
				+ ")\n"
				+ "\t-t thread-num -- set the number of threads (default: "
				+ Parameters.numThreads
				+ ")\n"
				+ "\t-d duration   -- set the length of the benchmark, in milliseconds (default: "
				+ Parameters.numMilliseconds
				+ ")\n"
				+ "\t-u updates    -- set the number of updates (default: "
				+ Parameters.numWrites
				+ ")\n"
				+ "\t-a writeAll   -- set the percentage of composite updates (default: "
				+ Parameters.numWriteAlls
				+ ")\n"
				+ "\t-s snapshot   -- set the percentage of composite read-only operations (default: "
				+ Parameters.numSnapshots
				+ ")\n"
				+ "\t-r range      -- set the element range (default: "
				+ Parameters.range
				+ ")\n"
				+ "\t-b benchmark  -- set the benchmark (default: "
				+ Parameters.benchClassName
				+ ")\n"
				+ "\t-i size       -- set the datastructure initial size (default: "
				+ Parameters.size
				+ ")\n"
				+ "\t-n iterations -- set the bench iterations in the same JVM (default: "
				+ Parameters.iterations
				+ ")\n"
				+ "\t-W warmup     -- set the JVM warmup length, in seconds (default: "
				+ Parameters.warmUp + ").";
		System.err.println(syntax);
	}

	/**
	 * Print the parameters that have been given as an input to the benchmark
	 */
	private void printParams() {
		String params = "Benchmark parameters" + "\n" + "--------------------"
				+ "\n" + "  Detailed stats:          \t"
				+ (Parameters.detailedStats ? "enabled" : "disabled")
				+ "\n"
				+ "  Number of threads:       \t"
				+ Parameters.numThreads
				+ "\n"
				+ "  Length:                  \t"
				+ Parameters.numMilliseconds
				+ " ms\n"
				+ "  Write ratio:             \t"
				+ Parameters.numWrites
				+ " %\n"
				+ "  WriteAll ratio:          \t"
				+ Parameters.numWriteAlls
				+ " %\n"
				+ "  Snapshot ratio:          \t"
				+ Parameters.numSnapshots
				+ " %\n"
				+ "  Size:                    \t"
				+ Parameters.size
				+ " elts\n"
				+ "  Range:                   \t"
				+ Parameters.range
				+ " elts\n"
				+ "  WarmUp:                  \t"
				+ Parameters.warmUp
				+ " s\n"
				+ "  Iterations:              \t"
				+ Parameters.iterations
				+ "\n"
				+ "  Benchmark:               \t"
				+ Parameters.benchClassName;
		System.out.println(params);
	}

	/**
	 * Print the statistics on the standard output
	 */
	private void printBasicStats() {
		for (short threadNum = 0; threadNum < Parameters.numThreads; threadNum++) {
			switch(benchType) {
			case INTSET:
				numAdd += threadLoopsSet[threadNum].numAdd;
				numRemove += threadLoopsSet[threadNum].numRemove;
				numAddAll += threadLoopsSet[threadNum].numAddAll;
				numRemoveAll += threadLoopsSet[threadNum].numRemoveAll;
				numSize += threadLoopsSet[threadNum].numSize;
				numContains += threadLoopsSet[threadNum].numContains;
				failures += threadLoopsSet[threadNum].failures;
				total += threadLoopsSet[threadNum].total;
				//aborts += threadLoopsSet[threadNum].aborts;
				getCount += threadLoopsSet[threadNum].getCount;
				nodesTraversed += threadLoopsSet[threadNum].nodesTraversed;
				structMods += threadLoopsSet[threadNum].structMods;
				break;
			case MAP:
				numAdd += threadLoops[threadNum].numAdd;
				numRemove += threadLoops[threadNum].numRemove;
				numAddAll += threadLoops[threadNum].numAddAll;
				numRemoveAll += threadLoops[threadNum].numRemoveAll;
				numSize += threadLoops[threadNum].numSize;
				numContains += threadLoops[threadNum].numContains;
				failures += threadLoops[threadNum].failures;
				total += threadLoops[threadNum].total;
				//aborts += threadLoops[threadNum].aborts;

				getCount += threadLoops[threadNum].getCount;
				getTimeTotal += threadLoops[threadNum].getTimeTotal;
				getTimeSquareSum += threadLoops[threadNum].getTimeSquareSum;

				nodesTraversed += threadLoops[threadNum].nodesTraversed;
				structMods += threadLoops[threadNum].structMods;
				scanTimeMax = Math.max(scanTimeMax, threadLoops[threadNum].scanTimeMax);
				scanTimeTotal += threadLoops[threadNum].scanTimeTotal;
				scanTimeSquareSum += threadLoops[threadNum].scanTimeSquareSum;
				scanCount += threadLoops[threadNum].scanCount;
				scanKeysCount += threadLoops[threadNum].scanKeysCount;

				putTimeMax = Math.max(putTimeMax, threadLoops[threadNum].putTimeMax);
				putTimeTotal += threadLoops[threadNum].putTimeTotal;
				putTimeSquareSum += threadLoops[threadNum].putTimeSquareSum;
				putCount += threadLoops[threadNum].putCount;

				putThreads += threadLoops[threadNum].putThreads;
				scanThreads += threadLoops[threadNum].scanThreads;

				break;
			case SORTEDSET:
				numAdd += threadLoopsSSet[threadNum].numAdd;
				numRemove += threadLoopsSSet[threadNum].numRemove;
				numAddAll += threadLoopsSSet[threadNum].numAddAll;
				numRemoveAll += threadLoopsSSet[threadNum].numRemoveAll;
				numSize += threadLoopsSSet[threadNum].numSize;
				numContains += threadLoopsSSet[threadNum].numContains;
				failures += threadLoopsSSet[threadNum].failures;
				total += threadLoopsSSet[threadNum].total;
				//aborts += threadLoopsSSet[threadNum].aborts;
				getCount += threadLoopsSSet[threadNum].getCount;
				nodesTraversed += threadLoopsSSet[threadNum].nodesTraversed;
				structMods += threadLoopsSSet[threadNum].structMods;
				break;
			}
		}
		throughput = ((double) total / elapsedTime);
		printLine('-');
		System.out.println("Benchmark statistics");
		printLine('-');
		System.out.println("  Average traversal length: \t"
				+ (double) nodesTraversed / (double) getCount);


		System.out.println("  Struct Modifications:     \t" + structMods);
		System.out.println("  Throughput (ops/s):       \t" + throughput);
		System.out.println("  Elapsed time (s):         \t" + elapsedTime);
		System.out.println("  Fill time (s):			\t" + fillTime);
		System.out.println("  Compactions num:			\t" + Parameters.compactionsNum.get());
		System.out.println("  Engaged chunks num:		\t" + Parameters.engagedChunks.get());

		System.out.println("  Get throughput (ops/s):   \t" + (double)getCount/elapsedTime);
		System.out.println("  Get count (ops):           \t" + getCount);
		System.out.println("  Total get time (ms):       \t" + getTimeTotal);
		System.out.println("  Get time avg (ms):         \t" + (double)getTimeTotal/getCount/1000/1000);

		System.out.println("  Put throughput (ops/s):    \t" + (double)putCount/elapsedTime);
		System.out.println("  Put count (ops):           \t" + putCount);
		System.out.println("  Total put time (ms):        \t" + putTimeTotal);
		System.out.println("  Put time avg (ms):         \t" + (double)putTimeTotal/putCount/1000/1000);
		System.out.println("  Put time stdev (ms):       \t" + Math.sqrt((double)putTimeSquareSum/putCount - Math.pow((double)putTimeTotal/putCount,2))/1000/1000);
		System.out.println("  Num of put threads:        \t" + putThreads);

		System.out.println("  Put time max (ms):         \t" + (double)putTimeMax / 1000 / 1000);


		System.out.println("  Scan throughput (scans/s):  \t" + (double)scanCount/elapsedTime);
		System.out.println("  Scan count (ops):           \t" + scanCount);
		System.out.println("  Total scan time (ms):       \t" + scanTimeTotal);

		System.out.println("  Scan time avg (ms):         \t" + (double)scanTimeTotal/scanCount/1000/1000);
		System.out.println("  Scan time stdev (ms):       \t" + Math.sqrt((double)scanTimeSquareSum/scanCount - Math.pow((double)scanTimeTotal/scanCount,2))/1000/1000);
		System.out.println("  Scan time max (ms):         \t" + (double)scanTimeMax / 1000 / 1000);

		System.out.println("  Scan per key time avg (ms): \t" + (double)scanTimeTotal/scanKeysCount/1000/1000);
		System.out.println("  Scan keys num avg:          \t" + (double)scanKeysCount/scanCount);
		System.out.println("  Num of scan threads:        \t" + scanThreads);

		System.out.println("  Operations:               \t" + total
				+ "\t( 100 %)");

		System.out
				.println("    effective updates:     \t"
						+ (numAdd + numRemove + numAddAll + numRemoveAll)
						+ "\t( "
						+ formatDouble(((double) (numAdd + numRemove
								+ numAddAll + numRemoveAll) * 100)
								/ (double) total) + " %)");
		System.out.println("    |--add successful:     \t" + numAdd + "\t( "
				+ formatDouble(((double) numAdd / (double) total) * 100)
				+ " %)");
		System.out.println("    |--remove succ.:       \t" + numRemove + "\t( "
				+ formatDouble(((double) numRemove / (double) total) * 100)
				+ " %)");
		System.out.println("    |--addAll succ.:       \t" + numAddAll + "\t( "
				+ formatDouble(((double) numAddAll / (double) total) * 100)
				+ " %)");
		System.out.println("    |--removeAll succ.:    \t" + numRemoveAll
				+ "\t( "
				+ formatDouble(((double) numRemoveAll / (double) total) * 100)
				+ " %)");
		System.out.println("    size successful:       \t" + numSize + "\t( "
				+ formatDouble(((double) numSize / (double) total) * 100)
				+ " %)");
		System.out.println("    contains succ.:        \t" + numContains
				+ "\t( "
				+ formatDouble(((double) numContains / (double) total) * 100)
				+ " %)");
		System.out.println("    unsuccessful ops:      \t" + failures + "\t( "
				+ formatDouble(((double) failures / (double) total) * 100)
				+ " %)");
		switch(benchType) {
		case INTSET:
			System.out.println("  Final size:              \t" + setBench.size());
			if (Parameters.numWriteAlls == 0) System.out.println("  Expected size:           \t" + (Parameters.size+numAdd-numRemove));
			break;
		case MAP:
			System.out.println("  Final size:              \t" + mapBench.size());
			if (Parameters.numWriteAlls == 0) System.out.println("  Expected size:           \t" + (Parameters.size+numAdd-numRemove));
			break;
		case SORTEDSET:
			System.out.println("  Final size:              \t" + sortedBench.size());
			if (Parameters.numWriteAlls == 0) System.out.println("  Expected size:           \t" + (Parameters.size+numAdd-numRemove));
			break;
		}
		//System.out.println("  Other size:              \t" + map.size());

		// TODO what should print special for maint data structures
		// if (bench instanceof CASSpecFriendlyTreeLockFree) {
		// System.out.println("  Balance:              \t"
		// + ((CASSpecFriendlyTreeLockFree) bench).getBalance());
		// }
		// if (bench instanceof SpecFriendlyTreeLockFree) {
		// System.out.println("  Balance:              \t"
		// + ((SpecFriendlyTreeLockFree) bench).getBalance());
		// }
		// if (bench instanceof TransFriendlyMap) {
		// System.out.println("  Balance:              \t"
		// + ((TransFriendlyMap) bench).getBalance());
		// }
		// if (bench instanceof UpdatedTransFriendlySkipList) {
		// System.out.println("  Bottom changes:              \t"
		// + ((UpdatedTransFriendlySkipList) bench)
		// .getBottomLevelRaiseCount());
		// }

		switch(benchType) {
		case INTSET:
			if (setBench instanceof MaintenanceAlg) {
				System.out.println("  #nodes (inc. deleted): \t"
						+ ((MaintenanceAlg) setBench).numNodes());
			}
			break;
		case MAP:
			if (mapBench instanceof MaintenanceAlg) {
				System.out.println("  #nodes (inc. deleted): \t"
						+ ((MaintenanceAlg) mapBench).numNodes());
			}
			break;
		case SORTEDSET:
			if (mapBench instanceof MaintenanceAlg) {
				System.out.println("  #nodes (inc. deleted): \t"
						+ ((MaintenanceAlg) sortedBench).numNodes());
			}
			break;
		}

	}

	/**
	 * Detailed Warmup TM Statistics
	 */
	private int numCommits = 0;
	private int numStarts = 0;
	private int numAborts = 0;

	private int numCommitsReadOnly = 0;
	private int numCommitsElastic = 0;
	private int numCommitsUpdate = 0;

	private int numAbortsBetweenSuccessiveReads = 0;
	private int numAbortsBetweenReadAndWrite = 0;
	private int numAbortsExtendOnRead = 0;
	private int numAbortsWriteAfterRead = 0;
	private int numAbortsLockedOnWrite = 0;
	private int numAbortsLockedBeforeRead = 0;
	private int numAbortsLockedBeforeElasticRead = 0;
	private int numAbortsLockedOnRead = 0;
	private int numAbortsInvalidCommit = 0;
	private int numAbortsInvalidSnapshot = 0;

	private double readSetSizeSum = 0.0;
	private double writeSetSizeSum = 0.0;
	private int statSize = 0;
	private int txDurationSum = 0;
	private int elasticReads = 0;
	private int readsInROPrefix = 0;

	/**
	 * This method is called between two runs of the benchmark within the same
	 * JVM to enable its warmup
	 */
	public void resetStats() {

		for (short threadNum = 0; threadNum < Parameters.numThreads; threadNum++) {
			switch (benchType) {
			case INTSET:
			
			threadLoopsSet[threadNum].numAdd = 0;
			threadLoopsSet[threadNum].numRemove = 0;
			threadLoopsSet[threadNum].numAddAll = 0;
			threadLoopsSet[threadNum].numRemoveAll = 0;
			threadLoopsSet[threadNum].numSize = 0;
			threadLoopsSet[threadNum].numContains = 0;
			threadLoopsSet[threadNum].failures = 0;
			threadLoopsSet[threadNum].total = 0;
			threadLoopsSet[threadNum].aborts = 0;
			threadLoopsSet[threadNum].nodesTraversed = 0;
			threadLoopsSet[threadNum].getCount = 0;
			threadLoopsSet[threadNum].structMods = 0;
			break;
			case MAP:
			threadLoops[threadNum].numAdd = 0;
			threadLoops[threadNum].numRemove = 0;
			threadLoops[threadNum].numAddAll = 0;
			threadLoops[threadNum].numRemoveAll = 0;
			threadLoops[threadNum].numSize = 0;
			threadLoops[threadNum].numContains = 0;
			threadLoops[threadNum].failures = 0;
			threadLoops[threadNum].total = 0;
			threadLoops[threadNum].aborts = 0;
			threadLoops[threadNum].nodesTraversed = 0;
			threadLoops[threadNum].structMods = 0;

			threadLoops[threadNum].scanCount = 0;
			threadLoops[threadNum].scanTimeTotal = 0;
			threadLoops[threadNum].scanTimeMax = 0;
			threadLoops[threadNum].scanTimeTotal = 0;
			threadLoops[threadNum].scanKeysCount = 0;
			threadLoops[threadNum].scanThreads = 0;

			threadLoops[threadNum].getTimeTotal =0;
			threadLoops[threadNum].getTimeSquareSum = 0;
			threadLoops[threadNum].getCount = 0;



			threadLoops[threadNum].putCount = 0;
			threadLoops[threadNum].putTimeMax = 0;
			threadLoops[threadNum].putTimeTotal = 0;
			threadLoops[threadNum].putTimeSquareSum = 0;
			threadLoops[threadNum].putThreads = 0;

			break;
			case SORTEDSET:
			threadLoopsSSet[threadNum].numAdd = 0;
			threadLoopsSSet[threadNum].numRemove = 0;
			threadLoopsSSet[threadNum].numAddAll = 0;
			threadLoopsSSet[threadNum].numRemoveAll = 0;
			threadLoopsSSet[threadNum].numSize = 0;
			threadLoopsSSet[threadNum].numContains = 0;
			threadLoopsSSet[threadNum].failures = 0;
			threadLoopsSSet[threadNum].total = 0;
			threadLoopsSSet[threadNum].aborts = 0;
			threadLoopsSSet[threadNum].nodesTraversed = 0;
			threadLoopsSSet[threadNum].getCount = 0;
			threadLoopsSSet[threadNum].structMods = 0;
			break;
			}

		}
		numAdd = 0;
		numRemove = 0;
		numAddAll = 0;
		numRemoveAll = 0;
		numSize = 0;
		numContains = 0;
		failures = 0;
		total = 0;

		scanCount = 0;
		readCount = 0;
		putCount = 0;
		getCount = 0;
		scanKeysCount = 0;

		Parameters.compactionsNum.set(0);
		Parameters.engagedChunks.set(0);

		scanTimeTotal=0;
		putTimeTotal =0;
		getTimeTotal = 0;

		scanTimeSquareSum = 0;
		putTimeSquareSum = 0;
		getTimeSquareSum = 0;


		//aborts = 0;
		nodesTraversed = 0;
		getCount = 0;
		structMods = 0;


		numCommits = 0;
		numStarts = 0;
		numAborts = 0;

		numCommitsReadOnly = 0;
		numCommitsElastic = 0;
		numCommitsUpdate = 0;

		numAbortsBetweenSuccessiveReads = 0;
		numAbortsBetweenReadAndWrite = 0;
		numAbortsExtendOnRead = 0;
		numAbortsWriteAfterRead = 0;
		numAbortsLockedOnWrite = 0;
		numAbortsLockedBeforeRead = 0;
		numAbortsLockedBeforeElasticRead = 0;
		numAbortsLockedOnRead = 0;
		numAbortsInvalidCommit = 0;
		numAbortsInvalidSnapshot = 0;

		readSetSizeSum = 0.0;
		writeSetSizeSum = 0.0;
		statSize = 0;
		txDurationSum = 0;
		elasticReads = 0;
		readsInROPrefix = 0;
	}

	public void recordPreliminaryStats() {
		numAborts = Statistics.getTotalAborts();
		numCommits = Statistics.getTotalCommits();
		numCommitsReadOnly = Statistics.getNumCommitsReadOnly();
		numCommitsElastic = Statistics.getNumCommitsElastic();
		numCommitsUpdate = Statistics.getNumCommitsUpdate();
		numStarts = Statistics.getTotalStarts();
		numAbortsBetweenSuccessiveReads = Statistics
				.getNumAbortsBetweenSuccessiveReads();
		numAbortsBetweenReadAndWrite = Statistics
				.getNumAbortsBetweenReadAndWrite();
		numAbortsExtendOnRead = Statistics.getNumAbortsExtendOnRead();
		numAbortsWriteAfterRead = Statistics.getNumAbortsWriteAfterRead();
		numAbortsLockedOnWrite = Statistics.getNumAbortsLockedOnWrite();
		numAbortsLockedBeforeRead = Statistics.getNumAbortsLockedBeforeRead();
		numAbortsLockedBeforeElasticRead = Statistics
				.getNumAbortsLockedBeforeElasticRead();
		numAbortsLockedOnRead = Statistics.getNumAbortsLockedOnRead();
		numAbortsInvalidCommit = Statistics.getNumAbortsInvalidCommit();
		numAbortsInvalidSnapshot = Statistics.getNumAbortsInvalidSnapshot();
		readSetSizeSum = Statistics.getSumReadSetSize();
		writeSetSizeSum = Statistics.getSumWriteSetSize();
		;
		statSize = Statistics.getStatSize();
		txDurationSum = Statistics.getSumCommitingTxTime();
		elasticReads = Statistics.getTotalElasticReads();
		readsInROPrefix = Statistics.getTotalReadsInROPrefix();
	}

	/**
	 * Print the detailed statistics on the standard output
	 */
	private void printDetailedStats() {

		numCommits = Statistics.getTotalCommits() - numCommits;
		numStarts = Statistics.getTotalStarts() - numStarts;
		numAborts = Statistics.getTotalAborts() - numAborts;

		numCommitsReadOnly = Statistics.getNumCommitsReadOnly()
				- numCommitsReadOnly;
		numCommitsElastic = Statistics.getNumCommitsElastic()
				- numCommitsElastic;
		numCommitsUpdate = Statistics.getNumCommitsUpdate() - numCommitsUpdate;

		numAbortsBetweenSuccessiveReads = Statistics
				.getNumAbortsBetweenSuccessiveReads()
				- numAbortsBetweenSuccessiveReads;
		numAbortsBetweenReadAndWrite = Statistics
				.getNumAbortsBetweenReadAndWrite()
				- numAbortsBetweenReadAndWrite;
		numAbortsExtendOnRead = Statistics.getNumAbortsExtendOnRead()
				- numAbortsExtendOnRead;
		numAbortsWriteAfterRead = Statistics.getNumAbortsWriteAfterRead()
				- numAbortsWriteAfterRead;
		numAbortsLockedOnWrite = Statistics.getNumAbortsLockedOnWrite()
				- numAbortsLockedOnWrite;
		numAbortsLockedBeforeRead = Statistics.getNumAbortsLockedBeforeRead()
				- numAbortsLockedBeforeRead;
		numAbortsLockedBeforeElasticRead = Statistics
				.getNumAbortsLockedBeforeElasticRead()
				- numAbortsLockedBeforeElasticRead;
		numAbortsLockedOnRead = Statistics.getNumAbortsLockedOnRead()
				- numAbortsLockedOnRead;
		numAbortsInvalidCommit = Statistics.getNumAbortsInvalidCommit()
				- numAbortsInvalidCommit;
		numAbortsInvalidSnapshot = Statistics.getNumAbortsInvalidSnapshot()
				- numAbortsInvalidSnapshot;

		assert (numAborts == (numAbortsBetweenSuccessiveReads
				+ numAbortsBetweenReadAndWrite + numAbortsExtendOnRead
				+ numAbortsWriteAfterRead + numAbortsLockedOnWrite
				+ numAbortsLockedBeforeRead + numAbortsLockedBeforeElasticRead
				+ numAbortsLockedOnRead + numAbortsInvalidCommit + numAbortsInvalidSnapshot));

		assert (numStarts - numAborts) == numCommits;

		readSetSizeSum = Statistics.getSumReadSetSize() - readSetSizeSum;
		writeSetSizeSum = Statistics.getSumWriteSetSize() - writeSetSizeSum;
		statSize = Statistics.getStatSize() - statSize;
		txDurationSum = Statistics.getSumCommitingTxTime() - txDurationSum;

		printLine('-');
		System.out.println("TM statistics");
		printLine('-');

		System.out.println("  Commits:                  \t" + numCommits);
		System.out
				.println("  |--regular read only  (%) \t"
						+ numCommitsReadOnly
						+ "\t( "
						+ formatDouble(((double) numCommitsReadOnly / (double) numCommits) * 100)
						+ " %)");
		System.out
				.println("  |--elastic (%)            \t"
						+ numCommitsElastic
						+ "\t( "
						+ formatDouble(((double) numCommitsElastic / (double) numCommits) * 100)
						+ " %)");
		System.out
				.println("  |--regular update (%)     \t"
						+ numCommitsUpdate
						+ "\t( "
						+ formatDouble(((double) numCommitsUpdate / (double) numCommits) * 100)
						+ " %)");
		System.out.println("  Starts:                   \t" + numStarts);
		System.out.println("  Aborts:                   \t" + numAborts
				+ "\t( 100 %)");
		System.out
				.println("  |--between succ. reads:   \t"
						+ (numAbortsBetweenSuccessiveReads)
						+ "\t( "
						+ formatDouble(((double) (numAbortsBetweenSuccessiveReads) * 100)
								/ (double) numAborts) + " %)");
		System.out
				.println("  |--between read & write:  \t"
						+ numAbortsBetweenReadAndWrite
						+ "\t( "
						+ formatDouble(((double) numAbortsBetweenReadAndWrite / (double) numAborts) * 100)
						+ " %)");
		System.out
				.println("  |--extend upon read:      \t"
						+ numAbortsExtendOnRead
						+ "\t( "
						+ formatDouble(((double) numAbortsExtendOnRead / (double) numAborts) * 100)
						+ " %)");
		System.out
				.println("  |--write after read:      \t"
						+ numAbortsWriteAfterRead
						+ "\t( "
						+ formatDouble(((double) numAbortsWriteAfterRead / (double) numAborts) * 100)
						+ " %)");
		System.out
				.println("  |--locked on write:       \t"
						+ numAbortsLockedOnWrite
						+ "\t( "
						+ formatDouble(((double) numAbortsLockedOnWrite / (double) numAborts) * 100)
						+ " %)");
		System.out
				.println("  |--locked before read:    \t"
						+ numAbortsLockedBeforeRead
						+ "\t( "
						+ formatDouble(((double) numAbortsLockedBeforeRead / (double) numAborts) * 100)
						+ " %)");
		System.out
				.println("  |--locked before eread:   \t"
						+ numAbortsLockedBeforeElasticRead
						+ "\t( "
						+ formatDouble(((double) numAbortsLockedBeforeElasticRead / (double) numAborts) * 100)
						+ " %)");
		System.out
				.println("  |--locked on read:        \t"
						+ numAbortsLockedOnRead
						+ "\t( "
						+ formatDouble(((double) numAbortsLockedOnRead / (double) numAborts) * 100)
						+ " %)");
		System.out
				.println("  |--invalid commit:        \t"
						+ numAbortsInvalidCommit
						+ "\t( "
						+ formatDouble(((double) numAbortsInvalidCommit / (double) numAborts) * 100)
						+ " %)");
		System.out
				.println("  |--invalid snapshot:      \t"
						+ numAbortsInvalidSnapshot
						+ "\t( "
						+ formatDouble(((double) numAbortsInvalidSnapshot / (double) numAborts) * 100)
						+ " %)");
		System.out.println("  Read set size on avg.:    \t"
				+ formatDouble(readSetSizeSum / statSize));
		System.out.println("  Write set size on avg.:   \t"
				+ formatDouble(writeSetSizeSum / statSize));
		System.out.println("  Tx time-to-commit on avg.:\t"
				+ formatDouble((double) txDurationSum / numCommits)
				+ " microsec");
		System.out.println("  Number of elastic reads       " + elasticReads);
		System.out
				.println("  Number of reads in RO prefix  " + readsInROPrefix);
	}

	private static String formatDouble(double result) {
		try (Formatter formatter = new Formatter(Locale.US))
		{
			return formatter.format("%.2f", result).out().toString();
		}
	}
}
