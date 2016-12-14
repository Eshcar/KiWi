package kiwisplit;

import java.util.Collections;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.locks.ReentrantLock;

import contention.abstractions.CompositionalMap;

/**
 * Created by msulamy on 7/29/15.
 */
public class Tester implements Runnable
{
	private static final KiWiMap kiwi = new KiWiMap();
	private static final Vector<Integer> vals = new Vector<Integer>();
	private static final ReentrantLock lock = new ReentrantLock();
	
	public void run()
	{
		final int SIZE = 1000000;

		Vector<Integer> vec = new Vector<>(SIZE);
		Random rand = new Random();
		
		for (int i = 0; i < SIZE; ++i) {
			int x = rand.nextInt(Integer.MAX_VALUE);
			vec.add(x);
			kiwi.put(x, x);
			
			if (i % 10000 == 0)
			{
				// validation from vec removed for now
				
				Integer[] result = new Integer[10000000];
				int r = kiwi.getRange(result, 5, 1000);
				
				for (int z = 0; z < r; ++z)
				{
					if (!kiwi.get(result[z]).equals(result[z]))
						System.out.println("ERROR!!");
				}
				
				
			}
		}
		
		//kiwi.debugPrint();
		
		for (Integer x : vec) {
			Integer n = kiwi.get(x);
			if ((n == null) || (! n.equals(x))) {
				System.out.println("ERROR! 1");
			}
		}
		
		lock.lock();
		//vals.setSize(vals.size() + SIZE);
		for (int x : vec)
			vals.add(x);
		lock.unlock();
	}
	
	public static void test() throws Exception
	{
		Thread[] threads = new Thread[8];
		
		for (int i = 0; i < threads.length; ++i)
			threads[i] = new Thread(new Tester());
		
		for (int i = 0; i < threads.length; ++i)
			threads[i].start();
		
		for (int i = 0; i < threads.length; ++i)
			threads[i].join();
		
		Collections.sort(vals);
		int idx = 0;
		System.out.println("Checking upto: " + Integer.MAX_VALUE);
		
		//int max = Collections.max(vals);
		
		final int DIV = 30000000;
		
		System.out.print("|");
		for (int i = 0; i < Integer.MAX_VALUE / DIV; ++i)
			System.out.print('-');
		if (Integer.MAX_VALUE % DIV != 0)
			System.out.print('-');
		System.out.println("|");
	
		System.out.print("|");
		
		for (int i = 0; i < Integer.MAX_VALUE; ++i)
		{
			if (i % DIV == 0)
				System.out.print('.');

			if ((idx < vals.size()) && (vals.get(idx) == i)) {
				if (!kiwi.get(i).equals(i))
					System.out.println("ERROR! 2");
				
				while ((idx < vals.size()) && (vals.get(idx) == i))
					++idx;
			}
			else {
				if (kiwi.get(i) != null)
					System.out.println("ERROR! 3");
			}
		}
		System.out.println("|");

		System.out.println("Check finished");
	}

	public static void main(String[] args) throws Exception
	{
		boolean myTest = false;
		
		if (myTest)
			test();
		else
		{
			CompositionalMap<Integer,Integer> map;
			
			if (args.length > 0)
				map = contention.benchmark.Test.runTest(args);
			else
				map = contention.benchmark.Test.runTest(new String[] {
					"-W", "5", "-u", "100", "-s", "0", "-R", "false", "-MR", "1000", "-mR", "1000",
					"-d", "5000", "-t", "4", "-i", "1000000", "-kd", "true", "-ks", "true", "-K", "true",
					"-r", "2000000", "-b",
					//"trees.lockfree.LockFreeJavaSkipList"
					"trees.lockfree.LockFreeKSTRQ"
					//"kiwi.KiWiMap"
				});
			
			KiWiMap kMap = (KiWiMap) map;
			
			int keys = kMap.debugCountKeys();
			int dups = kMap.debugCountDups();
			
		}
	}
}
