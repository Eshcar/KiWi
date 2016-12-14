package kiwisplit;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class ThreadData
{
	/** Thread data class for Scan operations **/
	public static class ScanData extends ThreadData
	{
		public final AtomicInteger version = new AtomicInteger(Chunk.NONE);
	}
	
	/** Thread data class for Put operations **/
	public static class PutData<K extends Comparable<? super K>,V> extends ThreadData
	{
		public final Chunk<K,V>	chunk;
		public final int		orderIndex;
		
		public PutData(Chunk<K,V> chunk, int orderIndex)
		{
			this.chunk = chunk;
			this.orderIndex = orderIndex;
		}
	}
}
