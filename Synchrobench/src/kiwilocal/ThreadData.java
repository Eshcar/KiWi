package kiwilocal;

import kiwilocal.Chunk;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ThreadData
{
	/** Thread data class for Scan operations **/
	public static class ScanData<K extends Comparable<? super K>,V> extends ThreadData
	{
		public Chunk<K,V> chunk = null;
		public final AtomicLong version = new AtomicLong(Chunk.NONE_VERSION);

		public boolean isMultiChunk()
		{
			return chunk == null;
		}
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
