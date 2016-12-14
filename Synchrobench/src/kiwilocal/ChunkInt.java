package kiwilocal;


import kiwilocal.Chunk;

import java.util.concurrent.atomic.AtomicInteger;

public class ChunkInt extends Chunk<Integer,Integer> {
	private static AtomicInteger nextChunk;
	private static ChunkInt[] chunks;

	public static void setPoolSize(int numChunks) {
		chunks = new ChunkInt[numChunks];

	}

	public static void initPool() {
		if (chunks != null) {
			nextChunk = new AtomicInteger(0);
			for (int i = 0; i < chunks.length; ++i)
				chunks[i] = new ChunkInt(null, null);
		}
	}

	private static final int DATA_SIZE = Integer.SIZE / 8;    // average # of BYTES of item in data array (guesstimate)

	public ChunkInt() {
		this(Integer.MIN_VALUE, null);
	}

	public ChunkInt(Integer minKey, ChunkInt creator) {
		super(minKey, DATA_SIZE, creator);
	}

	@Override
	public Chunk<Integer, Integer> newChunk(Integer minKey) {
		if (chunks == null) {
			return new ChunkInt(minKey, this);
		} else {
			int next = nextChunk.getAndIncrement();
			ChunkInt chunk = chunks[next];
			chunks[next] = null;
			chunk.minKey = minKey;
			chunk.creator = this;
			return chunk;
		}
	}

	@Override
	public Integer readKey(int orderIndex) {
		return get(orderIndex, OFFSET_KEY);
	}

	@Override
	public Integer readData(int oi, int di) {
		// get data - convert next 4 bytes to Integer data
		int data = dataArray[di] << 24 | (dataArray[di + 1] & 0xFF) << 16 |
				(dataArray[di + 2] & 0xFF) << 8 | (dataArray[di + 3] & 0xFF);

		return data;
	}

	@Override
	public int allocate(Integer key, Integer data) {
		// allocate items in order and data array => data-array only contains int-sized data
		int oi = baseAllocate(data != null ? DATA_SIZE : 0);

		if (oi >= 0) {
			// write integer key into (int) order array at correct offset
			set(oi, OFFSET_KEY, (int) key);

			if (data != null) {
				// get data index
				int di = get(oi, OFFSET_DATA);

				// write int data as bytes into data-array
				dataArray[di] = (byte) (data >>> 24);
				dataArray[di + 1] = (byte) (data >>> 16);
				dataArray[di + 2] = (byte) (data >>> 8);
				dataArray[di + 3] = (byte) (data.intValue());
			}
		}

		// return order-array index (can be used to get data-array index)
		return oi;
	}

	@Override
	public int allocateSerial(Integer key, Integer data) {
		// allocate items in order and data array => data-array only contains int-sized data
		int oi = baseAllocateSerial(data != null ? DATA_SIZE : 0);

		if (oi >= 0) {
			// write integer key into (int) order array at correct offset
			set(oi, OFFSET_KEY, (int) key);

			if (data != null) {
				// get data index
				int di = get(oi, OFFSET_DATA);

				// write int data as bytes into data-array
				dataArray[di] = (byte) (data >>> 24);
				dataArray[di + 1] = (byte) (data >>> 16);
				dataArray[di + 2] = (byte) (data >>> 8);
				dataArray[di + 3] = (byte) (data.intValue());
			}
		}

		// return order-array index (can be used to get data-array index)
		return oi;
	}
}
