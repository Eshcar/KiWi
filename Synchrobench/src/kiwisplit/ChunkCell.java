package kiwisplit;

public class ChunkCell extends Chunk<Cell, Cell>
{
	private static final int DATA_SIZE = 100;		// average # of BYTES of item in data array (guesstimate)
	
	public ChunkCell()
	{
		this(Cell.Empty, null);
	}
	public ChunkCell(Cell minKey, ChunkCell creator)
	{
		super(minKey, DATA_SIZE, creator);
	}
	@Override
	public Chunk<Cell,Cell> newChunk(Cell minKey)
	{
		return new ChunkCell(minKey.clone(), this);
	}
	
	
	@Override
	public Cell readKey(int orderIndex)
	{
		int di = get(orderIndex, OFFSET_DATA);		// get data index where key resides
		int keyLen = get(orderIndex, OFFSET_KEY);	// get key length from order-array
		return new Cell(dataArray, di, keyLen);		// return key from data-array
	}
	@Override
	public Cell readData(int oi, int di)
	{
		int keyLen = get(oi, OFFSET_KEY);			// get key length (to skip in data-array)
		di += keyLen;								// skip key length
		
		// get data length - convert next 4 bytes to int
		int dataLen = dataArray[di] << 24 | (dataArray[di+1] & 0xFF) << 16 |
			(dataArray[di+2] & 0xFF) << 8 | (dataArray[di+3] & 0xFF);
		
		// return relevant portion (Cell) of data-array
		return new Cell(dataArray, di+Integer.SIZE/8, dataLen);
	}
	
	@Override
	public int allocate(Cell key, Cell data)
	{
		// allocate items in order and data array => variable-sized key and data go into data array
		int oi = baseAllocate(key.getLength() + data.getLength() + Integer.SIZE/8); // +INT for data length
		if (oi >= 0)
		{
			// write key length into order array at correct offset
			set(oi, OFFSET_KEY, key.getLength());
			
			// get data index
			int di = get(oi, OFFSET_DATA);

			int keyLen = key.getLength();
			int dataLen = data.getLength();
			
			// write key into data array
			System.arraycopy(key.getBytes(), key.getOffset(), dataArray, di, keyLen);
			di += keyLen;
			
			// write data length into next 4 bytes (int)
			dataArray[di]	= (byte) (dataLen >>> 24);
			dataArray[di+1]	= (byte) (dataLen >>> 16);
			dataArray[di+2]	= (byte) (dataLen >>> 8);
			dataArray[di+3]	= (byte) (dataLen);
			
			// write data into next part of data-array
			System.arraycopy(data.getBytes(), data.getOffset(), dataArray, di+4, dataLen);
		}
		
		// return order-array index (can be used to get data-array index)
		return oi;
	}
}
