package trees.lockbased.lockremovalutils;

public final class NonSharedFastSimpleRandom {
	private long state;
    
    public NonSharedFastSimpleRandom(long l){
    	state = l * 0x123456789abcdefL;
    }

    public int nextInt() {
        final long next = step(state);
        state = next;
        return extract(next);
    }

    private long step(final long x) {
        return x * 2862933555777941757L + 3037000493L;
    }

    private int extract(final long x) {
        return (int)(x >> 30);
    }
}
