package trees.lockbased.lockremovalutils;

import java.util.concurrent.atomic.AtomicReference;

public class SpinHeapReentrant extends AtomicReference<Object>
{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final int THRESHOLD = Math.max(1, Integer.valueOf(System.getProperty("spin", "100000")));

    private int depth = 0;
    private int incoming = 0;
    private volatile int version = 0;  
    
    public int getVersion(){
    	return this.version; 
    }
    
    public boolean isLocked(){
    	return this.get()!=null;
    }
    
    public void acquire(final Thread self) {
        lock(self);
        depth++;
        //if (depth == 1) version++;
    }
    
    public boolean tryAcquire(final Thread self){
    	if ((this.get() == null && this.compareAndSet(null, self)) || this.get() == self ) {
    		 depth++;
    		 //version++;
    		 return true;
        }
    	return false; 
    }

    public void reacquire() {        
        depth++;
    }

    public void release() {
        depth--;
        assert(depth > -1);
        maybeUnlock();
    }

    public void addIncoming(final Thread self) {
        lock(self);
        incoming++;
        maybeUnlock();
    }

    public void removeIncoming(final Thread self) {
        lock(self);
        incoming--;
        maybeUnlock();
    }
    
    private void lock(final Thread self) {
        if (this.get() != self && !tryLock(self)) {
            slowLock(self);
        }
    }

    private boolean tryLock(final Thread self) {
        for (int i = 0; i < THRESHOLD; ++i) {
            if (this.get() == null && this.compareAndSet(null, self)) {
                return true;
            }
            /*if(i%1000 == 0  && i!=0) {
            	System.out.println(i+ ": trying to acquire node, thread id " + self + "locked by" + this.get() );
            	System.out.flush();
            }*/
           
        }
        return false;
    }

    private void slowLock(final Thread self) {
        do {
            Thread.yield();
        } while (!tryLock(self));
    }

    private void maybeUnlock() {
        if (depth == 0 && incoming < 2) {
        	version++;
            this.lazySet(null);
        }
    }
    
    /*for debugging ... */
    public Object lockedBy(){
    	return this.get(); 
    }
    
}
