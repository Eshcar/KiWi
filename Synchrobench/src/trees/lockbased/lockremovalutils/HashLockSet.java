package trees.lockbased.lockremovalutils;

import java.util.HashMap;
import java.util.Map.Entry;

public class HashLockSet {
	private HashMap<SpinHeapReentrant,Integer> lockSet;
	private HashMap<SpinHeapReentrant,Integer> successfullyLocked;
	
	public HashLockSet(){
		lockSet = new HashMap<SpinHeapReentrant,Integer>();
		successfullyLocked = new HashMap<SpinHeapReentrant,Integer>(); 
	}
	
	public HashLockSet(int size){
		lockSet = new HashMap<SpinHeapReentrant,Integer>(size);
		successfullyLocked = new HashMap<SpinHeapReentrant,Integer>(size); 
	}

	
	public void clear(){
		lockSet.clear();
	}
	
	public void add(SpinHeapReentrant node){
		assert(node!=null);
		//In the hash set we also add a counter for reentrant
		if(lockSet.containsKey(node)){
			int times = lockSet.get(node);
			times = times + 1; 
			lockSet.put(node, times);
		}else{
			lockSet.put(node,1);
		}
	}
	
	public void remove(SpinHeapReentrant node){
		//assert(lockSet.containsKey(node));
		int count = lockSet.get(node);
		if (count >1){
			count = count -1; 
			lockSet.put(node, count);
		}else{
			assert(count == 1 );
			lockSet.remove(node);
		}
	}
	
	public boolean tryLockAll(Thread self){
		SpinHeapReentrant node; 
		for(Entry<SpinHeapReentrant, Integer> entry : lockSet.entrySet()) {
			node = entry.getKey();
		    int times = entry.getValue();
		    
		    if(!node.tryAcquire(self)){
	    		//free previous nodes from the lockSet
	    		for(Entry<SpinHeapReentrant, Integer> locked : successfullyLocked.entrySet()){
	    			node = locked.getKey();
	    			times = locked.getValue();
	    			for(int k=0; k< times; k++){
	    				node.release();
	    			}
	    		}
	    		successfullyLocked.clear();
	    		return false; 
	    	}
		    successfullyLocked.put(node, times);
		    for(int i=1; i<times; i++){
		    	//re-acquire according to times
		    	node.reacquire();
		    }
		}	
		successfullyLocked.clear();
		return true; 
	}

	public void releaseAll() {
		for(Entry<SpinHeapReentrant, Integer> entry : lockSet.entrySet()) {
			SpinHeapReentrant node = entry.getKey();
		    int times = entry.getValue();
		    for(int i=0; i<times; i++){
		    	//release according to times
		    	node.release();
		    }
		}
	}
}
