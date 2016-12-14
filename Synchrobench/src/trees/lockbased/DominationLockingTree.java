package trees.lockbased;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import trees.lockbased.lockremovalutils.SpinHeapReentrant;
import contention.abstractions.CompositionalMap;

public class DominationLockingTree<K,V> implements CompositionalMap<K, V>{
	
	private final Comparator<? super K> comparator;
	//private final K min; 
	private final Node<K, V> root; 
	
	public DominationLockingTree() {	
		//this.min = min;
		this.root = new Node(Integer.MAX_VALUE,null); 
        this.comparator = null;        
    }
	
	@SuppressWarnings("unchecked")
	private Comparable<? super K> comparable(final Object object) {

		if (object == null) throw new NullPointerException();
		if (comparator == null) return (Comparable<? super K>)object;

		return new Comparable<K>() {
			final Comparator<? super K> compar = comparator;
			final K obj = (K) object;

			public int compareTo(final K other) { 
				return compar.compare(obj, other); 
			}
		};
	}
	
	/*Node*/
    private static class Node<K, V> extends SpinHeapReentrant {
    	/**
    	 * Default 
    	 */
    	private static final long serialVersionUID = 1L;

    	enum Direction {
            LEFT, RIGHT
        }
    	
    	public Node(K key, V value) {
    		this.key = key;
    		this.value = value;
    		this.left = null;
    		this.right = null;
    	}
    	
    	final K key;
    	V value;
    	Node<K, V> left;
    	Node<K, V> right; 

    	void setChild(final Direction dir, final Node<K,V> n, final Thread self) {
            if (n != null)
                n.addIncoming(self);
            if (dir == Direction.LEFT) {
                if (left != null)
                    left.removeIncoming(self);
                left = n;
            } else {
                if (right != null)
                    right.removeIncoming(self);
                right = n;
            }
        }
    }
	
    /*Thread Locals*/
    private final ThreadLocal<Thread> threadSelf = new ThreadLocal<Thread>(){
        @Override
        protected Thread initialValue()
        {
            return Thread.currentThread();
        }
    };
    
    /*Helper functions*/
    
    private Node<K,V> acquire(Node<K,V> node, Thread self) {
		if (node != null) {
            node.acquire(self);
        }
        return node;
	}
	
	private void release(Node<K,V> node) {
		if (node != null){
	           node.release();
		}
	}
	
	public Node<K,V> assign(Node<K,V> prevValue,
			Node<K,V> newValue, Thread self) {
		acquire(newValue, self);
        release(prevValue);
        return newValue;
	}
	
	/*Map functions*/
    
	@Override
	public boolean containsKey(Object key) {
		if(get(key)!=null){
			return true;
		}
		return false;
	}

	@Override
	public boolean containsValue(Object arg0) {
		throw new RuntimeException("unimplemented method");
		// TODO Auto-generated method stub
		//return false;
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		 throw new RuntimeException("unimplemented method");
			// TODO Auto-generated method stub
			//return null;
	}

	@Override
	public V get(Object key) {
		final Comparable<? super K> k = comparable(key);
		Thread self = threadSelf.get();
		
		Node<K, V> curr = acquire(this.root,self); 
		while(curr!=null){
			int res = k.compareTo(curr.key);
			if(res == 0) break; 
			if(res > 0){ //key > x.key
				curr = assign(curr, curr.right, self); 
			}else{
				curr = assign(curr, curr.left, self); 
			}
		}
		if(curr!=null){
			release(curr);
			return curr.value;
		}
		return null; 
	
	}

	@Override
	public boolean isEmpty() {
		return root.left==null;
	}

	@Override
	public Set<K> keySet() {
		// Non Linearizable
		Set<K> keys = new HashSet<K>();
		doGetAllKeys(this.root.left,keys);
		return keys; 
	}
	
	private void doGetAllKeys(Node<K, V> node, Set<K> keys ){
		if(node!=null){
			keys.add(node.key);
			doGetAllKeys(node.left, keys);
			doGetAllKeys(node.right, keys);
		}
	}
	
	@Override
	public V put(K key, V val) {
		final Comparable<? super K> k = comparable(key);
		Thread self = threadSelf.get();
		
		V oldValue = null;
		Node<K, V> prev = null;
		Node<K, V> curr = acquire(this.root, self); 
		int res = -1;
		while(curr!=null){
			prev = assign(prev,curr,self);
			res = k.compareTo(curr.key);
			if(res == 0) {
				oldValue = curr.value;
				break; 
			}
			if(res > 0){ //key > x.key
				curr = assign(curr, curr.right, self) ; 
			}else{
				curr = assign(curr, curr.left, self) ; 
			}
		}
		if(res == 0) {
			curr.value = val; 
			release(prev);
			release(curr);
			return oldValue;
		}
		Node<K, V> node = acquire(new Node<K, V>(key,val),self);
		if (res > 0 ) { 
			prev.setChild(Node.Direction.RIGHT,node,self); 
		} else {
			prev.setChild(Node.Direction.LEFT,node,self); 
		}
		release(prev);
		release(curr);
		release(node);
		return oldValue;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		//NOT LINEARIZABLE
		Set<? extends K> keysToAdd = m.keySet();
		for(K key : keysToAdd){
			put(key, m.get(key));
		}
	}

	@Override
	public V remove(Object key) {
		final Comparable<? super K> k = comparable(key);
		Thread self = threadSelf.get();
		
		V oldValue = null;
		Node<K, V> prev = null;
		Node<K, V> curr = acquire(this.root, self); 
		int res = -1;
		while(curr!=null){
			res = k.compareTo(curr.key);
			if(res == 0) {
				oldValue = curr.value;
				break; 
			}
			prev = assign(prev,curr,self);
			if(res > 0){ //key > x.key
				curr = assign(curr, curr.right, self) ; 
			}else{
				curr = assign(curr, curr.left, self) ; 
			}
		}
		
		if(res!= 0) {	
			release(prev);
			release(curr);
			return oldValue;
		}
		Node<K, V> currL = acquire(curr.left,self); 
		Node<K, V> currR = acquire(curr.right,self); 
		boolean isLeft = prev.left == curr; 
		if (currL == null){ //no left child
			if(isLeft){
				prev.setChild(Node.Direction.LEFT,currR,self);
			}else {
				prev.setChild(Node.Direction.RIGHT,currR,self);
			}
			curr.setChild(Node.Direction.RIGHT,null,self);
		} else if (currR == null){ //no right child
			if(isLeft){
				prev.setChild(Node.Direction.LEFT,currL,self);
			}else {
				prev.setChild(Node.Direction.RIGHT,currL,self);
			}
			curr.setChild(Node.Direction.LEFT,null,self);
		}else { //both children
			Node<K, V> prevSucc = acquire(curr,self); //TODO re-acquire ?? 
			Node<K, V> succ =acquire(currR,self);  //TODO re-acquire ?? 
			Node<K, V> succL = acquire(succ.left,self); 
			while(succL != null){
				prevSucc =assign(prevSucc,succ,self);
				succ = assign(succ,succL,self);
				succL =  assign(succL,succ.left,self);
			}
			
			if (prevSucc != curr){	
				Node<K, V> succR=  acquire(succ.right,self); 
				prevSucc.setChild(Node.Direction.LEFT,succR,self);				
				succ.setChild(Node.Direction.RIGHT,currR,self);
				release(succR);
			}
			succ.setChild(Node.Direction.LEFT,currL,self);
			if (isLeft){
				prev.setChild(Node.Direction.LEFT,succ,self); 
			} else{
				prev.setChild(Node.Direction.RIGHT,succ,self); 
			}
			
			curr.setChild(Node.Direction.RIGHT,null,self);
			curr.setChild(Node.Direction.LEFT,null,self);
			release(prevSucc);
			release(succ);
			release(succL);
			
		}
		
		release(prev);
		release(curr);
		release(currL);
		release(currR);
		assert(prev ==null || prev.lockedBy()!=self );
		assert(curr ==null || curr.lockedBy()!=self);
		assert(currL ==null || currL.lockedBy()!=self);
		assert(currR ==null || currR.lockedBy()!=self);
		return oldValue; 
	}

	@Override
	public Collection<V> values() {
		 throw new RuntimeException("unimplemented method");
		// TODO Auto-generated method stub
		//return null;
	}

	@Override
	public V putIfAbsent(K key, V val) {
		final Comparable<? super K> k = comparable(key);
		Thread self = threadSelf.get();		
		V oldValue = null;
		Node<K, V> prev = null;
		Node<K, V> curr = acquire(this.root, self); 
		int res = -1;
		while(curr!=null){
			prev = assign(prev,curr,self);
			res = k.compareTo(curr.key);
			if(res == 0) {
				oldValue = curr.value;
				break; 
			}
			if(res > 0){ //key > x.key
				curr = assign(curr, curr.right, self) ; 
			}else{
				curr = assign(curr, curr.left, self) ; 
			}
		}
		if(res == 0) {
			//curr.value = val; 
			release(prev);
			release(curr);
			return oldValue;
		}
		Node<K, V> node = acquire(new Node<K, V>(key,val),self);
		if (res > 0 ) { 
			prev.setChild(Node.Direction.RIGHT,node,self); 
		} else {
			prev.setChild(Node.Direction.LEFT,node,self); 
		}
		release(prev);
		release(curr);
		release(node);
		return oldValue;
	}

	@Override
	public void clear() {
		acquire(root,threadSelf.get()); 
		root.setChild(Node.Direction.LEFT,null,threadSelf.get());
		release(root);
	}

	@Override
	public int size() {
		//NOT LINEARIZABLE
		return keySet().size();
	}

	@Override
	public int getRange(K[] result, K min, K max) {
		throw new RuntimeException("unimplemented method");
	}

}
