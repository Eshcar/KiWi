package trees.lockbased;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import trees.lockbased.lockremovalutils.SpinHeapReentrant;
import contention.abstractions.CompositionalMap;
import contention.benchmark.Parameters;

public class Simple2PLSkiplist<K,V> implements CompositionalMap<K, V> {
	private final Comparator<? super K> comparator;
	private final int maxKey;
	private final int maxLevel;
	private final int maxHeight;
	private final Node<K, V> root; 
	private final int min;
	private final int max;
	private final Node<K,V> sentenial;

	
	/*Constructor*/
	public Simple2PLSkiplist(){
		this.maxKey = Parameters.range;
		this.maxHeight = (int) Math.ceil(Math.log(maxKey) / Math.log(2));
		this.maxLevel = maxHeight-1; 
		this.max = Integer.MAX_VALUE; 
		this.min = Integer.MIN_VALUE;
		this.sentenial = new Node(max,null,maxHeight);
		this.root = new Node(min,null,maxHeight,sentenial);
		//this.maxRangeSize = maxRangeSize;
        this.comparator = null;
	}
	
	/*Node*/
	private static class Node<K, V>  extends SpinHeapReentrant{		
		private static final long serialVersionUID = 1L;
		
		public Node(K key, V value, int height) {
			this.key = key;
			this.value = value;
			this.next = new Object[height];
			for (int i=0; i< height; i++){
				this.next[i]=null;
			}
		}
		
		public Node(K key, V value, int height, Node<K,V> next) {
			this.key = key;
			this.value = value;
			this.next = new Object[height];
			for (int i=0; i< height; i++){
				this.next[i]=next;
			}
		}
		
		final K key;
		V value;
		Object[] next;
	}
	
	private static class SkipListRandom {
		int seed; 
		
		public SkipListRandom(){
			seed = new Random(System.currentTimeMillis()).nextInt()
	                | 0x0100;
		}
		
		int randomHeight(int maxHeight) {
	        int x = seed;
	        x ^= x << 13;
	        x ^= x >>> 17;
	        seed = (x ^= x << 5);
	        if ((x & 0x8001) != 0) {
	            return 1;
	        }
	        int level = 1;
	        while (((x >>>= 1) & 1) != 0) {
	            ++level;
	        }
	        return Math.min(level + 1, maxHeight);
	    }
	}
	
	/*Thread Locals*/
	private final ThreadLocal<SkipListRandom> skipListRandom = new ThreadLocal<SkipListRandom>(){
        @Override
        protected SkipListRandom initialValue()
        {
            return new SkipListRandom();
        }
    };
    
    private final ThreadLocal<Thread> self = new ThreadLocal<Thread>(){
        @Override
        protected Thread initialValue()
        {
            return Thread.currentThread();
        }
    };
    
    private final ThreadLocal<Object[]> threadPreds = new ThreadLocal<Object[]>();
	private final ThreadLocal<Object[]> threadSuccs = new ThreadLocal<Object[]>();
	
	private final ThreadLocal<SpinHeapReentrant[]> threadLocked = new ThreadLocal<SpinHeapReentrant[]>(){
		 @Override
        protected SpinHeapReentrant[] initialValue()
        {
            return new SpinHeapReentrant[maxHeight*10+Parameters.maxRangeSize];
        }
	 };
    
	/*Helper functions*/
	
	@SuppressWarnings("unchecked")
	private Comparable<? super K> comparable(final Object object) {

		if (object == null) throw new NullPointerException();
		if (comparator == null) return (Comparable<? super K>)object;

		return new Comparable<K>() {
			final Comparator<? super K> cmp = comparator;
			final K obj = (K) object;

			public int compareTo(final K other) { 
				return cmp.compare(obj, other); 
			}
		};
	}
	
	public Node<K,V> acquire(Node<K,V> node, Thread self) {
		if (node != null) {
            node.acquire(self);
        }
        return node;
	}
	
	private void release(SpinHeapReentrant node) {
		if (node != null){
	           node.release();
		}
	}
	
	/*Map functions*/
	@Override
	public boolean containsKey(Object arg0) {
		throw new RuntimeException("unimplemented method");
		// TODO Auto-generated method stub
		//return false;
	}

	@Override
	public boolean containsValue(Object value) {
		throw new RuntimeException("unimplemented method");
		// TODO Auto-generated method stub
		//return false;
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		throw new RuntimeException("unimplemented method");
		// TODO Auto-generated method stub
		//return false;
	}

	@Override
	public V get(Object key) {
		if(threadPreds.get() == null){
			threadPreds.set(new Object[maxHeight]);
			threadSuccs.set(new Object[maxHeight]);
		}
		return getImp(comparable(key),(K) key,self.get());
	}

	@SuppressWarnings("unchecked")
	private V getImp(Comparable<? super K> cmp, K key, Thread self) {
		Object[] preds = threadPreds.get();
		Object[] succs = threadSuccs.get();
		SpinHeapReentrant[] locked = threadLocked.get();
		int l =0; 
	
		V value = null;
		Node<K,V> pred = root; 
		pred.acquire(self);
		locked[l] = pred;
		l++;
		for(int layer = maxLevel ; layer> -1 ; layer-- ){
			Node<K,V> curr = (Node<K, V>) pred.next[layer];
			curr.acquire(self);
			locked[l] = curr;
			l++;
			while (true) {
				int res = cmp.compareTo(curr.key);
				if(res == 0) {
					value = curr.value;
					for(int j=0;j<l;j++){
						release(locked[j]);
					}
					return value;
				}
				if(res < 0){ //key < curr.key
					break;
				}
				//pred.release();
				pred = curr;
				curr = (Node<K, V>) pred.next[layer];
				curr.acquire(self);
				locked[l] = curr;
				l++;
			}			
			preds[layer] = pred;
			succs[layer] = curr;		
			if(layer != 0){
				pred.acquire(self);
				locked[l] = pred;
				l++;
			}
		}
		for(int j=0;j<l;j++){
			release(locked[j]);
		}
		return value;
	}
	
	@Override
	public boolean isEmpty() {
		return root.next[0]==sentenial;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Set<K> keySet() {
		Set<K> hash = new HashSet<K>(); 
		Node<K,V> prev = root; 
		
		prev = (Node<K, V>) prev.next[0];
		while(comparable(prev.key).compareTo(sentenial.key)!=0){
			hash.add(prev.key); 
			prev = (Node<K, V>) prev.next[0];
		}
		return hash;
	}

	@Override
	public V put(K key, V value) {
		if(threadPreds.get() == null){
			threadPreds.set(new Object[maxHeight]);
			threadSuccs.set(new Object[maxHeight]);
		}
		return putImpl(comparable(key),key,value,self.get());
	}
	
	@SuppressWarnings("unchecked")
	private V putImpl(final Comparable<? super K> cmp, final K key, final V value, Thread self) {
		V oldValue = null;
		int height = skipListRandom.get().randomHeight(maxHeight-1);
		int layerFound = -1; 
		Object[] preds = threadPreds.get();
		Object[] succs = threadSuccs.get();
		SpinHeapReentrant[] locked = threadLocked.get();
		int l =0; 
		
		Node<K,V> pred = root; 
		pred.acquire(self);
		locked[l] = pred;
		l++;
		for(int layer = maxLevel ; layer> -1 ; layer-- ){ 
			Node<K,V> curr = (Node<K, V>) pred.next[layer];
			curr.acquire(self);
			locked[l] = curr;
			l++;
			while (true) {
				int res = cmp.compareTo(curr.key);
				if(res == 0) {
					if(layerFound==-1){
						layerFound = layer; 				
						oldValue = curr.value;
					}				
					break; 
				}
				if(res < 0){ //key < x.key
					break;
				}			
				//pred.release();
				pred = curr;
				curr = (Node<K, V>) pred.next[layer];
				curr.acquire(self);
				locked[l] = curr;
				l++;
	
			}
			preds[layer] = pred;
			succs[layer] = curr;
			if(layer != 0){
				pred.acquire(self);
				locked[l] = pred;
				l++;
			}
		}
		
		if( layerFound!= -1 ){ 	//key was found change value only... 
			
			//Lock needed nodes
			for(int i = layerFound ; i > -1 ; i--){
				((Node<K,V>)preds[i]).acquire(self);
				((Node<K,V>)succs[i]).acquire(self);
			}
			
			//unlock other nodes
			for(int i=0;i<l;i++){
				release(locked[i]);
			}
			
			for(int i = layerFound ; i > -1 ; i--){	
				Node<K,V> curr = (Node<K,V>)succs[i];
				pred = ((Node<K,V>) preds[i]);
				if( cmp.compareTo(curr.key )!= 0 ){
					assert(false);
				};
				curr.value = value;
				pred.release();
				curr.release();
			}
			return oldValue;
		}
		
		//Lock needed nodes
		for(int i = height-1 ; i > -1 ; i--){
			((Node<K,V>)preds[i]).acquire(self);
			((Node<K,V>)succs[i]).acquire(self);
		}
		
		//unlock other nodes
		for(int i=0;i<l;i++){
			release(locked[i]);
		}
		
		Node<K,V> node = new Node<K,V>(key,value,height);
		node.acquire(self);
		for(int i = height-1 ; i > -1 ; i--){
			pred = ((Node<K,V>) preds[i]);
			Node<K,V> succ = ((Node<K,V>) succs[i]);
			node.next[i] = succ;	
			pred.next[i] = node;
			pred.release();
			succ.release();
		}
		node.release();
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
		if(threadPreds.get() == null){
			threadPreds.set(new Object[maxHeight]);
			threadSuccs.set(new Object[maxHeight]);
		}
		return removeImpl(comparable(key),(K) key,self.get());
	}

	@SuppressWarnings("unchecked")
	private V removeImpl(Comparable<? super K> cmp, K key, Thread self) {
		V oldValue = null;
		int layerFound = -1; 
		Object[] preds = threadPreds.get();
		Object[] succs = threadSuccs.get();
		SpinHeapReentrant[] locked = threadLocked.get();
		int l =0; 
		
		Node<K,V> pred = root;
		pred.acquire(self);
		locked[l] = pred;
		l++;
		for(int layer = maxLevel ; layer > -1 ; layer-- ){
			Node<K,V> curr = (Node<K, V>) pred.next[layer];
			curr.acquire(self);
			locked[l] = curr;
			l++;
			while (true) {
				int res = cmp.compareTo(curr.key);
				if(res == 0) {
					oldValue = curr.value;
					if(layerFound == -1){ layerFound = layer; }
					break; 
				}
				if(res < 0){ //key < x.key
					break;
				}			
				//pred.release();
				pred = curr;
				curr = (Node<K, V>) pred.next[layer];
				curr.acquire(self);
				locked[l] = curr;
				l++;
			}
			preds[layer] = pred;
			succs[layer] = curr;
			if(layer != 0){
				pred.acquire(self);
				locked[l] = pred;
				l++;
			}
		}
		
		//lock needed nodes
		for(int i = layerFound ; i > -1 ; i--){
			pred = ((Node<K,V>) preds[i]);
			Node<K,V> succ = ((Node<K,V>) succs[i]);
			pred.acquire(self);
			succ.acquire(self);
		}
		
		//unlock other nodes
		for(int i=0;i<l;i++){
			release(locked[i]);
		}
		
		for(int i = layerFound ; i > -1 ; i--){
			pred = ((Node<K,V>) preds[i]);
			Node<K,V> succ = ((Node<K,V>) succs[i]);
			pred.next[i] = succ.next[i];
			pred.release();
			succ.release();
		}
		
		return oldValue;
	}
	
	@Override
	public Collection<V> values() {
		throw new RuntimeException("unimplemented method");
		// TODO Auto-generated method stub
		//return null;
	}

	@Override
	public V putIfAbsent(K key, V value) {
		if(threadPreds.get() == null){
			threadPreds.set(new Object[maxHeight]);
			threadSuccs.set(new Object[maxHeight]);
		}
		return putIfAbsentImpl(comparable(key),key,value,self.get());
	}

	@SuppressWarnings("unchecked")
	private V putIfAbsentImpl(Comparable<? super K> cmp, K key, V value,
			Thread self) {
		V oldValue = null;
		int height = skipListRandom.get().randomHeight(maxHeight-1);
		Object[] preds = threadPreds.get();
		Object[] succs = threadSuccs.get();
		SpinHeapReentrant[] locked = threadLocked.get();
		int l =0; 
		
		Node<K,V> pred = root; 
		pred.acquire(self);
		locked[l] = pred;
		l++;
		for(int layer = maxLevel ; layer> -1 ; layer-- ){
			Node<K,V> curr = (Node<K, V>) pred.next[layer];
			curr.acquire(self);
			locked[l] = curr;
			l++;
			while (true) {
				int res = cmp.compareTo(curr.key);
				if(res == 0) {	
					oldValue = curr.value;
					for(int i=0;i<l;i++){
						release(locked[i]);
					}
					return oldValue;
				}
				if(res < 0){ //key < x.key
					break;
				}			
				//pred.release();
				pred = curr;
				curr = (Node<K, V>) pred.next[layer];
				curr.acquire(self);
				locked[l] = curr;
				l++;
	
			}
			preds[layer] = pred;
			succs[layer] = curr;
			if(layer != 0){
				pred.acquire(self);
				locked[l] = pred;
				l++;
			}
		}
		
		//Lock needed nodes
		for(int i = height-1 ; i > -1 ; i--){
			((Node<K,V>)preds[i]).acquire(self);
			((Node<K,V>)succs[i]).acquire(self);
		}
		
		//unlock other nodes
		for(int i=0;i<l;i++){
			release(locked[i]);
		}
		
		Node<K,V> node = new Node<K,V>(key,value,height);
		node.acquire(self);
		for(int i = height-1 ; i > -1 ; i--){
			pred = ((Node<K,V>) preds[i]);
			Node<K,V> succ = ((Node<K,V>) succs[i]);
			node.next[i] = succ;	
			pred.next[i] = node;
			pred.release();
			succ.release();
		}
		node.release();
		return oldValue;	
	}

	@Override
	public void clear() {
		//non-concurrent
		for(int layer = maxLevel; layer>-1; layer--){
			root.next[layer]=sentenial; 
		}
	}

	@Override
	public int size() {
		//use this for ranges! 
		return keySet().size();
	}
	
	@Override
	public int getRange(K[] result, K min, K max) {
		if(threadPreds.get() == null){
			threadPreds.set(new Object[maxHeight]);
			threadSuccs.set(new Object[maxHeight]);
		}
		return dominationRangeImpl(result, comparable(min), comparable(max),self.get());
	}
	
	@SuppressWarnings("unchecked")
	private int dominationRangeImpl(K[] result, Comparable<? super K> cmpMin,
		Comparable<? super K> cmpMax, Thread self) {
		//Object[] result = rangeSet.get();
		int rangeCount = 0; 
		
		int layerFound = -1; 
		Object[] preds = threadPreds.get();
		Object[] succs = threadSuccs.get();
		SpinHeapReentrant[] locked = threadLocked.get();
		int l =0;
		
		Node<K,V> pred = root; 
		pred.acquire(self);
		locked[l] = pred;
		l++;
		for(int layer = maxLevel ; layer> -1 ; layer-- ){
			Node<K,V> curr = (Node<K, V>) pred.next[layer];
			curr.acquire(self);
			locked[l] = curr;
			l++;
			while (true) {
				int res = cmpMin.compareTo(curr.key);
				if(res == 0) {
					if(layerFound==-1){
						layerFound = layer; 				
					}				
					break; 
				}
				if(res < 0){ 
					break;
				}			
				//pred.release();
				pred = curr;
				curr = (Node<K, V>) pred.next[layer];
				curr.acquire(self);
				locked[l] = curr;
				l++;
	
			}
			preds[layer] = pred;
			succs[layer] = curr;
			if(layer != 0){
				//pred = pred.down;
				pred.acquire(self);
				locked[l] = pred;
				l++;
			}
		}
		
		pred = (Node<K, V>) preds[0];
		Node<K, V> curr = (Node<K, V>) succs[0]; 
		while(cmpMax.compareTo(curr.key) >= 0){		
			result[rangeCount] = curr.key;
			rangeCount++;
			pred = curr; 
			curr = (Node<K, V>) curr.next[0]; 
			curr.acquire(self);	
			locked[l] = curr;
			l++;
		}
		
		for(int i=0;i<l;i++){
			release(locked[i]);
		}
		return rangeCount;
	}
}
