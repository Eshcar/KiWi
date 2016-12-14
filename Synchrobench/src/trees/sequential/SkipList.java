package trees.sequential;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import contention.abstractions.CompositionalMap;

public class SkipList<K,V> implements CompositionalMap<K, V> {
	private final Comparator<? super K> comparator;
	private final int maxKey;
	private final int maxLevel;
	private final int maxHeight;
	private final Node<K, V> root; 
	private final int min;
	private final int max;
	private final Node<K,V> sentenial;
	
	/*Constructor*/
	public SkipList(){
		this.maxKey = 2000000;
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
	private static class Node<K, V> {		
	
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
    
    private final ThreadLocal<Object[]> threadPreds = new ThreadLocal<Object[]>();
	private final ThreadLocal<Object[]> threadSuccs = new ThreadLocal<Object[]>();
	
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
	
	/*Map functions*/
	
	@Override
	public boolean containsKey(Object key) {
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
		final Comparable<? super K> k = comparable(key);
		return getImp(k);
	}

	private V getImp(Comparable<? super K> cmp) {
		V value = null;
		Node<K,V> pred = root;
		for(int layer = maxLevel ; layer> -1 ; layer-- ){
			Node<K,V> curr = (Node<K, V>) pred.next[layer];
			while (curr!=null) {
				int res = cmp.compareTo(curr.key);
				if(res == 0) {
					return curr.value;
				}
				if(res < 0){ //key < x.key
					break;
				}			
				pred = curr; 		
				curr = (Node<K, V>) pred.next[layer];
			} 
		}
		return value;
	}

	@Override
	public boolean isEmpty() {
		return root.next[0]==sentenial;
	}

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
		return putImpl(comparable(key),key,value);
	}
	
	@SuppressWarnings("unchecked")
	private V putImpl(final Comparable<? super K> cmp, final K key, final V value) {
		V oldValue = null;
		int height = skipListRandom.get().randomHeight(maxHeight);
		Object[] preds = threadPreds.get();
		Object[] succs = threadSuccs.get();
		
		Node<K,V> pred = root; 
		for(int layer = maxLevel ; layer> -1 ; layer-- ){
			Node<K,V> curr = (Node<K, V>) pred.next[layer];
			while (curr!=null) {
				int res = cmp.compareTo(curr.key);
				if(res == 0) {
					oldValue = curr.value;
					break; 
				}
				if(res < 0){ //key < x.key
					break;
				}			
				pred = curr; 		
				curr = (Node<K, V>) pred.next[layer];
	
			}
			preds[layer] = pred;
			succs[layer] = curr;
		}
		
		if( succs[0]!=null && ((Node<K,V>)succs[0]).key == key ){
			//key was found change value only... 
			for(int i = maxHeight-1 ; i > -1 ; i--){
				Node<K,V> curr = (Node<K,V>)succs[i];
				if( curr !=null && curr.key == key ){
					curr.value = value;
				}
			}
			
			return oldValue;
		}
		
		Node<K,V> node = new Node<K,V>(key,value,height);
		for(int i = height-1 ; i > -1 ; i--){
			pred = ((Node<K,V>) preds[i]);
			node.next[i] = pred.next[i];
			pred.next[i] = node;
		}
		
		return oldValue;	
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
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
		return removeImpl(comparable(key));
	}

	@SuppressWarnings("unchecked")
	private V removeImpl(Comparable<? super K> cmp) {
		V oldValue = null;
		int layerFound = -1; 
		Object[] preds = threadPreds.get();
		Object[] succs = threadSuccs.get();
		
		Node<K,V> pred = root; 
		for(int layer = maxLevel ; layer> -1 ; layer-- ){
			Node<K,V> curr = (Node<K, V>) pred.next[layer];
			while (curr!=null) {
				int res = cmp.compareTo(curr.key);
				if(res == 0) {
					oldValue = curr.value;
					if(layerFound == -1){ layerFound=layer; }
					break; 
				}
				if(res < 0){ //key < x.key
					break;
				}			
				pred = curr; 		
				curr = (Node<K, V>) pred.next[layer];
	
			}
			preds[layer] = pred;
			succs[layer] = curr;
		}
		
		for(int i = layerFound ; i > -1 ; i--){
			pred = ((Node<K,V>) preds[i]);
			Node<K,V> succ = ((Node<K,V>) succs[i]);
			pred.next[i] = succ.next[i]; 
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
		return putIfAbsentImpl(comparable(key),key,value);
	}

	private V putIfAbsentImpl(Comparable<? super K> cmp, K key, V value) {
		V oldValue = null;
		int height = skipListRandom.get().randomHeight(maxHeight);
		Object[] preds = threadPreds.get();
		Object[] succs = threadSuccs.get();
		
		Node<K,V> pred = root; 
		for(int layer = maxLevel ; layer> -1 ; layer-- ){
			Node<K,V> curr = (Node<K, V>) pred.next[layer];
			while (curr!=null) {
				int res = cmp.compareTo(curr.key);
				if(res == 0) {
					oldValue = curr.value;
					return oldValue; 
				}
				if(res < 0){ //key < x.key
					break;
				}			
				pred = curr; 		
				curr = (Node<K, V>) pred.next[layer];
	
			}
			preds[layer] = pred;
			succs[layer] = curr;
		}
		
		Node<K,V> node = new Node<K,V>(key,value,height);
		for(int i = height-1 ; i > -1 ; i--){
			pred = ((Node<K,V>) preds[i]);
			node.next[i] = pred.next[i];
			pred.next[i] = node;
		}
		
		return oldValue;	
	}

	@Override
	public void clear() {
		for(int layer = maxLevel; layer>-1; layer--){
			root.next[layer]=sentenial; 
		}
	}

	@Override
	public int size() {
		return keySet().size();
	}

	@Override
	public int getRange(K[] result, K min, K max) {
		return getRangeImpl(result, comparable(min), comparable(max));
	}
	
	@SuppressWarnings("unchecked")
	private int getRangeImpl(K[] result, Comparable<? super K> cmpMin,
			Comparable<? super K> cmpMax) {
		int rangeCount = 0; 
		int res;
		Node<K,V> curr = root;
		Node<K,V> pred = root;
		
		for(int layer = maxLevel ; layer> -1 ; layer-- ){
			curr = (Node<K, V>) pred.next[layer];
			while (true) {
				res = cmpMin.compareTo(curr.key);
				if(res == 0) {
					break; 
				}
				if(res < 0){ 
					break;
				}			
				pred = curr;
				curr = (Node<K, V>) pred.next[layer];
			}
		}
		
		while(cmpMax.compareTo(curr.key) >= 0){		
			result[rangeCount] = curr.key; 
			rangeCount++;
			pred = curr; 
			curr = (Node<K, V>) curr.next[0]; 
		}
		
		return rangeCount;
	}

	

}
