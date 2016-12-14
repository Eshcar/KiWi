package trees.sequential;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import trees.lockbased.lockremovalutils.SpinHeapReentrant;
import contention.abstractions.CompositionalMap;

public class BinaryTree<K,V> implements CompositionalMap<K, V>{
	
	private final Comparator<? super K> comparator;
	//private final K min; 
	private final Node<K, V> root; 
	
	public BinaryTree() {	
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

    	void setChild(final Direction dir, final Node<K,V> n) {
          
            if (dir == Direction.LEFT) {
               
                left = n;
            } else {
               
                right = n;
            }
        }
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
		Node<K, V> x = this.root; 
		while(x!=null){
			int res = k.compareTo(x.key);
			if(res == 0) break; 
			if(res > 0){ //key > x.key
				x=x.right; 
			}else{
				x=x.left;		
			}
		}
		if(x!=null) return x.value;
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
		V oldValue = null;
		Node<K, V> prev = null;
		Node<K, V> curr = this.root; 
		int res = -1;
		while(curr!=null){
			prev=curr;
			res = k.compareTo(curr.key);
			if(res == 0) {
				oldValue = curr.value;
				break; 
			}
			if(res > 0){ //key > x.key
				curr=curr.right; 
			}else{
				curr=curr.left;		
			}
		}
		if(res == 0) {
			curr.value = val; 
			return oldValue;
		}
		Node<K, V> node = new Node<K, V>(key,val); 
		if (res > 0 ) { 
			prev.right = node; 
		} else {
			prev.left = node; 
		}
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
		Node<K, V> prev = null;
		Node<K, V> curr = this.root; 
		V oldValue = null;
		int res = -1;
		
		while(curr!=null){			
			res = k.compareTo(curr.key);
			if(res == 0) {
				oldValue = curr.value;
				break; 
			}
			prev=curr;
			if(res > 0){ //key > x.key
				curr=curr.right; 
			}else{
				curr=curr.left;		
			}
		}
		if(res!= 0) {			
			return oldValue;
		}
		Node<K, V> currL = curr.left; 
		Node<K, V> currR = curr.right; 
		Node<K, V> prevL = prev.left;
		boolean isLeft = prevL == curr; 
		if (currL == null){ //no left child
			if(isLeft){
				prev.left = currR;
			}else {
				prev.right= currR;
			}
		} else if (currR == null){ //no right child
			if(isLeft){
				prev.left = currL;
			}else {
				prev.right = currL;
			}
		}else { //both children
			Node<K, V> prevSucc = curr; 
			Node<K, V> succ = currR; 
			Node<K, V> succL = succ.left; 
			while(succL != null){
				prevSucc = succ;
				succ = succL;
				succL = succ.left;
			}
			
			if (prevSucc != curr){	
				Node<K, V> succR= succ.right; 
				prevSucc.left = succR;				
				succ.right = currR;
			}
			succ.left = currL;
			if (isLeft){
				prev.left = succ; 
			} else{
				prev.right = succ;
			}
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
	public V putIfAbsent(K key, V val) {
		final Comparable<? super K> k = comparable(key);	
		V oldValue = null;
		Node<K, V> prev = null;
		Node<K, V> curr = this.root; 
		int res = -1;
		while(curr!=null){
			prev=curr;
			res = k.compareTo(curr.key);
			if(res == 0) {
				oldValue = curr.value;
				break; 
			}
			if(res > 0){ //key > x.key
				curr=curr.right; 
			}else{
				curr=curr.left;		
			}
		}
		if(res == 0) {
			//curr.value = val; 
			return oldValue;
		}
		Node<K, V> node = new Node<K, V>(key,val); 
		if (res > 0 ) { 
			prev.right = node; 
		} else {
			prev.left = node; 
		}
		return oldValue;
	}

	@Override
	public void clear() {
		root.setChild(Node.Direction.LEFT,null);
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
