package trees.lockbased;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import trees.lockbased.lockremovalutils.NonSharedFastSimpleRandom;
import trees.lockbased.lockremovalutils.SpinHeapReentrant;
import contention.abstractions.CompositionalMap;

public class DominationLockingTreap<K,V> implements CompositionalMap<K, V>{
	
	private static class KeyCmp<K> implements Comparable<K> {
        private final Comparator<K> cmp;
        private final K key;

        private KeyCmp(final Comparator<K> cmp, final K key) {
            this.cmp = cmp;
            this.key = key;
        }

        public int compareTo(final K rhs) {
            return cmp.compare(key, rhs);
        }
    }

    final Comparator<K> cmp;
    final TreapNode<K,V> rootHolder = new TreapNode<K,V>(null, null, 0);

    public DominationLockingTreap() {
        this(null);
    }

    public DominationLockingTreap(final Comparator<K> cmp) {
        this.cmp = cmp;
    }
	
    @SuppressWarnings("unchecked")
    private Comparable<K> comparable(final Object key) {
        return (cmp == null) ? (Comparable<K>) key : new KeyCmp<K>(cmp, (K) key);
    }
    
	/*Node*/
	private static class TreapNode<K,V> extends SpinHeapReentrant{
		/**
		 * Default 
		 */
		private static final long serialVersionUID = 1L;
		
		enum Direction {
            LEFT, RIGHT
        }
    	
    	public TreapNode(K key, V value,final int priority) {
    		this.key = key;
    		this.value = value;
    		this.priority = priority;
    		this.left = null;
    		this.right = null;
    	}
    	
    	final K key;
    	V value;
    	TreapNode<K, V> left;
    	TreapNode<K, V> right; 
    	final int priority;
    	 
    	void setChild(final Direction dir, final TreapNode<K,V> n, final Thread self) {
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
    
    private final ThreadLocal<NonSharedFastSimpleRandom> fastRandom = new ThreadLocal<NonSharedFastSimpleRandom>(){
        @Override
        protected NonSharedFastSimpleRandom initialValue()
        {
            return new NonSharedFastSimpleRandom(Thread.currentThread().getId());
        }
    };
    
    /*Helper functions*/
    
    private TreapNode<K,V> acquire(TreapNode<K,V> node, Thread self) {
		if (node != null) {
            node.acquire(self);
        }
        return node;
	}
	
	private void release(TreapNode<K,V> node) {
		if (node != null){
	           node.release();
		}
	}
	
	public TreapNode<K,V> assign(TreapNode<K,V> prevValue,
			TreapNode<K,V> newValue, Thread self) {
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
		return getImpl(comparable(key));
	}

	private V getImpl(final Comparable<K> key) {
    	Thread self = threadSelf.get();
    	
    	TreapNode<K,V> parent;
    	TreapNode<K,V> node;

        parent = (TreapNode<K, V>) acquire(this.rootHolder,self);
        node = (TreapNode<K, V>) acquire(parent.right,self);
        
        while (node != null) {
            final int c = key.compareTo(node.key);
            if (c == 0) {
                break;
            }
            parent = (TreapNode<K, V>) assign(parent, node, self); 
            if (c < 0) {
                node = (TreapNode<K, V>) assign(node, node.left, self);
            }
            else {
                node = (TreapNode<K, V>) assign(node, node.right, self);
            }
        }
        final V v = (node == null) ? null : node.value;
        release(parent);
        release(node);
        return v;
    }
	
	@Override
	public boolean isEmpty() {
		return rootHolder.right == null;
	}

	@Override
	public Set<K> keySet() {
		// Non Linearizable
		Set<K> keys = new HashSet<K>();
		getAllKeysImpl(rootHolder.right,keys);
		return keys;
	}
	
	private void getAllKeysImpl(TreapNode<K, V> node, Set<K> keys) {
		if(node!=null){
			keys.add(node.key);
			getAllKeysImpl(node.left, keys);
			getAllKeysImpl(node.right, keys);
		}
	}

	@Override
	public V put(K key, V value) {
		return putImpl(comparable(key), key, value);
	}
	
	private V putImpl(final Comparable<K> cmp, final K key, final V value) {
    	Thread self = threadSelf.get();
        V prevValue = null;
        
        final int prio = fastRandom.get().nextInt();
        
        TreapNode<K,V> parent;
        TreapNode<K,V> node;
        TreapNode.Direction dir = TreapNode.Direction.RIGHT;


        parent = acquire(this.rootHolder,self);
        node = acquire(parent.right,self);
        
        int cmpRes;
        while (node != null && prio <= node.priority) {
        	cmpRes = cmp.compareTo(node.key);
            if (cmpRes == 0) {
                break;
            }
            parent = assign(parent, node, self); 
            if (cmpRes < 0) {
                node = assign(node, node.left, self);
                dir = TreapNode.Direction.LEFT;
            }
            else {
                node = assign(node, node.right, self);
                dir = TreapNode.Direction.RIGHT;
            }
        }

        TreapNode<K,V> x = acquire(new TreapNode<K,V>(key, value, prio), self);
		TreapNode<K,V> lessParent = null;
		TreapNode<K,V> moreParent = null;
		TreapNode.Direction lessDir;
        TreapNode.Direction moreDir;      
        
        if (node == null){
            // simple
            parent.setChild(dir, x, self);
        }
        else {
            final int c0 = cmp.compareTo(node.key);
            if (c0 == 0) {
                // TODO: remove this node, then insert later with the new priority (prio must be > node.priority)

                // The update logic results in the post-update priority being
                // the minimum of the existing entry's and x's.  This skews the
                // uniform distribution slightly.
            	
            	//this is the old version:
            	prevValue = node.value;
            	node.value = value;
            }
            else {
                // TODO: update the existing node if it is a child of the current node
                parent.setChild(dir, x, self); // add the new node
                if (c0 < 0) {
                    x.setChild(TreapNode.Direction.RIGHT, node, self);  
                    moreParent = assign(moreParent,node,self);
                    moreDir = TreapNode.Direction.LEFT;
                    lessParent = assign(lessParent,x,self);
                    lessDir = TreapNode.Direction.LEFT;
                    node = assign(node,node.left,self);
                    
                    moreParent.setChild(TreapNode.Direction.LEFT, null, self);
                } else {
                	x.setChild(TreapNode.Direction.LEFT, node, self); 
                    lessParent = assign(lessParent,node,self);
                    lessDir = TreapNode.Direction.RIGHT;
                    moreParent =assign(moreParent,x,self);
                    moreDir = TreapNode.Direction.RIGHT;
                    node = assign(node,node.right,self);
                   
                    lessParent.setChild(TreapNode.Direction.RIGHT,null,self);
                }

                while (node != null) {
                    cmpRes = cmp.compareTo(node.key);
                    if (cmpRes == 0) {
                        lessParent.setChild(lessDir, node.left, self);
                        moreParent.setChild(moreDir, node.right, self);                
                        node.setChild(TreapNode.Direction.LEFT, null, self); //added 4/8/2014
                        node.setChild(TreapNode.Direction.RIGHT, null, self);
                        prevValue = node.value;
                        break;
                    }
                    else if (cmpRes < 0) {
                        moreParent.setChild(moreDir, node, self);
                        moreParent = assign(moreParent,node,self);
                        moreDir = TreapNode.Direction.LEFT;
                        node = assign(node,moreParent.left,self);
                        moreParent.setChild(TreapNode.Direction.LEFT, null, self);
                    }
                    else {
                        lessParent.setChild(lessDir, node, self);
                        lessParent = assign(lessParent,node,self);
                        lessDir = TreapNode.Direction.RIGHT;
                        node = assign(node,lessParent.right,self);
                        lessParent.setChild(TreapNode.Direction.RIGHT, null, self);
                    }
                }
            }
        }
        release(parent);
        release(node);
        release(moreParent);
        release(lessParent);
        release(x);
        return prevValue;
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
		 return removeImpl(comparable(key));
	}
	
	private V removeImpl(final Comparable<K> cmp) {
    	Thread self = threadSelf.get();
        V prevValue = null;
       
        TreapNode<K,V> parent;
        TreapNode<K,V> node;
        TreapNode<K,V> nL = null;
        TreapNode<K,V> nR = null;
        TreapNode.Direction dir = TreapNode.Direction.RIGHT;

        parent = acquire(this.rootHolder,self);
        node = acquire(parent.right,self);
        
        int cmpRes;
        while (node != null) {
        	cmpRes = cmp.compareTo(node.key);
            if (cmpRes == 0) {
            	prevValue = node.value;
            	break;
            }
            parent = assign(parent, node, self); 
            if (cmpRes < 0) {
                node = assign(node, node.left, self);
                dir = TreapNode.Direction.LEFT;
            }
            else {
                node = assign(node, node.right, self);
                dir = TreapNode.Direction.RIGHT;
            }
        }

        while (node != null) {
            if (node.left == null) {
                parent.setChild(dir, node.right, self);
                break;
            }
            else if (node.right == null) {
                parent.setChild(dir, node.left, self);
                break;
            }
            else {
                nL = assign(nL,node.left,self);
                nR = assign(nR,node.right,self);
            
                if (nL.priority > nR.priority) {
                	TreapNode<K, V> nLR = acquire(nL.right,self); // ???
                    node.setChild(TreapNode.Direction.LEFT, nLR, self);
                    parent.setChild(dir, nL, self);
                    nL.setChild(TreapNode.Direction.RIGHT, node, self);
             
                    parent = assign(parent,nL,self);
                    dir = TreapNode.Direction.RIGHT;
                    release(nLR); //???
                }
                else {
                    node.setChild(TreapNode.Direction.RIGHT, nR.left, self);
                    parent.setChild(dir, nR, self);
                    nR.setChild(TreapNode.Direction.LEFT, node, self);
               
                    parent = assign(parent,nR,self);
                    dir = TreapNode.Direction.LEFT;
                }
            }
        }

        // code that prevents treeness violation for an object that happens to
        // be unreachable
        if (node != null) {
            node.setChild(TreapNode.Direction.LEFT, null, self);
            node.setChild(TreapNode.Direction.RIGHT, null, self);
        }
        release(parent);
        release(node);
        release(nL);
        release(nR);
        return prevValue;
    }

	@Override
	public Collection<V> values() {
		throw new RuntimeException("unimplemented method");
		// TODO Auto-generated method stub
		//return null;
	}

	@Override
	public V putIfAbsent(K k, V v) {
		// Cannot be implemented, use put instead 
		return put(k,v);
	}

	@Override
	public void clear() {
		 rootHolder.right = null;
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
