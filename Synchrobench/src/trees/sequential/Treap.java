package trees.sequential;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;



import trees.lockbased.lockremovalutils.NonSharedFastSimpleRandom;
import contention.abstractions.CompositionalMap;

public class Treap<K,V> implements CompositionalMap<K, V>{
	
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
    final Node<K,V> rootHolder = new Node<K,V>(null, null, 0);

    public Treap() {
        this(null);
    }

    public Treap(final Comparator<K> cmp) {
        this.cmp = cmp;
    }
	
    @SuppressWarnings("unchecked")
    private Comparable<K> comparable(final Object key) {
        return (cmp == null) ? (Comparable<K>) key : new KeyCmp<K>(cmp, (K) key);
    }
    
	/*Node*/
	private static class Node<K,V>{
		/**
		 * Default 
		 */
		private static final long serialVersionUID = 1L;
		
		enum Direction {
            LEFT, RIGHT
        }
    	
    	public Node(K key, V value,final int priority) {
    		this.key = key;
    		this.value = value;
    		this.priority = priority;
    		this.left = null;
    		this.right = null;
    	}
    	
    	final K key;
    	V value;
    	Node<K, V> left;
    	Node<K, V> right; 
    	final int priority;
    	
    	void setChild(final Direction dir, final Node<K,V> n) {
            if (dir == Direction.LEFT) {
                left = n;
            } else {
                right = n;
            }
        }

	}

	private final ThreadLocal<NonSharedFastSimpleRandom> fastRandom = new ThreadLocal<NonSharedFastSimpleRandom>(){
        @Override
        protected NonSharedFastSimpleRandom initialValue()
        {
            return new NonSharedFastSimpleRandom(Thread.currentThread().getId());
        }
    };
    
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
    	Node<K,V> parent;
        Node<K,V> node;

        parent = this.rootHolder;
        node = parent.right;
        
        while (node != null) {
            final int c = key.compareTo(node.key);
            if (c == 0) {
                break;
            }
            parent = node;
            if (c < 0) {
                node = node.left;
            }
            else {
                node = node.right;
            }
        }
        final V v = (node == null) ? null : node.value;
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
	
	private void getAllKeysImpl(Node<K, V> node, Set<K> keys) {
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
    	V prev = null;
        
        final int prio = fastRandom.get().nextInt();
        Node<K,V> x = new Node<K,V>(key, value, prio);
        
        Node<K,V> parent;
        Node<K,V> node;
        Node.Direction dir = Node.Direction.RIGHT;
        // parent.[dir] == node

        parent = this.rootHolder;         
        node = parent.right;

        while (node != null && prio <= node.priority) {
            final int c = cmp.compareTo(node.key);
            if (c == 0) {
                break;
            }
        
            parent = node;
            if (c < 0) {
                node = node.left;               
                dir = Node.Direction.LEFT;
            }
            else {
                node = node.right;               
                dir = Node.Direction.RIGHT;
            }
        }

        Node<K,V> lessParent = null;
        Node<K,V> moreParent = null;
        Node.Direction lessDir;
        Node.Direction moreDir;
        if (node == null) {
            // simple
            parent.setChild(dir, x);
      
        }
        else {
            final int c0 = cmp.compareTo(node.key);
            if (c0 == 0) {
                // TODO: remove this node, then insert later with the new priority (prio must be > node.priority)

                // The update logic results in the post-update priority being
                // the minimum of the existing entry's and x's.  This skews the
                // uniform distribution slightly.
                prev = node.value;
                node.value = value;
            }
            else {
                // TODO: update the existing node if it is a child of the current node

                parent.setChild(dir, x); // add the new node
            

                if (c0 < 0) {
                    x.right = node;
                    moreParent = node;
                    moreDir = Node.Direction.LEFT;
                    lessParent = x;
                    lessDir = Node.Direction.LEFT;
                    node = node.left;
                    
                    moreParent.left = null;
                } else {
                    x.left = node;
                    lessParent = node;
                    lessDir = Node.Direction.RIGHT;
                    moreParent = x;
                    moreDir = Node.Direction.RIGHT;
                    node = node.right;
                   
                    lessParent.right = null;
                }

                while (node != null) {
                    final int c = cmp.compareTo(node.key);
                    if (c == 0) {
                        lessParent.setChild(lessDir, node.left);
                        moreParent.setChild(moreDir, node.right);
                        prev = node.value;
                        break;
                    }
                    else if (c < 0) {
                        moreParent.setChild(moreDir, node);
                   
                        moreParent = node;
                        moreDir = Node.Direction.LEFT;
                        node = moreParent.left;
                       
                        moreParent.left = null;
                    }
                    else {
                        lessParent.setChild(lessDir, node);
                       
                        lessParent = node;
                        lessDir = Node.Direction.RIGHT;
                        node = lessParent.right;
                       
                        lessParent.right = null;
                    }
                }
            }
        }
        return prev;
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
    	V prev = null;

        Node<K,V> parent;
        Node<K,V> node;
        Node<K,V> nL = null;
        Node<K,V> nR = null;
        Node.Direction dir = Node.Direction.RIGHT;

        parent = this.rootHolder;       
        node = parent.right;
        
        while (node != null) {
            final int c = cmp.compareTo(node.key);
            if (c == 0) {
                prev = node.value;
                break;
            }
            parent = node;
            if (c < 0) {
                node = node.left;
                dir = Node.Direction.LEFT;
            }
            else {
                node = node.right;
                dir = Node.Direction.RIGHT;
            }
        }

        while (node != null) {
            if (node.left == null) {
                parent.setChild(dir, node.right);
                break;
            }
            else if (node.right == null) {
                parent.setChild(dir, node.left);
                break;
            }
            else {
                nL = node.left;
                nR = node.right;
            
                if (nL.priority > nR.priority) {
                    node.left = nL.right;
                    parent.setChild(dir, nL);
                    nL.right = node;
             
                    parent = nL;
                    dir = Node.Direction.RIGHT;
                }
                else {
                    node.right = nR.left;
                    parent.setChild(dir, nR);
                    nR.left = node;
               
                    parent = nR;
                    dir = Node.Direction.LEFT;
                }
            }
        }

        // code that prevents treeness violation for an object that happens to
        // be unreachable
        if (node != null) {
            node.left = null;
            node.right = null;
        }
        return prev;
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

