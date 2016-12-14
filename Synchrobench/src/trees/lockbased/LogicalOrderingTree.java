package trees.lockbased;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

import contention.abstractions.CompositionalMap;

public class LogicalOrderingTree<K, V> implements CompositionalMap<K, V> {
	
	final private LogicalOrdaringNode<K,V> root;
	
	private Comparator<? super K> comparator;
	
	public LogicalOrderingTree(){
		LogicalOrdaringNode<K,V> parent = new LogicalOrdaringNode(Integer.MIN_VALUE);
		root = new LogicalOrdaringNode(Integer.MAX_VALUE, null, parent, parent);
		root.parent = parent;
		parent.rightChild = root;
		parent.setSuccessor(root);
	}
	
	public LogicalOrderingTree(final Comparator<? super K> comparator, K min, K max) {
		this(min, max);
		this.comparator = comparator;
	}
	
	public LogicalOrderingTree(K min, K max) {
		LogicalOrdaringNode<K,V> parent = new LogicalOrdaringNode<K,V>(min);
		root = new LogicalOrdaringNode<K,V>(max, null, parent, parent);
		root.parent = parent;
		parent.rightChild = root;
		parent.setSuccessor(root);
	}
	
	@SuppressWarnings("unchecked")
	private Comparable<? super K> comparable(final Object key) {
		if (key == null) {
			throw new NullPointerException();
		}
		if (comparator == null) {
			return (Comparable<? super K>)key;
		}
		return new Comparable<K>() {
			final Comparator<? super K> _cmp = comparator;

			@SuppressWarnings("unchecked")
			public int compareTo(final K rhs) { return _cmp.compare((K)key, rhs); }
		};
	}


	@Override
	public V putIfAbsent(K val, V item) {
		final Comparable<? super K> value = comparable(val);
		LogicalOrdaringNode<K,V> node = null;
		K nodeValue = null;
		int res = -1;
		while (true) {
			node = root;
			LogicalOrdaringNode<K,V> child;
			res = -1;
			while (true) {
				if (res == 0) break;
				if (res > 0) {
					child = node.rightChild;
				} else {
					child = node.leftChild;
				}
				if (child == null) break;
				node = child;
				nodeValue = node.value;
				res = value.compareTo(nodeValue);
			}
			res = value.compareTo(node.value);
			final LogicalOrdaringNode<K,V> pred = res > 0 ? node : node.predecessor;
			pred.lockSuccessor();
			if (pred.valid) {
				final K predVal = pred.value;
				final int predRes = pred == node? res: value.compareTo(predVal);
				if (predRes > 0) {
					final LogicalOrdaringNode<K,V> succ = pred.successor;
					final K succVal = succ.value;
					final int res2 = succ == node? res: value.compareTo(succVal);
					if (res2 <= 0) {
						if (res2 == 0) {
							V item2 = (V) succ.item;
							pred.unlockSuccessor();
							return item2;
						}
						LogicalOrdaringNode<K,V> newNode = new LogicalOrdaringNode<K,V>(val, item, pred, succ);
						newNode.lockSuccessor();
						succ.setPredecessor(newNode);
						pred.setSuccessor(newNode);
						updateLayoutUponInsertion(node, pred, succ, newNode);
						newNode.unlockSuccessor();
						pred.successorLock.unlock();
						return null;
					}
				}
			}
			pred.unlockSuccessor();
		}
	}

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
	public V get(Object inputVal) {
		final Comparable<? super K> value = comparable(inputVal);
		
		LogicalOrdaringNode<K,V> node = root;
		LogicalOrdaringNode<K,V> child;
		int res = -1;
		K val;
		while (true) {
			if (res == 0) break;
			if (res > 0) {
				child = node.rightChild;
			} else {
				child = node.leftChild;
			}
			if (child == null) break;
			node = child;
			val = node.value;
			res = value.compareTo(val);
		}
		while (res < 0) {
			node = node.predecessor;
			val =  node.value;
			res = value.compareTo(val);
		}
		while (res > 0) {
			node = node.successor;
			val =  node.value;
			res = value.compareTo(val);
		} 
		if(res == 0 && node.valid){
			return (V) node.item;
		}
		return null;
	}

	@Override
	public boolean isEmpty() {
		throw new RuntimeException("unimplemented method");
		// TODO Auto-generated method stub
		//return false;
	}

	@Override
	public Set<K> keySet() {
		throw new RuntimeException("unimplemented method");
		// TODO Auto-generated method stub
		//return false;
	}

	@Override
	public V put(K val, V item) {
		final Comparable<? super K> value = comparable(val);
		LogicalOrdaringNode<K,V> node = null;
		K nodeValue = null;
		int res = -1;
		while (true) {
			node = root;
			LogicalOrdaringNode<K,V> child;
			res = -1;
			while (true) {
				if (res == 0) break;
				if (res > 0) {
					child = node.rightChild;
				} else {
					child = node.leftChild;
				}
				if (child == null) break;
				node = child;
				nodeValue = node.value;
				res = value.compareTo(nodeValue);
			}
			res = value.compareTo(node.value);
			final LogicalOrdaringNode<K,V> pred = res > 0 ? node : node.predecessor;
			pred.lockSuccessor();
			if (pred.valid) {
				final K predVal = pred.value;
				final int predRes = pred == node? res: value.compareTo(predVal);
				if (predRes > 0) {
					final LogicalOrdaringNode<K,V> succ = pred.successor;
					final K succVal = succ.value;
					final int res2 = succ == node? res: value.compareTo(succVal);
					if (res2 <= 0) {
						if (res2 == 0) {
							V item2 = (V) succ.item;
							pred.unlockSuccessor();
							return item2;
						}
						LogicalOrdaringNode<K,V> newNode = new LogicalOrdaringNode<K,V>(val, item, pred, succ);
						newNode.lockSuccessor();
						succ.setPredecessor(newNode);
						pred.setSuccessor(newNode);
						updateLayoutUponInsertion(node, pred, succ, newNode);
						newNode.unlockSuccessor();
						pred.successorLock.unlock();
						return null;
					}
				}
			}
			pred.unlockSuccessor();
		}
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
	public V remove(Object val) {
		Comparable<? super K> value = comparable(val);
		LogicalOrdaringNode<K,V> node = null;
		LogicalOrdaringNode<K,V> pred;
		while (true) {
			K nodeValue = null;
			int res = 0;
			while (true) {
				node = root;
				LogicalOrdaringNode<K,V> child;
				while (true) {
					nodeValue = node.value;
					res = value.compareTo(nodeValue);
					if (res == 0) break;
					if (res > 0) {
						child = node.rightChild;
					} else {
						child = node.leftChild;
					}
					if (child == null) break;
					node = child;
				}
				pred = res > 0 ? node : node.predecessor;
				pred.lockSuccessor();
				if (pred.valid) {
					final K predVal = pred.value;
					final int predRes = pred == node? res: value.compareTo(predVal);
					if (predRes > 0) {
						LogicalOrdaringNode<K,V> succ = pred.successor;
						final K succVal = succ.value;
						int res2 = succVal == node? res: value.compareTo(succVal);
						if (res2 <= 0) {
							if (res2 != 0) {
								pred.unlockSuccessor();
								return null;
							}
							succ.lockSuccessor();
							LogicalOrdaringNode<K,V> nodeForRemoval = getNodeForRemoval(succ);
							V item = (V) succ.item;
							succ.valid = false;
							boolean parent = false;
							succ.unlockSuccessor();
							if (nodeForRemoval != succ) {
								if (!detachNode(succ, nodeForRemoval)) { // detach and lock succ if can
									succ.lockNode();
								}
 								LogicalOrdaringNode<K, V> succParent = lockParent(succ, succ.parent);
								nodeForRemoval.parent = succParent;
								LogicalOrdaringNode<K,V> rightChild = succ.rightChild;
								LogicalOrdaringNode<K,V> leftChild = succ.leftChild;
								nodeForRemoval.leftChild = leftChild;
								nodeForRemoval.rightChild = rightChild;
								
								nodeForRemoval.predecessor = pred;
								pred.successor = nodeForRemoval;
								pred.unlockSuccessor();
								
								if (rightChild != null) {
									rightChild.parent = nodeForRemoval;
								}
								leftChild.parent = nodeForRemoval;
								if (succParent.leftChild == succ) {
									succParent.leftChild = nodeForRemoval;
								} else {
									succParent.rightChild = nodeForRemoval;
								}
								nodeForRemoval.unlockSuccessor();
								nodeForRemoval.unlockNode();
								succ.unlockNode();
								succParent.unlockNode();
							} else { 
								succ = nodeForRemoval.successor;
								succ.setPredecessor(pred);
								pred.setSuccessor(succ);
								updateLayoutUponRemoval(nodeForRemoval, pred.successorLock, parent);
								pred.unlockSuccessor();
							}
							return item;
						}
					}
				}
				pred.successorLock.unlock();
			}
		}
	}

	@Override
	public Collection<V> values() {
		throw new RuntimeException("unimplemented method");
		// TODO Auto-generated method stub
		//return false;
	}

	@Override
	public void clear() {
		LogicalOrdaringNode<K,V> parent = new LogicalOrdaringNode(Integer.MIN_VALUE);
		root.predecessor = parent;
		root.successor = parent;
		root.parent = parent;
		root.rightChild = null;
		root.leftChild = null; // ??? 
		parent.rightChild = root;
		parent.setSuccessor(root);
		
	}

	@Override
	public int size() {
		return countNodes(root.leftChild);
	}

	final public int countNodes(LogicalOrdaringNode<K,V> node) {
		if (node == null) return 0;
		int rMax = countNodes(node.rightChild);
		int lMax = countNodes(node.leftChild);
		return rMax+lMax + 1;
	}

	@Override
	public int getRange(K[] result, K min, K max) {
		throw new RuntimeException("unimplemented method");
		// TODO Auto-generated method stub
		//return false;
	}

	private boolean detachNode(LogicalOrdaringNode<K, V> succ,
			LogicalOrdaringNode<K, V> nodeForRemoval) {
		nodeForRemoval.lockNode();
		LogicalOrdaringNode<K, V> succRightChild = nodeForRemoval.rightChild;
		while (succRightChild != null && !succRightChild.tryLockNode()) {
			nodeForRemoval.unlockNode();
			Thread.yield();
			nodeForRemoval.lockNode();
			succRightChild = nodeForRemoval.rightChild;
		}
		LogicalOrdaringNode<K, V> nfrParent = nodeForRemoval.parent;
		nfrParent = lockParent(nodeForRemoval, nfrParent);
		
		if (succRightChild != null) {
			succRightChild.parent = nfrParent;
		}
		if (nfrParent.leftChild == nodeForRemoval) {
			nfrParent.leftChild = succRightChild;
		} else {
			nfrParent.rightChild = succRightChild;
		}
		if (succRightChild != null) {
			succRightChild.unlockNode();
		}
		if (nfrParent != succ) {
			nfrParent.unlockNode();
			return false;
		}
		return true;
	}
	
	final private void updateLayoutUponRemoval(LogicalOrdaringNode<K,V> removedNode, Lock lock, boolean isParent) {
		LogicalOrdaringNode<K, V> rightChild = removedNode.rightChild;
		LogicalOrdaringNode<K,V> child = rightChild == null ? getAndLockLeftChild(removedNode) : getAndLockRightChild(removedNode);
		
		LogicalOrdaringNode<K,V> parent = removedNode.parent;
		if (!isParent) {
			parent = lockParent(removedNode,  parent);
		}
		if (child != null) {
			child.parent = parent;
		}
		if (parent.leftChild == removedNode) {
			parent.leftChild = child;
		} else {
			parent.rightChild = child;
		}
		if (rightChild == null && child != null) {
			child.unlockNode();
		}
		removedNode.lock.unlock();
		parent.unlockNode();
	}
	
	final private LogicalOrdaringNode<K,V> getNodeForRemoval(LogicalOrdaringNode<K,V> treeNode) {
		if (treeNode.leftChild == null || treeNode.rightChild == null) {
			return treeNode;
		}
		LogicalOrdaringNode<K,V> successor = treeNode.successor;
		successor.successorLock.lock();
		return successor;
	}
	
	final private LogicalOrdaringNode<K,V> lockParent(LogicalOrdaringNode<K,V> removed, LogicalOrdaringNode<K,V> parent) {
		parent.lockNode();
		while (removed.parent != parent) {
			parent.unlockNode();
			parent = removed.parent;
			parent.lockNode();
		}
		return parent;
	}

	final private LogicalOrdaringNode<K,V> getAndLockRightChild(LogicalOrdaringNode<K,V> removed) {
		removed.lockNode();
		LogicalOrdaringNode<K, V> child = removed.rightChild;
		return child;
	}
	
	final private LogicalOrdaringNode<K,V> getAndLockLeftChild(LogicalOrdaringNode<K,V> removed) {
		removed.lockNode();
		LogicalOrdaringNode<K, V> child = removed.leftChild;
		while (child != null && !child.tryLockNode()) {
			removed.unlockNode();
			Thread.yield();
			removed.lockNode();
			child = removed.leftChild;
		}
		return child;
	}

	
	final private void updateLayoutUponInsertion(LogicalOrdaringNode<K,V> foundNode, LogicalOrdaringNode<K,V> pred, LogicalOrdaringNode<K,V> succ, LogicalOrdaringNode<K,V> newNode) {
		LogicalOrdaringNode<K,V> createdNode =  newNode;
		LogicalOrdaringNode<K,V> parent = succ.leftChild == null? succ : pred;
		createdNode.parent = parent;
		if (parent == pred) {
			parent.rightChild = createdNode;
		} else {
			parent.leftChild = createdNode;
		}
		// because you don't lock any treeNode, in the removal you need to lock the successorLock of the succ's succ to avoid 
		// insertion to the successor's right child
	}
}
