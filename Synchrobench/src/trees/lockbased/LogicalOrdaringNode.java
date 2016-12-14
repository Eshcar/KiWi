package trees.lockbased;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class LogicalOrdaringNode<K, V> {
	
	public final K value;
	public volatile Object item;
	public volatile boolean valid;
	public volatile LogicalOrdaringNode<K, V> predecessor;
	public volatile LogicalOrdaringNode<K, V> successor;
	final public Lock successorLock;
	
	final public Lock lock;
	public volatile LogicalOrdaringNode<K, V> parent;
	public volatile LogicalOrdaringNode<K, V> rightChild;
	public volatile LogicalOrdaringNode<K, V> leftChild;
	
	public LogicalOrdaringNode(K value) {
		this(value, null, null, null);
	}
	
	public LogicalOrdaringNode(K value, Object item, LogicalOrdaringNode<K, V> pred, LogicalOrdaringNode<K, V> succ) {
		this.value = value;
		this.item = item;
		this.valid = true;
		this.predecessor = pred;
		this.successorLock = new ReentrantLock();
		this.successor = succ;
		this.lock = new ReentrantLock();
	}

	public void lockNode() {
		lock.lock();
	}
	
	public boolean tryLockNode() {
		return lock.tryLock();
	}
	
	public void unlockNode() {
		lock.unlock();
	}
	
	public void lockSuccessor() {
		successorLock.lock();
	}
	
	public void unlockSuccessor() {
		successorLock.unlock();
	}
	
	public void invalidate() {
		this.valid = false;
	}
	
	public void setPredecessor(LogicalOrdaringNode<K,V> pred) {
		this.predecessor = pred;
	}
	
	public void setSuccessor(LogicalOrdaringNode<K,V> succ) {
		this.successor = succ;
	}
	
	@Override
	public String toString() {
		String delimiter = "  ";
		StringBuilder sb = new StringBuilder();

		return sb.append("(" + value + delimiter + ", " + valid + ")" + delimiter).toString();
	}

}