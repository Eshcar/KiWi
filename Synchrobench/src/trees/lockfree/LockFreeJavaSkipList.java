package trees.lockfree;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import contention.abstractions.CompositionalMap;

public class LockFreeJavaSkipList<K,V> implements CompositionalMap<K,V>{
	 private final ConcurrentSkipListMap<K,V> skiplist; 
	 
	 public LockFreeJavaSkipList(){
		 skiplist = new ConcurrentSkipListMap<K,V>(); 
	 }
	 
	
	@Override
	public boolean containsKey(Object key) {
		return skiplist.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return skiplist.containsValue(value);
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return skiplist.entrySet();
	}

	@Override
	public V get(Object key) {
		return skiplist.get(key);
	}

	@Override
	public boolean isEmpty() {
		return skiplist.isEmpty();
	}

	@Override
	public Set<K> keySet() {
		return skiplist.keySet();
	}

	@Override
	public V put(K key, V value) {
		return skiplist.put(key, value);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		skiplist.putAll(m);
	}

	@Override
	public V remove(Object key) {
		return skiplist.remove(key);
	}

	@Override
	public Collection<V> values() {
		return skiplist.values();
	}

	@Override
	public V putIfAbsent(K key, V value) {
		return skiplist.putIfAbsent(key, value);
	}

	@Override
	public void clear() {
		skiplist.clear();
	}

	@Override
	public int size() {
		return skiplist.size();
	}



	@Override
	public int getRange(K[] result, K min, K max) {
		ConcurrentNavigableMap<K, V> map = skiplist.subMap(min, max);
		int count=0;
	// temp fix to cause skipList to return values
/*		for(K key : map.keySet()){
			result[count]=key;
			count++;
		}
*/
		for(V val : map.values())
		{
			result[count] = (K)val;
			count++;
		}
		return count;
	}

}
