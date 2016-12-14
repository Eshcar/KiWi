package trees.lockbased;

import com.sun.tools.corba.se.idl.constExpr.Not;
import contention.abstractions.CompositionalMap;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import trees.lockbased.snaptree.SnapTreeMap;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Created by dbasin on 1/26/16.
 */
public class LockBasedSnapTree <E extends Comparable<? super E>,V> implements CompositionalMap<E,V> {
    private SnapTreeMap<E,V> snapTree;

    public LockBasedSnapTree()
    {
        snapTree = new SnapTreeMap<>();
    }

    @Override
    public V putIfAbsent(E e, V v) {
        throw new NotImplementedException();
    }

    @Override
    public void clear() {
        snapTree = new SnapTreeMap<>();
    }

    @Override
    public Set<E> keySet() {
        throw new NotImplementedException();
    }

    @Override
    public Collection<V> values() {
        throw new NotImplementedException();
    }

    @Override
    public Set<Entry<E, V>> entrySet() {
        throw new NotImplementedException();
    }

    @Override
    public int size() {
        return snapTree.size();
    }

    @Override
    public boolean isEmpty() {
        return snapTree.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return snapTree.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return snapTree.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return snapTree.get(key);
    }

    @Override
    public V put(E key, V value) {
        return snapTree.put(key, value);
    }

    @Override
    public V remove(Object key) {
        return snapTree.remove(key);
    }

    @Override
    public void putAll(Map<? extends E, ? extends V> m) {
        throw new NotImplementedException();
    }

    @Override
    public int getRange(E[] result, E min, E max) {
        ConcurrentNavigableMap<E, V> map = snapTree.clone().subMap(min, max);
        int count=0;
        // temp fix to cause skipList to return values
/*		for(K key : map.keySet()){
			result[count]=key;
			count++;
		}
*/
        try {
            for (V val : map.values()) {
                result[count] = (E) val;
                count++;
            }
        }catch(Exception e)
        {
            e.printStackTrace();
        }
        return count;
    }
}
