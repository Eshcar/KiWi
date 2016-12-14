package trees.lockbased.lockremovalutils;

import java.util.HashMap;
import java.util.Map.Entry;



public final class ReadSet<K,V>{
	
	private final static int READ_SET_SIZE = 256; 
	private final int actualSize;
	private SpinHeapReentrant readSetObjects[];
	private int readSetVersions[]; 
	private int count; 
	private HashMap<SpinHeapReentrant,Integer> readSetHash = new HashMap<SpinHeapReentrant,Integer>(); 

	public ReadSet(){
		readSetObjects = new SpinHeapReentrant[READ_SET_SIZE];
		readSetVersions = new int[READ_SET_SIZE];
		actualSize = READ_SET_SIZE;
		count = 0; 
	}
	
	public ReadSet(int size){
		readSetObjects = new SpinHeapReentrant[size];
		readSetVersions = new int[size];
		actualSize = size;
		count = 0; 
	}
	
	public void clear(){
		count = 0;
		if (!readSetHash.isEmpty()){
			readSetHash.clear();
		}
	}
	
	public void add( SpinHeapReentrant node, int version){
		if (count < actualSize){
			readSetObjects[count] = node;
			readSetVersions[count] = version;
			count++;
		}else{
			if(!readSetHash.containsKey(node)){
				readSetHash.put(node, version);
			}
			//readSetHash.putIfAbsent(node, version);
		}
	}
	

	public boolean validate(final Thread self){
		SpinHeapReentrant node; 
		for (int i=0; i<count; i++){
			node =  readSetObjects[i];
			if (node.lockedBy()!= self && node.isLocked()){
				return false; 
			}
			if(readSetVersions[i]!= node.getVersion()) return false;
		
		}
		if(!readSetHash.isEmpty()){
			for(Entry<SpinHeapReentrant, Integer> entry : readSetHash.entrySet()) {
				node = entry.getKey();
			    int version = entry.getValue();
			    if (node.lockedBy()!= self && node.isLocked()){
					return false; 
				}
				if(version!= node.getVersion()) return false;
			}
		}
		return true;
	}
	
	public void incrementLocalVersion( SpinHeapReentrant node ){
		SpinHeapReentrant currNode; 
		for (int i=0; i<count; i++){
			currNode = readSetObjects[i];
			if( currNode == node){
				readSetVersions[i]++;
			}
		}
		if(!readSetHash.isEmpty()){
			for(Entry<SpinHeapReentrant, Integer> entry : readSetHash.entrySet()) {
				node = entry.getKey();
			    int version = entry.getValue();
			    version++;
			    readSetHash.put(node, version);
			}
		}
	}

	public int getCount() {
		if(readSetHash.isEmpty()){
			return count; 
		}else{
			return count + readSetHash.size();
		}
	}
	
	public boolean contains(SpinHeapReentrant node){
		for (int i=0; i<count; i++){
			SpinHeapReentrant currNode = readSetObjects[i];
			if( currNode.equals(node)){
				return true; 
			}
		}
		if(readSetHash.containsKey(node)){
			return true; 
		}
		return false; 
	}
}
