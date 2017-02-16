package org.apache.storm.grouping;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;



public class RelaxedConsistentHashing{
	private HashFunction hashFunction = Hashing.murmur3_128(13);
	private final int numWorkers; 
	private final int numReplicas;
	private final SortedMap<Integer, Integer> circle =
			new TreeMap<Integer, Integer>();
	private final HashMap<Integer, HashSet<Integer>> replicaCount = 
			new HashMap<Integer, HashSet<Integer>>();

	public RelaxedConsistentHashing(int numberOfWorkers, int numberOfReplicas) {
		this.numWorkers = numberOfWorkers;
		this.numReplicas = numberOfReplicas;

		Collection<Integer> nodes = new ArrayList<Integer>();
		for(int i = 0 ; i< numWorkers;i++) 
			nodes.add(i);
		
		for (int node : nodes) {
			add(node);
		}
	}

	public void add(int node) {
		for (int i = 0; i < numReplicas; i++) {
			String str = node +":"+ i;
			int key = (int) (Math.abs(hashFunction.hashBytes(str.getBytes()).asLong()));
			circle.put(key,
					node);
			if(replicaCount.containsKey(node))
				replicaCount.get(node).add(key);
			else {
				HashSet<Integer> temp = new HashSet<Integer>();
				temp.add(key);
				replicaCount.put(node, temp);
			}
				
		}
		
	}
	
	public void remove(int node) {
		
		for (int i = 0; i < numReplicas; i++) {
			String str = node +":"+ i;
			int key = (int) (Math.abs(hashFunction.hashBytes(str.getBytes()).asLong()));
			circle.remove(key);
		}
	}

	public int getServer(byte[] raw) {
		if (circle.isEmpty()) {
			return -1;
		}
		int hash = (int) (Math.abs(hashFunction.hashBytes(raw).asLong()));
		if (!circle.containsKey(hash)) {
			SortedMap<Integer, Integer> tailMap =
					circle.tailMap(hash);
			hash = tailMap.isEmpty() ?
					circle.firstKey() : tailMap.firstKey();
		}
		return circle.get(hash);
	}


}