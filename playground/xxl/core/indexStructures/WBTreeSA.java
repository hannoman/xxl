package xxl.core.indexStructures;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;

import xxl.core.collections.MappedList;
import xxl.core.collections.containers.TypeSafeContainer;
import xxl.core.functions.FunctionsJ8;
import xxl.core.io.converters.Converter;
import xxl.core.util.HUtil;

public class WBTreeSA<K extends Comparable<K>, V, P> {
	/** Standalone version of a weight-balanced B+-Tree.
	 * 
	 * @param K type of the keys
	 * @param V type of the actual data
	 * @param P type of the ContainerIDs
	 */

	/** Number of entries in a leaf L e(L): leafParam <= e(L) < 2*leafParam */ 
	int leafParam;

	/**
	 * Number of entries in a node N e(N): branchingParam / 4 <? e(N) <?
	 * 4*branchingParam
	 */
	int branchingParam;

	Node root;

	public java.util.function.Function<V, K> getKey; // TODO: set it somewhere

	public TypeSafeContainer<P, Node> container; // TODO: set it somewhere

	/** Remember how high the tree is... */
	int rootHeight;

	/** Meta-information about the root. Root has no parent so it must be saved elsewhere. */
	ChildMetaInfo rootMetaInfo;

	/** ContainerID of the root. */
	P rootContainerID;

	//-- Converters
	public Converter<K> keyConverter;
	public Converter<V> valueConverter;
	public Converter<Node> nodeConverter;

	//-- Constructors
	public WBTreeSA(int leafParam, int branchingParam) {
		// super();
		this.leafParam = leafParam;
		this.branchingParam = branchingParam;
		this.root = null;
	}

	/**
	 * Class containing meta information about the child-nodes, which is kept in
	 * the directory of the parent node for performance reasons.
	 * 
	 * - comes down to just "weight" atm.
	 * 
	 * @author Dominik Krappel
	 */
	public class ChildMetaInfo {
		int weight;
	}

	//-- Node class
	public abstract class Node {
		public abstract boolean isLeaf();

		public boolean isInner() {
			return !isLeaf();
		};

		public InnerNode asInner() {
			return isInner() ? (InnerNode) this : null;
		}

		public LeafNode asLeaf() {
			return isLeaf() ? (LeafNode) this : null;
		}		
		
	}

	public class InnerNode extends Node {
		List<K> separators;
		List<P> pagePointers;
		/** weights are saved inside the parent to make it possible to determine a node's split without having to load all child-nodes from disk
		 * to access the weight information.
		 */
		List<ChildMetaInfo> metaInfo;

		
		@Override
		public boolean isLeaf() {
			return false;
		}
		
		public int totalWeight() {
			int summed = 0;
			for(ChildMetaInfo childMet : metaInfo) {
				summed += childMet.weight;
			}
			return summed;
		}
		
		
		/** Determines a feasible split position through linear search.
		 * 
		 * TODO: as of now only considers the quality of the left offspring node.
		 * 
		 * @param targetWeight the weight per node which should be approached
		 * @return the position of the node after which the child-list should be split
		 */
		public int determineSplitposition(int targetWeight) {
			int curSum = 0;
			int curSplitWeightMiss = 0;
			
			int bestSplitAfterPos = -1;
			int bestSplitWeightMiss = Math.abs(targetWeight - curSum);
			
			for(int curSplitAfterPos = 0; curSplitAfterPos < pagePointers.size(); curSplitAfterPos++) {
				curSum += metaInfo.get(curSplitAfterPos).weight;
				curSplitWeightMiss = Math.abs(targetWeight - curSum);
				
				if(curSplitWeightMiss < bestSplitWeightMiss) {
					bestSplitWeightMiss = curSplitWeightMiss;
					bestSplitAfterPos = curSplitAfterPos;
				}				
			}
			
			return bestSplitAfterPos;
		}
		
		public SplitInfo split(int targetWeight) {
			int splitPos = determineSplitposition(targetWeight);
			
			InnerNode newode = new InnerNode();
			
			//- split separators
			// separators[splitPos] becomes the separator between the offspring 
			newode.separators = HUtil.splitOffRight(separators, splitPos+1, new ArrayList<K>());
			K offspringSeparator = separators.remove(splitPos);
			
			//- split other lists
			newode.pagePointers = HUtil.splitOffRight(pagePointers, splitPos, new ArrayList<P>());			
			newode.metaInfo = HUtil.splitOffRight(metaInfo, splitPos, new ArrayList<ChildMetaInfo>());
			
			//- put new node into Container
			P newodeCID = container.insert(newode);			
			
			return new SplitInfo(newodeCID, offspringSeparator);			
		}
		
	}

	public class LeafNode extends Node {
		List<V> values;

		@Override
		public boolean isLeaf() {
			return true;
		}
		
		/**
		 * Splits the leaf in the middle.
		 */
		public SplitInfo split() {
			LeafNode newode = new LeafNode();
			newode.values = HUtil.splitOffRight(values, values.size() / 2, new ArrayList<V>());
			K separator = getKey.apply(newode.values.get(0));
			
			//- put new node into Container
			P newodeCID = container.insert(newode);			
			
			return new SplitInfo(newodeCID, separator);
		}
	}

	public boolean weightUnderflow(int weight, int level) {
		return weight < HUtil.intPow(branchingParam, level) / 2;
	}
	
	public boolean weightOverflow(int weight, int level) {
		return weight > 2 * HUtil.intPow(branchingParam, level);
	}		
	
	public boolean leafUnderflow(int weight) {
		return weight < leafParam;
	}
	
	public boolean leafOverflow(int weight) {
		return weight >= 2*leafParam;
	}
	
	
	
	
	/**
	 * Own SplitInfo class (perhaps its similar to the one used in XXL?) to encapsulate
	 * - the ContainerID of the generated Node
	 * - a key used for the Separation of the nodes.
	 *  
	 * @author Dominik Krappel	 *
	 */
	class SplitInfo {
		P newnodeCID;
		K separator;
		
		public SplitInfo(P newnodeCID, K separator) {		
			this.newnodeCID = newnodeCID;
			this.separator = separator;
		}
	}
	
	
	/** Trying a recursive approach "with preemptive splitting"??.
	 * - Not tail recursive.
	 * 
	 * @return the containerID of the new generated Node if a split occured.
	 */
	public SplitInfo insert(V value, P belowCID, int level) {
		Node node = container.get(belowCID);
		if(node.isLeaf()) {
			insertLeaf(value, belowCID);
		} 
		
		InnerNode inode = (InnerNode) node;
		K key = getKey.apply(value);
		
		// check if this will need to split and if so do it now
		if(weightOverflow(inode.totalWeight() + 1, level)) {
			int targetWeight = HUtil.intPow(branchingParam, level);
			SplitInfo splitInfo = inode.split(targetWeight);
			
			// determine offspring node to continue with, which is guaranteed to not split again
			if(key.compareTo(splitInfo.separator) <= 0) {
				// stay in old node; this mainly just reenters this function 
				insert(value, belowCID, level);
			} else {
				insert(value, splitInfo.newnodeCID, level);
			}
			
			return splitInfo; // propagate splitInfo to parent
		} else { // no split here			
			//- determine next node
			int pos = HUtil.findPos(inode.separators, key);
			
			P nextCID = inode.pagePointers.get(pos);
			ChildMetaInfo nextMeta = inode.metaInfo.get(pos);
			nextMeta.weight++;
			
			SplitInfo splitInfo = insert(value, nextCID, level-1); // recursion
			if(splitInfo != null) { // a split occured in child and we need to update the directory
				inode.separators.add(pos, splitInfo.separator);
				inode.pagePointers.add(pos+1, splitInfo.newnodeCID);				
			}						
			container.update(belowCID, inode); // update the container contents
			
			return null; // no splitInfo to propagate
		}
	}
	
	public SplitInfo insertLeaf(V value, P belowCID) {
		LeafNode lnode = (LeafNode) container.get(belowCID);
		
		SplitInfo splitInfo = null;
		// insertLeaf(value, P belowCID, (LeafNode) node, level-1)
		K key = getKey.apply(value);
		int insertPos = HUtil.findPos(new MappedList<V,K>(lnode.values, FunctionsJ8.toOldFunction(getKey)), key);
		lnode.values.add(insertPos, value);
		
		if(leafOverflow(lnode.values.size())) {
			splitInfo = lnode.split();			
		}
		
		//- update container contents
		container.update(belowCID, lnode);
		
		return splitInfo;
	}

	
	/** Naive (= without considerations regarding greater system design) implementation
	 * of <tt>insert</tt>. Doing as much things directly as possible.
	 *  
	 * @param value data value to insert
	 */
	public void insert(V value) {
		K key = getKey.apply(value);
				
		// keep track of the nodes which have to split because of a weight violation
		Stack<P> toSplit_ofWeight = new Stack<P>();
		Stack<P> path = new Stack<P>();
		// keeping track of parents during the descent as we don't keep that information in the nodes anymore
		HashMap<P,P> parents = new HashMap<P,P>(); 
		
		
		P containerID = rootContainerID;
		Node node = root;
		ChildMetaInfo meta = rootMetaInfo;
		int level = rootHeight;
		
		// bookkeep information about parents
		parents.put(containerID, !path.isEmpty() ? path.peek() : null);
		path.push(containerID);
		
		// increase weight of root
		meta.weight++;
		if(weightOverflow(meta.weight, level-1)) {
			toSplit_ofWeight.push(containerID);
		}
		
		while(node.isInner()) {
			//- we are in an inner node, so lets find the next node
			InnerNode inode = (InnerNode) node;
			int pos = Math.abs(Collections.binarySearch(inode.separators, key));
			
			containerID = inode.pagePointers.get(pos);
			meta = inode.metaInfo.get(pos);
			
			//-- descend
			node = container.get(containerID);
			level--;
			
			//-- doing stuff on downward traversal which can already be done
			meta.weight++;
			if(weightOverflow(meta.weight, level-1)) {
				toSplit_ofWeight.push(containerID);
			}
			container.update(containerID, node); // tell container that this has changed			
		}
		
		assert(node.isLeaf());
		
		//-- actual insert value in leaf 
		LeafNode lnode = (LeafNode) node;
		P leafCID = containerID;
		
		// old version using helpers from XXL
		int insertPos = Collections.binarySearch(new MappedList<V,K>(lnode.values, FunctionsJ8.toOldFunction(getKey)), key);
		
		// new version utilizing Java8 functional capabilities
		// nvm, doesn't work that way and would probably be less efficient anyway
		// int insertPos = Collections.binarySearch(lnode.values.stream().map(getKey), key); 
				
		lnode.values.add(Math.abs(insertPos), value);		
		container.update(containerID, node); // tell container that this has changed		
		
		// check if leaf is overflowing
		if(leafOverflow(meta.weight)) {
			LeafNode newode = new LeafNode();
			List<V> transferPart = lnode.values.subList(lnode.values.size() / 2, lnode.values.size());
			newode.values = new ArrayList<V>(transferPart);
			transferPart.clear();
			
			InnerNode pnode = (InnerNode) container.get(parents.get(leafCID));
			// TODO
		}
		
	}
		

}
