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
import xxl.core.util.Triple;

public class WBTreeSA<K extends Comparable<K>, V, P> {
	/** Standalone version of a weight-balanced B+-Tree.
	 * Based on "Optimal Dynamic Interval Management in External Memory" by L. Arge, J.S. Vitter
	 *
	 * TODO: this version tries to split the nodes already on downward traversal which should be slightly more performant
	 * 		than splitting the nodes after the search has progressed to a leaf node in an additional upward run. (Afaik this technique is
	 * 		usual on some B-Tree variants too).
	 * 		Although this should not cause much danger the theorems from the paper should be reevaluated in this context.
	 * 		And it's not even implemented properly as of now, as recursion still takes place before updating the directory. :/
	 * 
	 * TODO: B+-tree style level wise "Verpointerung" not yet implemented
	 *   
	 * @param K type of the keys
	 * @param V type of the actual data
	 * @param P type of the ContainerIDs (= CIDs)
	 */

	/** Number of entries in a leaf L e(L): leafParam <= e(L) < 2*leafParam */ 
	int leafParam;

	/** Number of entries in a node N e(N): branchingParam / 4 <? e(N) <? 4*branchingParam */
	int branchingParam;

	Node root;

	public java.util.function.Function<V, K> getKey; // TODO: set it somewhere

	public TypeSafeContainer<P, Node> container; // TODO: set it somewhere

	/** Remember how high the tree is... */
	int rootHeight;

	/** Meta-information about the root. Root has no parent so it must be saved elsewhere. */
	ChildMetaInfo rootMetaInfo;

	/** ContainerID of the root. */
	P rootCID;

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
	 * Class containing meta information about the child-nodes, which is kept in
	 * the directory of the parent node for performance reasons.
	 * 
	 * - comes down to just "weight" atm.
	 * 
	 * @author Dominik Krappel
	 */
	public class ChildMetaInfo {
		int weight;
		
		public ChildMetaInfo(int weight) { this.weight = weight; }
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
		 * TODO: as of now only considers the quality of the left offspring node. Is this a problem?
		 * 
		 * @param targetWeight the weight per node which should be approached
		 * @return the position of the node after which the child-list should be split
		 */
		public Triple<Integer, Integer, Integer> determineSplitposition(int targetWeight) {
			int curSum = 0;
			int curSplitWeightMiss = 0;
			
			int bestSplitAfterPos = -1;
			int bestRemLeftWeight = curSum;
			int bestSplitWeightMiss = Math.abs(targetWeight - curSum);
			
			
			for(int curSplitAfterPos = 0; curSplitAfterPos < pagePointers.size(); curSplitAfterPos++) {
				curSum += metaInfo.get(curSplitAfterPos).weight;
				curSplitWeightMiss = Math.abs(targetWeight - curSum);
				
				if(curSplitWeightMiss < bestSplitWeightMiss) {
					bestSplitWeightMiss = curSplitWeightMiss;
					bestSplitAfterPos = curSplitAfterPos;
					bestRemLeftWeight = curSum;
				}				
			}
			
			int totalWeight = curSum;
			
			return new Triple<Integer, Integer, Integer>(bestSplitAfterPos, bestRemLeftWeight, totalWeight - bestRemLeftWeight);
		}
		
		public SplitInfo split(int targetWeight) {
			Triple<Integer, Integer, Integer> splitPosInfo = determineSplitposition(targetWeight);
			int splitPos = splitPosInfo.getElement1();
			int remLeft = splitPosInfo.getElement2();
			int remRight = splitPosInfo.getElement3();
			
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
			
			return new SplitInfo(newodeCID, offspringSeparator, remLeft, remRight);			
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
			int remLeft = values.size() / 2;
			int remRight = values.size() - remLeft;
			
			newode.values = HUtil.splitOffRight(values, values.size() / 2, new ArrayList<V>());
			K separator = getKey.apply(newode.values.get(0));
			
			//- put new node into Container
			P newodeCID = container.insert(newode);			
			
			return new SplitInfo(newodeCID, separator, remLeft, remRight);
		}
	}

	/**
	 * Own SplitInfo class (perhaps its similar to the one used in XXL?) to encapsulate
	 * - the ContainerID of the generated Node
	 * - a key used for the separation of the nodes.
	 * - the weights of the resulting left and right node
	 * 
	 * @author Dominik Krappel
	 */
	class SplitInfo {
		P newnodeCID;
		K separator;
		int weightLeft;
		int weightRight;		
		
		public SplitInfo(P newnodeCID, K separator, int weightLeft, int weightRight) {		
			this.newnodeCID = newnodeCID;
			this.separator = separator;
			this.weightLeft = weightLeft;
			this.weightRight = weightRight;
		}
	}
	
	
	/** Trying a recursive approach "with preemptive splitting"??.
	 * - Not tail recursive.
	 * 
	 * @return the containerID of the new generated Node if a split occured.
	 */
	public SplitInfo insert(V value, P belowCID, int level) {
		Node node = container.get(belowCID);
		if(node.isLeaf()) return insertLeaf(value, belowCID);
		
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
		} else { // no split in this node (or already done)			
			//- determine next node
			int pos = HUtil.findPos(inode.separators, key);
			
			P nextCID = inode.pagePointers.get(pos);
			ChildMetaInfo nextMeta = inode.metaInfo.get(pos);
			nextMeta.weight++;
			
			SplitInfo splitInfo = insert(value, nextCID, level-1); // recursion
			if(splitInfo != null) { // a split occured in child and we need to update the directory
				inode.separators.add(pos, splitInfo.separator);
				inode.pagePointers.add(pos+1, splitInfo.newnodeCID);
				inode.metaInfo.get(pos).weight = splitInfo.weightLeft;
				inode.metaInfo.add(pos+1, new ChildMetaInfo(splitInfo.weightRight));
			}						
			container.update(belowCID, inode); // update the container contents
			
			return null; // no splitInfo to propagate
		}
	}
	
	/** Trying a iterative version again. */
	public void insertIter(V value) {
		
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

	
	public void insertRoot(V value) {
		K key = getKey.apply(value);
		InnerNode rnode = (InnerNode) container.get(rootCID);
		int level = rootHeight;
		
		// special treatment of root		
		if(weightOverflow(rootMetaInfo.weight + 1, rootHeight)) {
			// split root
			int targetWeight = HUtil.intPow(branchingParam, level);
			SplitInfo splitInfo = rnode.split(targetWeight);
			
			// create new root
			InnerNode newroot = new InnerNode();
			newroot.separators = new ArrayList<K>();
			newroot.separators.add(splitInfo.separator);
			
			newroot.pagePointers = new ArrayList<P>();
			newroot.pagePointers.add(rootCID);
			newroot.pagePointers.add(splitInfo.newnodeCID);
			
			newroot.metaInfo = new ArrayList<ChildMetaInfo>();
			newroot.metaInfo.add(new ChildMetaInfo(splitInfo.weightLeft));
			newroot.metaInfo.add(new ChildMetaInfo(splitInfo.weightRight));
			
			// overwrite old root
			P oldRootCID = rootCID;
			rootCID = container.insert(newroot);
			rootHeight++;
			rootMetaInfo.weight++;
						
			// determine offspring node to continue with, which is guaranteed to not split again
			if(key.compareTo(splitInfo.separator) <= 0)
				insert(value, oldRootCID, level);
			else insert(value, splitInfo.newnodeCID, level);
		} else {
			insert(value, rootCID, rootHeight);
		}
		
		
	}
		

}
