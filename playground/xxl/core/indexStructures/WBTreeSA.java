package xxl.core.indexStructures;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;
import java.util.function.*;

import xxl.core.functions.*;
import xxl.core.collections.MappedList;
import xxl.core.collections.containers.TypeSafeContainer;
import xxl.core.io.converters.Converter;
import xxl.core.math.HMaths;

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
		List<ChildMetaInfo> metaInfo;

		@Override
		public boolean isLeaf() {
			return false;
		}		
		
	}

	public class LeafNode extends Node {
		List<V> values;

		@Override
		public boolean isLeaf() {
			return true;
		}
	}

	public boolean weightOverflow(int weight, int level) {
		return weight > 2 * HMaths.intPow(branchingParam, level);
	}
	
	public boolean weightUnderflow(int weight, int level) {
		return weight < HMaths.intPow(branchingParam, level) / 2;
	}
	
	public void insert(V value) {
		K key = getKey.apply(value);
				
		// keep track of the nodes which have to split because of a weight violation
		Stack<P> toSplit_ofWeight = new Stack<P>();
		Stack<P> path = new Stack<P>();
		HashMap<P,P> parents = new HashMap<P,P>();
		
		Node node = root;
		int level = rootHeight;
		ChildMetaInfo meta = rootMetaInfo;
		P containerID = rootContainerID;
		
		// bookkeep information about parents
		parents.put(containerID, !path.isEmpty() ? path.peek() : null);
		path.push(containerID);
		
		while(true) {
			
			//-- doing stuff on downward traversal which can already be done
			meta.weight++;
			if(weightOverflow(meta.weight, level-1)) {
				toSplit_ofWeight.push(containerID);
			}
			container.update(containerID, node); // tell container that this has changed
			
			if(node.isLeaf()) {
				break;
			} else {
				//-- we are in an inner node, so lets find the next node
				InnerNode inode = (InnerNode) node;
				int pos = Math.abs(Collections.binarySearch(inode.separators, key));
				
				containerID = inode.pagePointers.get(pos);
				meta = inode.metaInfo.get(pos);
				// descend
				node = container.get(containerID);
				level--;
			}
			
		}
		
		assert(node.isLeaf());
		
		// actual insert value in leaf 
		LeafNode lnode = (LeafNode) node;
		int insertPos = Collections.binarySearch(new MappedList<V,K>(lnode.values, FunctionsJ8.toOldFunction(getKey)), key);
		lnode.values.add(Math.abs(insertPos), value);		
		container.update(containerID, node); // tell container that this has changed		
		
		// check if leaf is overflowing
		if()
		
	}

}
