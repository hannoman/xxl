package xxl.core.indexStructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import xxl.core.collections.MappedList;
import xxl.core.collections.containers.TypeSafeContainer;
import xxl.core.functions.FunctionsJ8;
import xxl.core.indexStructures.BPlusTree.IndexEntry;
import xxl.core.indexStructures.BPlusTree.Node;
import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.util.HUtil;
import xxl.core.util.Triple;

public class WBTreeSA_v3<K extends Comparable<K>, V, P> {
	/** Standalone version of a weight-balanced B+-Tree.
	 * Based on "Optimal Dynamic Interval Management in External Memory" by L. Arge, J.S. Vitter
	 *
	 * Trying to implement a simpler version which doesn't do preemptive splitting. It instead splits 
	 * the nodes during bottom-up back traversal. Therefore it also keeps the whole path in memory
	 * and is thereby prone to the same error as the xxl BPlusTree implementation regarding buffers:
	 * that the buffer size must always be greater or equal than the tree height.
	 * 
	 * - Mind that this is no real BPlusTree as there are no pointers between siblings and cousins. 
	 *   
	 * @param K type of the keys
	 * @param V type of the actual data
	 * @param P type of the ContainerIDs (= CIDs)
	 */

	/** Number of entries in a leaf L e(L): leafParam <= e(L) < 2*leafParam */ 
	int leafParam;

	/** Number of entries in a node N e(N): branchingParam / 4 <? e(N) <? 4*branchingParam */
	int branchingParam;

	public java.util.function.Function<V, K> getKey; // TODO: set it somewhere

	public TypeSafeContainer<P, Node> container; // TODO: set it somewhere

	/** Remember how high the tree is... */
	int rootHeight;

	/** Meta-information about the root. Root has no parent so it must be saved elsewhere. */
	int rootWeight;

	/** ContainerID of the root. */
	P rootCID;

	//-- Converters
	public Converter<K> keyConverter;
	public Converter<V> valueConverter;
	
	/** --- Constructors & Initialisation ---
	- All mandatory arguments are put into the constructor.
	- The container gets initialized during a later call to <tt>initialize</tt> as we 
		implement the <tt>NodeConverter</tt> functionality once again (like in xxl) as inner class of this tree class.
	- Information about the root --> initialize
	*/
	public WBTreeSA_v3(
			int leafParam, 
			int branchingParam, 
			Function<V, K> getKey, 
			Converter<K> keyConverter, 
			Converter<V> valueConverter) {
		super();
		this.leafParam = leafParam;
		this.branchingParam = branchingParam;
		this.getKey = getKey;
		this.keyConverter = keyConverter;
		this.valueConverter = valueConverter;
	}	

	/** Initialization from an existing tree.
	 * 
	 * @param container container where the exisiting tree resides
	 * @param rootCID containerID of the exisiting root
	 * @param rootHeight height of the existing tree
	 * @param rootMetaInfo weight of the existing tree
	 */
//	public void initializeOld(
//			TypeSafeContainer<P, Node> container, 
//			P rootCID, 
//			int rootHeight, 
//			int rootWeight) {
//		this.container = container;
//		this.rootCID = rootCID;
//		this.rootHeight = rootHeight;
//		this.rootWeight = rootWeight;
//	}
	
	/** Initializes a new tree.
	 * 
	 * @param container the container to store the new tree in.
	 */
	public void initializeNew(
			TypeSafeContainer<P, Node> container) {
		this.container = container;		
	}

	public boolean weightUnderflow(int weight, int level) {
		return weight <= HUtil.intPow(branchingParam, level) / 2 * leafParam;
	}

	public boolean weightOverflow(int weight, int level) {
		return weight >= 2 * HUtil.intPow(branchingParam, level) * leafParam;
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

	//-- Node class
	public abstract class Node {
		
		public abstract boolean isLeaf();
		
		public boolean isInner() {
			return !isLeaf();
		}
		
		public abstract SplitInfo insert(V value, P thisCID, int level);		 
	}
	
	
	public class InnerNode extends Node {
		List<K> separators;
		List<P> pagePointers;
		/** weights are saved inside the parent to make it possible to determine a node's split without having to load all child-nodes from disk
		 * to access the weight information.
		 */
		List<Integer> childWeights;

		public boolean isLeaf() { return false; }
		
		public int totalWeight() {
			int summed = 0;
			for(int w : childWeights) {
				summed += w;
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
		protected Triple<Integer, Integer, Integer> determineSplitposition(int targetWeight) {
			int curSum = 0;
			int curSplitWeightMiss = 0;
			
			int bestSplitAfterPos = -1;
			int bestRemLeftWeight = curSum;
			int bestSplitWeightMiss = Math.abs(targetWeight - curSum);			
			
			for(int curSplitAfterPos = 0; curSplitAfterPos < pagePointers.size(); curSplitAfterPos++) {
				curSum += childWeights.get(curSplitAfterPos);
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
			newode.pagePointers = HUtil.splitOffRight(pagePointers, splitPos+1, new ArrayList<P>());			
			newode.childWeights = HUtil.splitOffRight(childWeights, splitPos+1, new ArrayList<Integer>());
			
			//- put new node into Container
			P newodeCID = container.insert(newode);
			
			return new SplitInfo(newodeCID, offspringSeparator, remLeft, remRight);			
		}
		
		public SplitInfo insert(V value, P thisCID, int level) {
			K key = getKey.apply(value);
			
			//- insert in sublevel
			int pos = HUtil.findPos(separators, key);
			
			P nextCID = pagePointers.get(pos);			
			childWeights.set(pos, childWeights.get(pos)+1); // update weight of child
			
			Node nextNode = container.get(nextCID);
			
			SplitInfo childSplitInfo = nextNode.insert(value, nextCID, level-1); // recursion
			
			if(childSplitInfo != null) { // a split occured in child and we need to update the directory
				separators.add  (pos  , childSplitInfo.separator);
				pagePointers.add(pos+1, childSplitInfo.newnodeCID);
				childWeights.set(pos  , childSplitInfo.weightLeft);
				childWeights.add(pos+1, childSplitInfo.weightRight);
			}
			
			// container.unfix(nextCID); // release lock on the childs memory			
			
			//- check for split here
			SplitInfo splitInfo = null;
			if(weightOverflow(totalWeight(), level)) {
				int targetWeight = HUtil.intPow(branchingParam, level) * leafParam;
				splitInfo = split(targetWeight);
			}
			container.update(thisCID, this);
			
			return splitInfo;
		}


		/**
		 * Determine the following node's CID on key's search path.
		 * @param key the key to look for
		 * @return P containerID of the next node
		 */
		public P chooseSubtree(K key) {
			int pos = HUtil.findPos(separators, key);
			return pagePointers.get(pos);
		}
		
		
	}

	public class LeafNode extends Node {
		List<V> values;

		public boolean isLeaf() { return true; }
		
		/**
		 * Splits the leaf in the middle.
		 */
		public SplitInfo split() {
			LeafNode newode = new LeafNode();
			int remLeft = values.size() / 2;
			int remRight = values.size() - remLeft;
			
			newode.values = HUtil.splitOffRight(values, values.size() / 2, new ArrayList<V>());
			// K separator = getKey.apply(newode.values.get(0)); // is this responsible for not finding all inserted values?
			K separator = getKey.apply(values.get(values.size()-1));
			
			//- put new node into Container
			P newodeCID = container.insert(newode);
			
			return new SplitInfo(newodeCID, separator, remLeft, remRight);
		}
		
		/**
		 * Returns the indices of the found values. 
		 * @param key key to look for
		 * @return list of indices i with "getKey(values[i]) == key" 
		 */
		public List<Integer> lookup(K key) {
			List<Integer> idx = new LinkedList<Integer>();
			
			List<K> mappedList = new MappedList<V,K>(values, FunctionsJ8.toOldFunction(getKey));
			
			int pos = Collections.binarySearch(mappedList, key); // get starting position by binary search
			
			if(pos >= 0) { // key found
				while(pos < values.size() && key.compareTo(getKey.apply(values.get(pos))) == 0) {
					idx.add(pos);
					pos++;
				}				
			}
			
			return idx;
		}

		public SplitInfo insert(V value, P thisCID, int levelUnused) {
			K key = getKey.apply(value);
			int insertPos = HUtil.findPos(new MappedList<V,K>(values, FunctionsJ8.toOldFunction(getKey)), key);
			values.add(insertPos, value);
			
			SplitInfo splitInfo = null;
			if(leafOverflow(values.size())) {
				splitInfo = split();
			}
			
			// update container contents of self
			container.update(thisCID, this);
			container.unfix(thisCID);
			return splitInfo;
		}
	}

	public NodeConverter getNodeConverter() {
		return new NodeConverter();
	}
	
	public class NodeConverter extends Converter<Node> {

		@Override
		public Node read(DataInput dataInput, Node unused) throws IOException {
			boolean isLeaf = dataInput.readBoolean();
			if (isLeaf) {
				return readLeafNode(dataInput);
			} else {
				return readInnerNode(dataInput);
			}
		}

		LeafNode readLeafNode(DataInput dataInput) throws IOException {
			// create Node shell
			LeafNode node = new LeafNode();

			// - read weight == number of entries
			int nChildren = dataInput.readInt();

			// -- read the content of the node
			node.values = new LinkedList<V>();
			for (int i = 0; i < nChildren; i++) {
				node.values.add(valueConverter.read(dataInput));
			}

			return node;
		}

		InnerNode readInnerNode(DataInput dataInput) throws IOException {
			InnerNode node = new InnerNode();
			
			// - read number of childs
			int nChildren = dataInput.readInt();

			// -- read separators
			node.separators = new LinkedList<K>();
			for(int i=0; i < nChildren-1; i++) { // only #childs-1 separators!
				node.separators.add(keyConverter.read(dataInput));
			}
			
			// -- read the ContainerIDs of the IndexEntries
			node.pagePointers = new LinkedList<P>();			
			for (int i = 0; i < nChildren; i++) {
				node.pagePointers.add(container.objectIdConverter().read(dataInput));
			}
			
			// -- read weights
			node.childWeights = new LinkedList<Integer>();
			for (int i = 0; i < nChildren; i++) {
				node.childWeights.add(dataInput.readInt());
			}

			return node;
		}

		@Override
		public void write(DataOutput dataOutput, Node node) throws IOException {			
			if (node.isLeaf()) {
				dataOutput.writeBoolean(true);
				writeRemainingLeafNode(dataOutput, (LeafNode) node);
			} else {
				dataOutput.writeBoolean(false);
				writeRemainingInnerNode(dataOutput, (InnerNode) node);
			}
		}

		void writeRemainingLeafNode(DataOutput dataOutput, LeafNode node) throws IOException {
			// - write number of children
			dataOutput.writeInt(node.values.size());
			
			// - write values
			for(V value : node.values) {
				valueConverter.write(dataOutput, value);
			}
		}

		void writeRemainingInnerNode(DataOutput dataOutput, InnerNode node) throws IOException {
			// - write number of children
			dataOutput.writeInt(node.pagePointers.size());

			// -- write separators
			for (K key : node.separators) {
				keyConverter.write(dataOutput, key);
			}

			// -- write ContainerIDs
			for (P childCID : node.pagePointers) {
				container.objectIdConverter().write(dataOutput, childCID);
			}
			
			// -- write weights
			for(int w : node.childWeights) {
				dataOutput.writeInt(w);
			}

		}

//		protected int indexEntrySize() {
//			// return BPlusTree.this.container().getIdSize()
//			// + keyConverter.getMaxObjectSize();
//			return this.bPlusTree.container().getIdSize() + this.bPlusTree.keyConverter.getMaxObjectSize();
//		}
//
//		protected int leafEntrySize() {
//			return this.bPlusTree.dataConverter.getMaxObjectSize();
//		}
//
//		protected int headerSize() {
//			return 2 * IntegerConverter.SIZE + BooleanConverter.SIZE + this.bPlusTree.container().getIdSize();
//		}
	}
	
	public void insert(V value) {
		
		if(rootCID == null) { // tree empty
			LeafNode root = new LeafNode();
			root.values = new ArrayList<V>();
			root.values.add(value);
			rootWeight = 1;
			rootHeight = 0;
			rootCID = container.insert(root);
			return;
		} else {
			
			SplitInfo splitInfo = container.get(rootCID).insert(value, rootCID, rootHeight);
			rootWeight++;
			
			if(splitInfo != null) { // new root
				InnerNode newroot = new InnerNode();
				
				newroot.separators = new ArrayList<K>();
				newroot.separators.add(splitInfo.separator);
				
				newroot.pagePointers = new ArrayList<P>();
				newroot.pagePointers.add(rootCID);
				newroot.pagePointers.add(splitInfo.newnodeCID);
				
				newroot.childWeights = new ArrayList<Integer>();
				newroot.childWeights.add(splitInfo.weightLeft);
				newroot.childWeights.add(splitInfo.weightRight);
				
				rootCID = container.insert(newroot);
				rootHeight++;
			}
		}
		
	}
			
	/**
	 * Lookup.
	 * @param key
	 * @return
	 */
	public List<V> get(K key) {
		int level = rootHeight;
		P nodeCID = rootCID;
		while(level > 0) {
			nodeCID = ((InnerNode) container.get(nodeCID)).chooseSubtree(key);
			level--;
		}		
		LeafNode lnode = (LeafNode) container.get(nodeCID);
		List<Integer> hitIdx = lnode.lookup(key);
		Stream<V> results = hitIdx.stream().map(lnode.values::get);
		ArrayList<V> resultsV = results.collect(Collectors.toCollection(ArrayList<V>::new));
		
		return resultsV;		
	}
	
}
