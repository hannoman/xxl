package xxl.core.indexStructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Stack;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.naming.OperationNotSupportedException;

import xxl.core.collections.MappedList;
import xxl.core.collections.containers.CastingContainer;
import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.TypeSafeContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.functions.FunJ8;
import xxl.core.io.converters.Converter;
import xxl.core.profiling.ProfilingCursor;
import xxl.core.util.HUtil;
import xxl.core.util.Interval;
import xxl.core.util.Triple;

public class WBTree<K extends Comparable<K>, V, P> implements Testable1DMap<K, V> {
	/** Standalone version of a weight-balanced B+-Tree.
	 * Based on "Optimal Dynamic Interval Management in External Memory" by L. Arge, J.S. Vitter
	 *
	 * Trying to implement a simpler version which doesn't do preemptive splitting. It instead splits 
	 * the nodes during bottom-up back traversal. Therefore it would also be beneficial to fix the whole
	 * path in memory. Although this implementation doesn't fix any pages, thereby circumventing the handicap 
	 * <tt>BPlusTree</tt>s have on <tt>BufferedContainer</tt>s (that the buffer size must always be greater or equal
	 * than the tree height).
	 * 
	 * - Mind that this is no real BPlusTree as there are no pointers between siblings and cousins.  
	 * - Working version.
	 *   
	 * TODO: generalize SplitInfo to InsertionInfo as in RSTree
	 *  
	 * @param K type of the keys
	 * @param V type of the actual data
	 * @param P type of the ContainerIDs (= CIDs)
	 */

	/** The leaf parameter <b>tK</b>, determining the amount of entries a leaf can contain.	<br>
	 * Following inequalities hold:									<br>
	 * 																<br>
	 * Number of entries in a leaf L =: e(L): 						<br>
	 * 		tK <= e(L) < 2*tK 										<br>
	 */ 
	final int tK;
	
	/** The branching parameter <b>tA</b>.							<br> 
	 * Following inequalities hold:									<br>
	 *																<br>
	 * Weight of an inner node N =: w(N) on level l:				<br>
	 * 		1/2 * tK * (tA ** l) < w(N) < 2 * tK * (tA ** l)		<br>
	 *																<br>
	 * Number of entries in an inner node N =: e(N) on level l: 	<br>
	 * 		1/4 * tA < e(N) < 4 * tA 
	 */
	final int tA;

	/** Ubiquitious getKey function which maps from values (V) to keys (K). */
	public java.util.function.Function<V, K> getKey;

	/** Container of the tree (and everything). */
	public TypeSafeContainer<P, Node> container;

	/** ContainerID of the root. */
	P rootCID;

	/** Remember how high the tree is... */
	int rootHeight;

	/** Weight-information about the root. Root has no parent so it must be saved elsewhere. */
	int rootWeight;

	/** --- Constructors & Initialisation ---
	- All mandatory arguments are put into the constructor.
	- The container gets initialized during a later call to <tt>initialize</tt> as we 
		implement the <tt>NodeConverter</tt> functionality once again (like in xxl) as inner class of this tree class.
	*/
	public WBTree(
			int leafParam, 
			int branchingParam, 
			Function<V, K> getKey) {
		super();
		this.tK = leafParam;
		this.tA = branchingParam;
		this.getKey = getKey;		
	}	

	/** Initialize the tree with a raw container (e.g. <tt>BlockFileContainer</tt>) and the needed converters.
	 * We construct the usable node container from them ourselfes.
	 * 
	 * @param rawContainer container to store the data in
	 * @param keyConverter converter for the key-type K 
	 * @param valueConverter converter for the value type V
	 */
	public void initialize_buildContainer(Container rawContainer, Converter<K> keyConverter, Converter<V> valueConverter) {
		NodeConverter nodeConverter = 
				new NodeConverter(keyConverter, valueConverter, rawContainer.objectIdConverter());
		this.container = new CastingContainer<P, Node>(new ConverterContainer(rawContainer, nodeConverter));
	}

	/** Initialize the tree with a ready-to-use container.
	 * The tree really only needs the (converting) container to store nodes. It doesn't need to know about
	 * the Converters used.
	 * 
	 * @see {@link NodeConverter#NodeConverter(Converter, Converter, Converter)}
	 * @param container a ready-to-go container which maps P to Nodes. Has to be built elsewhere.
	 */
	public void initialize_withReadyContainer(TypeSafeContainer<P, Node> container) {
		this.container = container;
	}
	

	public boolean weightUnderflow(int weight, int level) {
		return weight <= HUtil.intPow(tA, level) / 2 * tK;
	}

	public boolean weightOverflow(int weight, int level) {
		return weight >= 2 * HUtil.intPow(tA, level) * tK;
	}

	public boolean leafUnderflow(int weight) {
		return weight < tK;
	}

	public boolean leafOverflow(int weight) {
		return weight >= 2*tK;
	}

	/**
	 * Own SplitInfo class (perhaps its similar to the one used in XXL?) to encapsulate
	 * - the ContainerID of the generated Node
	 * - a key used for the separation of the nodes.
	 * - the weights of the resulting left and right node
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
			int pos = HUtil.binFindES(separators, key);
			
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
				int targetWeight = HUtil.intPow(tA, level) * tK;
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
			int pos = HUtil.binFindES(separators, key);
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
		public List<Integer> lookupIdxs(K key) {
			List<Integer> idx = new LinkedList<Integer>();
			
			List<K> mappedList = new MappedList<V,K>(values, FunJ8.toOld(getKey));
			
			int pos = HUtil.binFindSE(mappedList, key); // get starting position by binary search
			
			while(pos < values.size() && key.compareTo(getKey.apply(values.get(pos))) == 0) {
				idx.add(pos);
				pos++;
			}				
			
			return idx;
		}

		public SplitInfo insert(V value, P thisCID, int levelUnused) {
			K key = getKey.apply(value);
			int insertPos = HUtil.binFindES(new MappedList<V,K>(values, FunJ8.toOld(getKey)), key);
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
	
	
	/**
	 * Converter for the nodes of a weight-balanced B+-tree.
	 * Altough it is implemented as nested class it does not depend <b>directly</b> on any instance variables.
	 * (Only the created <tt>Node</tt> instances have an - again indirect - depency on the enclosing instance.)
	 * Instead it encapsulates all needed converters and hides them from the tree class (as the tree actually has 
	 * no use for calling them directly.<br>
	 * If one wants finer control over the constructed <tt>ConverterContainer</tt>, this class can be instantiated
	 * by <tt>WBTree<K,V,P>.NodeConverter nodeConverter = tree.new NodeConverter(...)</tt>. 
	 * 
	 * @see WBTree#initialize_withReadyContainer(TypeSafeContainer)
	 * @author Dominik Krappel
	 */
	public class NodeConverter extends Converter<Node> {

		Converter<K> keyConverter;
		Converter<V> valueConverter;
		Converter<P> cidConverter;
		
		public NodeConverter(Converter<K> keyConverter, Converter<V> valueConverter, Converter<P> cidConverter) {
			super();
			this.keyConverter = keyConverter;
			this.valueConverter = valueConverter;
			this.cidConverter = cidConverter;
		}

		@Override
		public Node read(DataInput dataInput, Node unused) throws IOException {
			boolean isLeaf = dataInput.readBoolean();
			if (isLeaf) return readLeafNode(dataInput);
			else 		return readInnerNode(dataInput);			
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
	
	public boolean insert(V value) {
		
		if(rootCID == null) { // tree empty
			LeafNode root = new LeafNode();
			root.values = new ArrayList<V>();
			root.values.add(value);
			rootWeight = 1;
			rootHeight = 0;
			rootCID = container.insert(root);
			return true;
		} else {
			int oldWeight = rootWeight;
			SplitInfo splitInfo = container.get(rootCID).insert(value, rootCID, rootHeight);
			
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
		
		List<Integer> hitIdx = lnode.lookupIdxs(key);
		Stream<V> results = hitIdx.stream().map(lnode.values::get);
		ArrayList<V> resultsV = results.collect(Collectors.toCollection(ArrayList<V>::new));
		
		return resultsV;		
	}
	
	public Cursor<V> rangeQuery(K lo, K hi){
		return new QueryCursor(lo, hi);
	}
	
	// public class QueryCursor extends xxl.core.indexStructures.QueryCursor {
	/* we won't subclass xxl.core.indexStructures.QueryCursor here as it is supposed for queries over trees which inherit 
	 	from xxl.core.indexStructures.Tree */

	/**
	 * A query cursor for simple range queries. 
	 */
	public class QueryCursor extends AbstractCursor<V> {
		final K lo;
		final K hi;		

		
		Stack<P> sNodes; // container.get(sNodes.peek()) =: current node		
		Stack<Integer> sIdx; // sIdx.peek() =: current index
		
		V precomputed;
		
		public QueryCursor(K lo, K hi, P startNode) {
			super();
			this.lo = lo;
			this.hi = hi;
			
			sNodes = new Stack<P>();
			sNodes.push(startNode);
			
			sIdx = new Stack<Integer>();			
						
			precomputed = null; // the next value to spit out
		}
		
		public QueryCursor(K lo, K hi) {
			this(lo, hi, rootCID);
		}

		/** Finds the path to the first entry and locks its nodes in the buffer of the container. */
		@Override
		public void open() {
			// get the current node and lock it in the buffer
			Node curNode = container.get(sNodes.peek());
			
			while(curNode.isInner()) {
				InnerNode curINode = (InnerNode) curNode;  
				
				// find the index of the next childnode
				int nextPos = HUtil.binFindES(curINode.separators, lo);
				sIdx.push(nextPos);
				
				// descend to next node
				P nextPID = curINode.pagePointers.get(nextPos);
				sNodes.push(nextPID);
				curNode = container.get(sNodes.peek());
			}
			
			// now our node is a leaf and we just need to find the starting position			
			LeafNode curLNode = (LeafNode) curNode;
			
			// find starting position
			List<K> mappedList = new MappedList<V,K>(curLNode.values, FunJ8.toOld(getKey));			
			int pos = HUtil.binFindES(mappedList, lo);
			sIdx.push(pos);
			
			// regarding first computation of hasNext:
			// we decrement the position here so that the following call of hasNext computes the right element
			sIdx.push(sIdx.pop() - 1); 			
			
			//- sets the open flag
			super.open();
		}

		@Override
		public void close() {			
			// release locked path
			while(!sNodes.empty())
				container.unfix(sNodes.pop());
			super.close();
		}
		
		private void descendToSmallest() {
			// get the current node and _don't_ fix it in the buffer
			Node curNode = container.get(sNodes.peek());			
			
			while(curNode.isInner()) {				
				InnerNode curINode = (InnerNode) curNode;
				// set the index of the current node
				sIdx.push(0);
				
				P nextPID = curINode.pagePointers.get(sIdx.peek());
				sNodes.push(nextPID);
				curNode = container.get(sNodes.peek());
			}
			
			// set the index in the leaf node too
			sIdx.push(0);
		}
		
		private boolean switchToNextNode() {
			// TODO: would be clearer if not recursive.
			
			// release the active node and index and unfix from the buffer 
			container.unfix(sNodes.pop()); 
			sIdx.pop();
			
			if(sNodes.empty()) // recursion exit, no value-next node can be found
				return false;
			
			// get the right brother from the parent node if present..
			InnerNode pNode = (InnerNode) container.get(sNodes.peek());
			sIdx.push(sIdx.pop() + 1); // increment counter		
			if(sIdx.peek() < pNode.pagePointers.size()) {
				sNodes.push(pNode.pagePointers.get(sIdx.peek()));
				descendToSmallest();
				return true;
			} else { // ..if not call myself recursively				
				return switchToNextNode();
			}
		}

		/** We just need to precompute the value here, all the other logic is handled by AbstractCursor. */ 
		protected boolean hasNextObject() {		
			LeafNode curLNode = (LeafNode) container.get(sNodes.peek());
			sIdx.push(sIdx.pop() + 1); // = increment counter			
	
			if(sIdx.peek() >= curLNode.values.size()) { // we need to switch to the node which has the next values
				if(switchToNextNode())
					// fetch the updated leaf node again // TODO separate tail of stack from the rest
					curLNode = (LeafNode) container.get(sNodes.peek());
				else  
					return false; // hit the right border of the index structure				
			}
			
			precomputed = curLNode.values.get(sIdx.peek());
			if(getKey.apply(precomputed).compareTo(hi) > 0) 
				return false; // hit the high border
			
			return true;
		}

		@Override
		protected V nextObject() {
			return precomputed;
		}

	}
	
	//-------------------------------------------------------------------------------
	//--- stupid stuff for interfaces
	//-------------------------------------------------------------------------------
	@Override
	public int height() { return rootHeight; }


	@Override
	public Function<V, K> getGetKey() {
		return getKey;
	}

	public ProfilingCursor<V> rangeQuery(Interval<K> query) {
		assert false : "Operation not supported.";
		return null;
	} 
	
}
