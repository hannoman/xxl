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
import xxl.core.collections.containers.CastingContainer;
import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.TypeSafeContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.functions.FunctionsJ8;
import xxl.core.io.converters.Converter;
import xxl.core.util.HUtil;
import xxl.core.util.Triple;

public class RSTree_v2<K extends Comparable<K>, V, P> {
	/** Implementation of the RS-Tree for 1-dimensional data.
	 * 
	 * Skeleton of WBTree used, as the RSTree also needs information about the weight of the nodes. 
	 *   
	 * @param K type of the keys
	 * @param V type of the actual data
	 * @param P type of the ContainerIDs (= CIDs)
	 */

	/** How many samples per node should be kept = parameter s. 
	 * The buffers must have between s/2 and 2*s items at all times. */
	final int samplesPerNode;
	
	/** The branching parameter == the fanout. */
	final int branchingParam;
	final int branchingLo, branchingHi;	
	final int leafLo, leafHi;
	
	/** Ubiquitious getKey function which maps from values (V) to keys (K). */
	public Function<V, K> getKey;

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
	public RSTree_v2(
			int samplesPerNode, 
			int branchingParam, 
			Function<V, K> getKey) {
		super();
		this.samplesPerNode = samplesPerNode;
		this.branchingParam = branchingParam;
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
	
//
//	public boolean weightUnderflow(int weight, int level) {
//		return weight <= HUtil.intPow(branchingParam, level) / 2 * tK;
//	}
//
//	public boolean weightOverflow(int weight, int level) {
//		return weight >= 2 * HUtil.intPow(branchingParam, level) * tK;
//	}
//
//	public boolean leafUnderflow(int weight) {
//		return weight < tK;
//	}
//
//	public boolean leafOverflow(int weight) {
//		return weight >= 2*tK;
//	}

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
		
		/** The list of samples kept in this node. */
		List<V> samples;
		
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
		
		public SplitInfo split() {			
			int splitPos = pagePointers.size() / 2; // as we now work with B-Tree splitting just split in the middle.
			
			InnerNode newode = new InnerNode();
			
			//- split separators
			// separators[splitPos] becomes the separator between the offspring 
			newode.separators = HUtil.splitOffRight(separators, splitPos, new ArrayList<K>());
			K offspringSeparator = separators.remove(splitPos);
			
			//- split pointers and weights
			newode.pagePointers = HUtil.splitOffRight(pagePointers, splitPos, new ArrayList<P>());			
			newode.childWeights = HUtil.splitOffRight(childWeights, splitPos, new ArrayList<Integer>());
			//-- recalculate resulting weights again
			int remLeft = 0;
			for(int w : childWeights) remLeft += w;
			int remRight = 0;
			for(int w : newode.childWeights) remRight += w;
			
			//-- split samples
			int keepSamples = samples.size() / 2;
			newode.samples = HUtil.splitOffRight(samples, keepSamples, new ArrayList<V>());
			//-- replenish samples if underflown
			if(sampleUnderflow())
				redrawSamples();
			if(newode.sampleUnderflow())
				newode.redrawSamples();			
			
			//- put new node into Container
			P newodeCID = container.insert(newode);
			
			return new SplitInfo(newodeCID, offspringSeparator, remLeft, remRight);			
		}
		
		public SplitInfo insert(V value, P thisCID, int level) {
			K key = getKey.apply(value);
			
			//- insert in sublevel
			int pos = HUtil.binFindL(separators, key);
			
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
			if(overflow()) {
				splitInfo = split();
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
			int pos = HUtil.binFindL(separators, key);
			return pagePointers.get(pos);
		}

		public boolean overflow() {
			return pagePointers.size() > branchingHi;
		}

		public boolean underflow() {
			return pagePointers.size() < branchingLo;
		}
		
		public boolean sampleUnderflow() {
			return samples.size() < samplesPerNode / 2;			
		}
		
		/** Replenishing of a sample buffer by draining from the childs' buffers. Might invoke recursive replenishing.
		 * 
		 * Ok, so what are we supposed to do here exactly?
		 * --> Q: What number of samples should we aim for? 
		 * 			avg = s, min = s / 2 or max = s * 2
		 *		  Or should we make it dependant on how many "excess" samples are readily available in the child nodes?
		 * --> Q: Draw how much from which child?
		 * 			Imo, we can only draw accordingly to the subtree sizes of the childs to get a correct sample.
		 * 			--> is this exactly determined or do we have to draw probabilistic?
		 * 
		 * @param d amount of excess samples we want to generate for parent nodes.
		 */
		public List<V> redrawSamples(int d) {
			if(samples.size() < samplesPerNode) { // buffer is so empty we want to fill it
				d += 2*samplesPerNode - samples.size(); 
			}
			
			int myWeight = totalWeight();
			LinkedList<V> newSamples = new LinkedList<V>();
			
			//-- determing how much samples we need from each child
			/* TODO: this is wrong as it does not lead to a uniform sample over the whole subtree
			 * as only those samples are considered, whose partitions (built from the boundaries of the subtrees)
			 * have sizes according to the expected case.
			 * Instead we have to generate newsamplesPerChild probabilistic (through a binomial distribution).   
			 */
			int[] newsamplesPerChild = HUtil.distributeByAbsoluteWeights(d, childWeights);
			
			
			
			
			
			return excessSamples;
			// TODO
			// pass
		}
	}

	public class NearLeafNode extends Node {
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
			int pos = HUtil.binFindL(separators, key);
			
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
			if(overflow()) {
				// TODO
				int targetWeight = HUtil.intPow(branchingParam, level) * tK;
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
			int pos = HUtil.binFindL(separators, key);
			return pagePointers.get(pos);
		}
	
		public boolean overflow() {
			return pagePointers.size() > branchingHi;
		}

		public boolean underflow() {
			return pagePointers.size() < branchingLo;
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
			int insertPos = HUtil.binFindL(new MappedList<V,K>(values, FunctionsJ8.toOldFunction(getKey)), key);
			values.add(insertPos, value);
			
			SplitInfo splitInfo = null;
			if(overflow()) {
				splitInfo = split();
			}
			
			// update container contents of self
			container.update(thisCID, this);
			container.unfix(thisCID);
			return splitInfo;
		}
		
		public boolean overflow() {
			return values.size() > leafHi;
		}
		
		public boolean underflow() {
			return values.size() < leafLo;
		}
	}
	
	
	/**
	 * Converter for the nodes of a weight-balanced B+-tree.
	 * Altough it is implemented as nested class it does not depend <b>directly</b> on any instance variables.
	 * (Only the created <tt>Node</tt> instances have an - again indirect - depency on the enclosing instance.)
	 * Instead it encapsulates all needed converters and hides them from the tree class (as the tree actually has 
	 * no use for calling them directly.<br>
	 * If one wants finer control over the constructed <tt>ConverterContainer</tt>, this class can be instantiated
	 * by <tt>WBTreeSA_v3<K,V,P>.NodeConverter nodeConverter = tree.new NodeConverter(...)</tt>. 
	 * 
	 * @see WBTreeSA_v3#initialize_withReadyContainer(TypeSafeContainer)
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
