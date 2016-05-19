package xxl.core.indexStructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Queue;
import java.util.Random;
import java.util.Stack;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import xxl.core.collections.MappedList;
import xxl.core.collections.containers.CastingContainer;
import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.TypeSafeContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.samplingcursor.StatefulSampler;
import xxl.core.cursors.sources.InfiniteSampler;
import xxl.core.functions.FunJ8;
import xxl.core.io.converters.Converter;
import xxl.core.util.HUtil;
import xxl.core.util.Randoms;
import xxl.core.util.Sample;

public class RSTree_v3<K extends Comparable<K>, V, P> implements TestableMap<K, V, P> {
	/** Implementation of the RS-Tree for 1-dimensional data.
	 * 
	 * Skeleton of WBTree used, as the RSTree also needs information about the weight of the nodes.
	 * 
	 * Q: perhaps roll InsertionInfo back to SplitInfo and always do a lookup before a modificating operation
	 * 		path to node should then be buffered and therefore not incur any additional IOs.
	 * 		-> nope, keep it that way, since it might be beneficial for batched operations in expansions,
	 * 			and removals of multiple values wouldn't benefit from it.
	 * 
	 *   DONE Mini-Milestone 1: Implement sample buffer maintenance for insertions.
	 *   DONE Mini-Milestone 1.5: Implement QueryCursor for range queries.
	 *   DONE Mini-Milestone 2: Implement lazy sampling query cursor
	 *   Mini-Milestone 3: Supply functions from the NodeConverter for estimating the amount of entries in leaf and inner nodes.
	 *   			And to automatically construct a tree with optimally set parameters. 
	 *   Mini-Milestone: Do real tests.
	 *   		--> see xxl.core.math.statistics.parametric.aggregates.ConfidenceAggregationFunction
	 *   Mini-Milestone: Generalize Query-Types
	 *   Mini-Milestone: Support removals
	 *      
	 *   
	 * @param K type of the keys
	 * @param V type of the actual data
	 * @param P type of the ContainerIDs (= CIDs)
	 */

	/** How many samples per node should be kept = parameter s. 
	 * The buffers must have between s/2 and 2*s items at all times. */
//	final int samplesPerNode;
	final int samplesPerNodeLo; // std: = samplesPerNode / 2
	final int samplesPerNodeHi; // std: = samplesPerNode * 2
	final int samplesPerNodeReplenishTarget; // how full shall the buffer be made if we have to replenish it? // std = samplesPerNodeHi
	
	/** RNG used for drawing samples and such. Use {@link #setRNG} to set it. */
	Random rng;
	
	/** The branching parameter == the fanout. */
//	final int branchingParam;
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
//	int rootWeight; // for now don't track this information

	/** --- Constructors & Initialisation ---
	- All mandatory arguments are put into the constructor.
	- The container gets initialized during a later call to <tt>initialize</tt> as we 
		implement the <tt>NodeConverter</tt> functionality once again (like in XXL) as inner class of this tree class.
	*/
	public RSTree_v3(int samplesPerNode, int branchingParam, int leafLo, int leafHi, Function<V, K> getKey) {
		super();
//		this.branchingParam = branchingParam;		
		this.getKey = getKey;
		
		this.leafLo = leafLo;
		this.leafHi = leafHi;
		
		//- setting defaults
		this.branchingLo = branchingParam / 2;
		this.branchingHi = branchingParam * 2;
		this.samplesPerNodeLo = samplesPerNode / 2;
		this.samplesPerNodeHi = samplesPerNode * 2;
		this.samplesPerNodeReplenishTarget = samplesPerNode;		
		this.rng = new Random();
	}

	
	

	public RSTree_v3(int samplesPerNodeLo, int samplesPerNodeHi, int branchingLo, int branchingHi, int leafLo, int leafHi, Function<V, K> getKey) {
		super();
		this.samplesPerNodeLo = samplesPerNodeLo;
		this.samplesPerNodeHi = samplesPerNodeHi;
		this.branchingLo = branchingLo;
		this.branchingHi = branchingHi;
		this.leafLo = leafLo;
		this.leafHi = leafHi;
		this.getKey = getKey;
		
		// defaults
		this.samplesPerNodeReplenishTarget = this.samplesPerNodeHi;
		this.rng = new Random();
	}




	/** Initialize the tree with a raw container (e.g. <tt>BlockFileContainer</tt>) and the needed converters.
	 * We construct the usable node container from them ourselfes.
	 * 
	 * @param rawContainer container to store the data in
	 * @param keyConverter converter for the key-type K 
	 * @param valueConverter converter for the value type V
	 */
	@Override
	public void initialize_buildContainer(Container rawContainer, Converter<K> keyConverter, Converter<V> valueConverter) {
		NodeConverter nodeConverter = 
				new NodeConverter(keyConverter, valueConverter, rawContainer.objectIdConverter());
		this.container = new CastingContainer<P, Node>(new ConverterContainer(rawContainer, nodeConverter));
	}

	public void initialize_withReadyContainer(TypeSafeContainer<P, Node> container) {
		this.container = container;
	}

	/** For repeatable results in testing. */
	public void setRNG(Random rng) {
		this.rng = rng;
	}
	
	/**
	 * Generalization of SplitInfo class which is used to report the result of an
	 * operation in a subtree. This can either be the information that and how the child has split (~ SplitInfo),
	 * or the count of the entries that got changed (think of removals, or insertions in duplicate free trees) which
	 * is needed to maintain aggregate meta-information. 
	 */
	class InsertionInfo {
		boolean isSplit = false;
		int weightNew = 0;
		// TODO: for single insertions we don't need the new weight, but only the info if it was
		// boolean insertionSuccessful = true;  
		
		P newnodeCID = null;
		K separator = null;
		int weightLeft = -1;
		int weightRight = -1;		
		
		public InsertionInfo(P newnodeCID, K separator, int weightLeft, int weightRight) {		
			this.isSplit = true;
			this.newnodeCID = newnodeCID;
			this.separator = separator;
			this.weightLeft = weightLeft;
			this.weightRight = weightRight;
		}

		public InsertionInfo(int weightNew) {
			this.isSplit = false;
			this.weightNew = weightNew;
		}
	}

	//-- Node class
	public abstract class Node {
		
		public abstract boolean isLeaf();
		
		public boolean isInner() { return !isLeaf(); }
		
		public abstract InsertionInfo insert(V value, P thisCID, int level);

		protected abstract List<V> drainSamples(int amount);

		public abstract int totalWeight();

		protected abstract List<V> relevantValues(K lo, K hi);
	}
	
	public class InnerNode extends Node {		
		protected List<K> separators;
		protected List<P> pagePointers;
		protected List<Integer> childWeights;
		
		/** The list of samples kept in this node. */
		LinkedList<V> samples;
		
		public boolean isLeaf() { return false; }
		
		/** A node should have a sample buffer attached if its weight is greater than 2*s,
		 * but what's exactly the rationale behind this? Doesn't the other parameters like leafCapacity
		 * and branchingParam also be taken into account?
		 */
		public boolean hasSampleBuffer() { return samples != null; }

		public boolean overflow() {
			return pagePointers.size() > branchingHi;
		}

		public boolean underflow() {
			return pagePointers.size() < branchingLo;
		}

		public boolean sampleUnderflow() {
			return samples.size() < samplesPerNodeLo;			
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

		@Override
		public int totalWeight() {
			// OPT: somehow prevent recalculation every time.
			return childWeights.stream().reduce(0, (x,y) -> x+y);			
		}

		public InsertionInfo insert(V value, P thisCID, int level) {
			K key = getKey.apply(value);
			
			//- insert in sublevel
			int pos = HUtil.binFindES(separators, key);			
			P nextCID = pagePointers.get(pos);
			Node nextNode = container.get(nextCID);
			int oldWeight = totalWeight(); 
					
			InsertionInfo childInsertInfo = nextNode.insert(value, nextCID, level-1); //== RECURSION ==
			
			if(childInsertInfo.isSplit) { // a split occured in child and we need to update the directory
				separators.add  (pos  , childInsertInfo.separator);
				pagePointers.add(pos+1, childInsertInfo.newnodeCID);
				childWeights.set(pos  , childInsertInfo.weightLeft);
				childWeights.add(pos+1, childInsertInfo.weightRight);
			} else { // update weight of child
				// .. this can only be done after insertion on leaf level, as only then it's clear how the weight was affected.
				childWeights.set(pos, childInsertInfo.weightNew);
			}
			
			// container.unfix(nextCID); // release lock on the childs memory			
			
			//- maintain sample buffer (which acts similiar to a reservoir sample on the passing values)
			int curWeight = totalWeight();
			if(hasSampleBuffer() && curWeight - oldWeight > 0) { // insertion actually took place 
				/* Replace every item currently present in the sample buffer with probability 1 / curWeight 
				 * with the newly inserted item.
				 * QUE: This is like it is described in the paper, but perhaps we can improve on this? */ 
				double p = 1.0 / (double)curWeight;				
				for(ListIterator<V> sampleIter = samples.listIterator(); sampleIter.hasNext(); ) {
					sampleIter.next();
					if(rng.nextDouble() < p)
						sampleIter.set(value);
				}
			}
			
			//- check for split here
			InsertionInfo insertionInfo = null;
			if(overflow())
				insertionInfo = split();
			else
				insertionInfo = new InsertionInfo(totalWeight());
			
			container.update(thisCID, this);
			
			return insertionInfo;
		}

		public InsertionInfo split() {			
			int splitPos = pagePointers.size() / 2; // B-Tree splitting == just split in the middle.
			
			InnerNode newode = new InnerNode();
			
			//- split separators
			// separators[splitPos] becomes the separator between the offspring 
			newode.separators = HUtil.splitOffRight(separators, splitPos, new ArrayList<K>()); // CHECK LinkedList seems to make more sense here
			K offspringSeparator = separators.remove(splitPos-1);
			
			//- split pointers and weights
			newode.pagePointers = HUtil.splitOffRight(pagePointers, splitPos, new ArrayList<P>());			
			newode.childWeights = HUtil.splitOffRight(childWeights, splitPos, new ArrayList<Integer>());
			
			//-- recalculate resulting weights again
			int weightLeft = this.childWeights.stream().reduce(0, (x,y) -> x+y);
			int weightRight = newode.childWeights.stream().reduce(0, (x,y) -> x+y);
			
			//-- distribute samples among the resulting nodes
			// we have to distinguish whether the resulting nodes still have buffers attached
			if(this.samples != null) {
				LinkedList<V> samplesLeft = new LinkedList<V>();
				LinkedList<V> samplesRight = new LinkedList<V>();
				for(V sample : samples) {
					if(getKey.apply(sample).compareTo(offspringSeparator) >= 0)
						samplesRight.add(sample);
					else
						samplesLeft.add(sample);
				}
				
				//- attach filtered samples to new nodes if they should have an attached sample buffer
				// OPT: could we do something instead of trashing the samples when they aren't needed?
				if(weightLeft > samplesPerNodeHi) {
					this.samples = samplesLeft;
					this.repairSampleBuffer();
				} else
					this.samples = null;
				
				if(weightRight > samplesPerNodeHi) {
					newode.samples = samplesRight;
					newode.repairSampleBuffer();
				} else
					newode.samples = null;
			}
			
			//- put new node into Container
			P newodeCID = container.insert(newode);
			
			return new InsertionInfo(newodeCID, offspringSeparator, weightLeft, weightRight);			
		}
		
		/**
		 * Returns all values relevant for a given query in this' node subtree. 
		 * Needed for the sampling cursor when we have no sample buffer attached to a node.
		 * OPT: only called from xxl.core.indexStructures.RSTree_v3.SamplingCursor.addToFrontier(P) -> inline?
		 */
		protected List<V> relevantValues(K lo, K hi) {
			List<V> allValues = new LinkedList<V>(); // OPT use something which allows for O(1) concatenation
			for(int i : relevantChildIdxs(lo, hi)) {
				Node child = container.get(pagePointers.get(i));
				allValues.addAll(child.relevantValues(lo,hi));
			}
			return allValues;
		}
		
		/**
		 * Returns the indices of all childs relevant for a given query. 
		 */
		protected List<Integer> relevantChildIdxs(K lo, K hi) {
			List<Integer> relChilds = new LinkedList<Integer>(); // OPT use something which allows for O(1) concatenation
			
			// CHECK whether this correctly uses all nodes in the canonical set
			
			// TODO: use general format for queries, unspecific to the 1-dimensional case.
			int startPos = HUtil.binFindES(separators, lo);
			int endPos = HUtil.binFindES(separators, hi);
			for(int i=startPos; i <= endPos; i++)
				relChilds.add(i);
			return relChilds;
		}
		
		protected List<P> relevantChildCIDs(K lo, K hi) {
			return HUtil.getAll(relevantChildIdxs(lo, hi), pagePointers);
		}
		
		/**
		 * Checks for a underflow in the sample buffer and repairs it.
		 * Repairing for InnerNodes is done by draining samples from the child nodes.
		 * OPT: only called from xxl.core.indexStructures.RSTree_v3.InnerNode.split() -> inline?
		 */
		protected void repairSampleBuffer() {
			if(sampleUnderflow()) {				
				int toDraw = samplesPerNodeReplenishTarget - samples.size();
				refillSamplesFromChildren(toDraw); 
			}
		}
		
		@Override
		protected List<V> drainSamples(int amount) {
			if(samples.size() - amount < samplesPerNodeLo) { // we have to refill, this includes the case where amount > samples.size()
				int toRedraw = amount + samplesPerNodeReplenishTarget - samples.size();
				refillSamplesFromChildren(toRedraw);				
			}
			
			// as refillSamplesFromChildren now does the permutation we can return the first elements
			List<V> toYield = HUtil.splitOffLeft(samples, amount, new LinkedList<V>());
			return toYield;
		}
		
		protected void refillSamplesFromChildren(int amount) {
			//-- determining how much samples we need from each child
			ArrayList<Integer> nSamplesPerChild = Randoms.multinomialDist(childWeights, amount, rng);
			
			//-- fetch samples from children
			LinkedList<V> newSamples = new LinkedList<V>();
			for (int i = 0; i < pagePointers.size(); i++) {
				Node child = container.get(pagePointers.get(i));
				List<V> fetchedFromChild = child.drainSamples(nSamplesPerChild.get(i));
				// .. and put them in sample buffer
				samples.addAll(fetchedFromChild);
			}
			
			//-- permute the newly built sample buffer
			Sample.permute(samples, rng);
		}
		
	}

	public class LeafNode extends Node {
		List<V> values;

		public boolean isLeaf() { return true; }
		
		public boolean overflow() {
			return values.size() > leafHi;
		}

		public boolean underflow() {
			return values.size() < leafLo;
		}

		public InsertionInfo insert(V value, P thisCID, int levelUnused) {
			K key = getKey.apply(value);
			
			//- insert new element // OPT: support for duplicate free trees
			int insertPos = HUtil.binFindES(new MappedList<V,K>(values, FunJ8.toOld(getKey)), key);
			values.add(insertPos, value);
			
			InsertionInfo insertInfo = null;
			if(overflow())
				insertInfo = split();
			else
				insertInfo = new InsertionInfo(values.size());
			
			// update container contents of self
			container.update(thisCID, this);
			container.unfix(thisCID);
			
			return insertInfo;
		}

		/**
		 * Splits the leaf in the middle.
		 */
		public InsertionInfo split() {
			LeafNode newode = new LeafNode();
			int remLeft = values.size() / 2;
			int remRight = values.size() - remLeft;
			
			newode.values = HUtil.splitOffRight(values, values.size() / 2, new ArrayList<V>());
			K separator = getKey.apply(values.get(values.size()-1));
			
			//- put new node into Container
			P newodeCID = container.insert(newode);
			
			return new InsertionInfo(newodeCID, separator, remLeft, remRight);
		}
		
		/**
		 * Returns the indices of the found values. 
		 * @param key key to look for
		 * @return list of indices i with "getKey(values[i]) == key" 
		 */
		public List<Integer> lookup(K key) {
			List<Integer> idx = new LinkedList<Integer>();
			
			List<K> mappedList = new MappedList<V,K>(values, FunJ8.toOld(getKey));
			
			int pos = Collections.binarySearch(mappedList, key); // get starting position by binary search
			
			if(pos >= 0) { // key found
				while(pos < values.size() && key.compareTo(getKey.apply(values.get(pos))) == 0) {
					idx.add(pos);
					pos++;
				}				
			}
			
			return idx;
		}

		/** Draws a WR-sample (with replacement (!)) from the underlying values. */
		protected List<V> drainSamples(int amount) {
			return Sample.wrKeep(values, amount, rng);
		}

		public int totalWeight() {
			return values.size();
		}

		/**
		 * Returns all values relevant for a given query in this' node subtree. 
		 * Needed for the sampling cursor when we have no sample buffer attached to a node.
		 */
		@Override
		protected List<V> relevantValues(K lo, K hi) {
			List<V> allValues = new LinkedList<V>();
						
			for(V value : values) {
				K key = getKey.apply(value);
				if(key.compareTo(lo) >= 0 && key.compareTo(hi) <= 0) // TODO: use general format for queries, unspecific to the 1-dimensional case.
					allValues.add(value);
			}
			return allValues;
		}
	}
	
	
	/**
	 * Converter for the nodes of a RS-tree. <br>
	 * Altough it is implemented as nested class it does not depend <b>directly</b> on any instance variables.
	 * (Only the created <tt>Node</tt> instances have an - again indirect - depency on the enclosing instance.)
	 * Instead it encapsulates all needed converters and hides them from the tree class (as the tree actually has 
	 * no use for calling them directly.<br>
	 * If one wants finer control over the constructed <tt>ConverterContainer</tt>, this class can be instantiated
	 * by <tt>RSTree_v3<K,V,P>.NodeConverter nodeConverter = tree.new NodeConverter(...)</tt>. 
	 * 
	 * @see RSTree_v3#initialize_withReadyContainer(TypeSafeContainer)
	 */
	@SuppressWarnings("serial")
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

		Node readLeafNode(DataInput dataInput) throws IOException {
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

		Node readInnerNode(DataInput dataInput) throws IOException {
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
			
			// - read number of samples
			int nSamples = dataInput.readInt();
			if(nSamples > 0) node.samples = new LinkedList<V>();
			// -- read samples
			for (int i = 0; i < nSamples; i++) {
				node.samples.add(valueConverter.read(dataInput));
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
			
			// -- write samples
			if(node.hasSampleBuffer()) {
				dataOutput.writeInt(node.samples.size());
				for(V sample : node.samples) {
					valueConverter.write(dataOutput, sample);
				}					
			} else {
				dataOutput.writeInt(0);
			}
			
		}
	}
	
	/**
	 * Insertion. 
	 */
	@Override
	public void insert(V value) {
		if(rootCID == null) { // tree empty
			LeafNode root = new LeafNode();
			root.values = new ArrayList<V>(leafHi);
			root.values.add(value);
			rootHeight = 0;
			rootCID = container.insert(root);
			return;
		} else {
			InsertionInfo insertionInfo = container.get(rootCID).insert(value, rootCID, rootHeight);			
			
			if(insertionInfo.isSplit) { // new root
				InnerNode newroot = new InnerNode();
				
				newroot.separators = new ArrayList<K>(branchingHi-1);
				newroot.separators.add(insertionInfo.separator);
				
				newroot.pagePointers = new ArrayList<P>(branchingHi);
				newroot.pagePointers.add(rootCID);
				newroot.pagePointers.add(insertionInfo.newnodeCID);
				
				newroot.childWeights = new ArrayList<Integer>(branchingHi);
				newroot.childWeights.add(insertionInfo.weightLeft);
				newroot.childWeights.add(insertionInfo.weightRight);
				
				// FIX: we forgot to fill the root node with samples again
				if(newroot.totalWeight() > samplesPerNodeHi) {
					newroot.samples = new LinkedList<V>();
					newroot.repairSampleBuffer();
				}
				
				rootCID = container.insert(newroot);
				rootHeight++;
			} else {
//				rootWeight++;
			}
		}		
	}
			
	/**
	 * Lookup.
	 */
	@Override
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

	@Override
	public Cursor<V> rangeQuery(K lo, K hi){
		return new QueryCursor(lo, hi);
	}
	
	/* TODO: we need different range queries for R-trees:
	 * 		- Q intersects R
	 * 		- Q contains R
	 * 		- Q is contained in R
	 */
	
	/**
	 * A query cursor for simple range queries.
	 * 
	 * We won't subclass xxl.core.indexStructures.QueryCursor here as it is 
	 * supposed for queries over trees which inherit from xxl.core.indexStructures.Tree.
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
			Node curNode = container.get(sNodes.peek(), false);
			
			while(curNode.isInner()) {
				InnerNode curINode = (InnerNode) curNode;  
				
				// find the index of the next childnode
				int nextPos = HUtil.binFindES(curINode.separators, lo);
				sIdx.push(nextPos);
				
				// descend to next node
				P nextPID = curINode.pagePointers.get(nextPos);
				sNodes.push(nextPID);
				curNode = container.get(sNodes.peek(), false);
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
			// get the current node and fix it in the buffer
			Node curNode = container.get(sNodes.peek(), false);			
			
			while(curNode.isInner()) {				
				InnerNode curINode = (InnerNode) curNode;
				// set the index of the current node
				sIdx.push(0);
				
				P nextPID = curINode.pagePointers.get(sIdx.peek());
				sNodes.push(nextPID);
				curNode = container.get(sNodes.peek(), false);
			}
			
			// set the index in the leaf node too
			sIdx.push(0);
		}
		
		private boolean switchToNextNode() {
			// OPT: would perhaps be clearer if not recursive.
			
			// release the active node and index and unfix from the buffer 
			container.unfix(sNodes.pop()); 
			sIdx.pop();
			
			if(sNodes.empty()) // recursion exit, no value-next node can be found
				return false;
			
			// get the right brother from the parent node if present..
			InnerNode pNode = (InnerNode) container.get(sNodes.peek());
			sIdx.push(sIdx.pop() + 1); // increment counter		
			if(sIdx.peek() < pNode.pagePointers.size()) { // switch over if we have a right brother
				sNodes.push(pNode.pagePointers.get(sIdx.peek()));
				descendToSmallest();
				return true;
			} else { // ..if not call myself recursively, that means looking for a brother of the parent				
				return switchToNextNode();
			}
		}
	
		/* We just need to precompute the value here, all the other logic is handled by AbstractCursor. */ 
		protected boolean hasNextObject() {		
			LeafNode curLNode = (LeafNode) container.get(sNodes.peek());
			sIdx.push(sIdx.pop() + 1); // = increment counter			
	
			if(sIdx.peek() >= curLNode.values.size()) { // we need to switch to the node which has the next values
				if(switchToNextNode())
					// fetch the updated leaf node again, that is the state change incured by switchToNextNode() 
					// TODO: separate tail of stack from the rest
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
	
	
	public Cursor<V> samplingRangeQuery(K lo, K hi){
		return new LazySamplingCursor(lo, hi);
	}

	/**
	 * Modification of Algorithm 1 of the paper. Tries to omit unsuccessful sampling attempts. 
	 * See algorithm 1 in the paper.
	 * Note that this doesn't correspond to the exact query if run till the end 
	 * (as there is no end), as this cursor produces values infinitely 
	 * (and reports values multiple times, even before all unique values are exhausted).  
	 */
	public class LazySamplingCursor extends AbstractCursor<V> {
		// the query // TODO: generalize from 1-dimensional case
		final K lo;
		final K hi;			
		
		/** Whether the cursor fixes the frontier nodes in the buffer. Mind that this cursor will eventually load
		 * the whole base relation that way.. */
		boolean fixing;
		
		/** Temporary variable to store nodes to inspect between construction and call to open() 
		 * as we only want to reserve ressources after open().
		 */
		List<P> initialCIDs; 
		
		List<P> frontierCIDs;

		List<Integer> weights;
		List<Integer> accWeights;
		int totalWeight;
		
		List<Iterator<V>> samplers; // FIXME: we still have to permute the iterators of the sample buffers 
		
		Queue<V> precomputed;
		
		/** Constructs a new SamplingCursor for the given query, which doesn't fix the frontier nodes in buffer. */
		public LazySamplingCursor(K lo, K hi) {
			this(lo, hi, false);
		}
		/** Constructs a new SamplingCursor for the given query, whether <tt>fixing</tt> the nodes in the buffer or not. */
		public LazySamplingCursor(K lo, K hi, boolean fixing) {
			this(lo, hi, Arrays.asList(rootCID), fixing);
		}		
		/* real constructor. */
		private LazySamplingCursor(K lo, K hi, List<P> initialCIDs, boolean fixing) {
			super();
			// query
			this.lo = lo;
			this.hi = hi;
			// nodes to inspect
			this.initialCIDs = initialCIDs;
			// config
			this.fixing = fixing;
			// state
			frontierCIDs = new LinkedList<P>();
			weights = new LinkedList<Integer>();
			accWeights = new ArrayList<Integer>(); // as accesses should outnumber modifications use ArrayList
			totalWeight = 0;
			samplers = new ArrayList<Iterator<V>>();
			// next values to spit out
			precomputed = new LinkedList<V>(); 
		}
	
		/** Finds the path to the first entry and locks its nodes in the buffer of the container. */
		@Override
		public void open() {
			for(P iniCID : initialCIDs)
				addToFrontier(iniCID);			
			//- sets the open flag
			super.open();
		}
		
		/**
		 * Only called from open().
		 */
		private void addToFrontier(P nodeCID) {
			Node node = container.get(nodeCID, !fixing); // fix
			
			frontierCIDs.add(nodeCID);
			
			if(node.isInner()) {
				InnerNode innerNode = (InnerNode) node;
				if(innerNode.hasSampleBuffer()) { // inner node with attached sample buffer
					// FIXME!!! we probably erronously yield samples from the sample buffers of canonical set nodes which aren't in the query
					// we filter not until we actually draw from the sample
					samplers.add(innerNode.samples.iterator()); 
					addWeight(innerNode.samples.size());
				} else { // inner node without attached sample buffer
					// QUE: this is in opposition to the paper evaluated eagerly (see algorithm 1, lines 7-11). Does this skew the probabilities?
					//		--> yes, probably yes.
					List<V> relevantSamples = innerNode.relevantValues(lo, hi);
					// FIXME: don't add empty relevantValues to list
					if(!relevantSamples.isEmpty()) {
						samplers.add(new InfiniteSampler<V>(relevantSamples, rng));
						addWeight(relevantSamples.size());
					}
				}
			} else {
				// this shouldn't be reached (at least for sufficiently large trees) as we extract all values from leaves in the InnerNodes without sample buffer
				System.out.println("Attention! Direct extraction from Leaf Node detected, is the tree really that small?");
				LeafNode leafNode = (LeafNode) node;
				List<V> relevantSamples = leafNode.relevantValues(lo, hi);
				samplers.add(new InfiniteSampler<V>(relevantSamples, rng));
				addWeight(relevantSamples.size());
			}			
		}
		
		private void removeFromFrontier(int idx) {
			container.unfix(frontierCIDs.remove(idx)); // unfix and remove from CID list // TODO: is this a problem if already unfixed?
			removeWeight(idx);
			samplers.remove(idx);			
		}
		
		private void addWeight(int w) {
			weights.add(w);
			if(accWeights.isEmpty()) {
				accWeights.add(w);
			} else {
				accWeights.add(accWeights.get(accWeights.size() - 1) + w);
			}			
			totalWeight += w;
		}
		
		/** Removes the weight at specified position from the weight list and recalculates affected values
		 * in the accumulated weights list.
		 */
		private void removeWeight(int idx) {
			int w = weights.remove(idx);
			totalWeight -= w;
			
			ListIterator<Integer> awIter = accWeights.listIterator(idx);
			awIter.next();
			awIter.remove();
			while(awIter.hasNext())				
				awIter.set(awIter.next() - w);
		}
	
		@Override
		public void close() {			
			// release all nodes which are still in frontier from the buffer
			for(P cid : frontierCIDs)
				container.unfix(cid);
			super.close();
		}
		
		/* We just need to precompute the value here, all the other logic is handled by AbstractCursor. */ 
		protected boolean hasNextObject() {		
			if(precomputed.isEmpty()) { // compute new values // single run here
				//-- determine Iterator (== Node) to draw from
				int r = rng.nextInt(totalWeight);
				int idx = HUtil.binFindES(accWeights, r);
				if(!samplers.get(idx).hasNext()) { // replace node with children and draw _only_ from them
					// should only happen for InnerNodes (those with sample buffers, which are finite).
					InnerNode innerNode = (InnerNode) container.get(frontierCIDs.get(idx));
					
					removeFromFrontier(idx);
					List<P> relChildCIDs = innerNode.relevantChildCIDs(lo, hi);
					LazySamplingCursor subCursor = new LazySamplingCursor(lo, hi, relChildCIDs, fixing);
					precomputed.addAll(subCursor.nextN_internal(1)); // TODO: permute <precomputed> if we go over to batched sampling 
					join(subCursor);
				} else {
					precomputed.add(samplers.get(idx).next()); // FIXME: we need to check if it lies in the query first
				}
			}
			
			return true;
		}

		public List<V> tryToSample(int n) {
			List<Integer> weights = sizedIterators.stream().map(x -> x.size()).collect(Collectors.toList());
			List<Integer> ds = Randoms.multinomialDist(weights, n, rng);
			for(int i=0; i < ds.size(); i++) {
				
			}
		}
		
		@Override
		protected V nextObject() {
			return precomputed.remove();
		}
		
		/** Batched iteration. */
		public List<V> nextN_internal(int n) {
			// OPT: actually _do_ some optimization. xD
			List<V> sampled = new LinkedList<V>();
			for(int i=0; i < n; i++)
				sampled.add(this.next());
			return sampled;
		}
		
		/** Joins a previously generated subcursor back into the parent. The other cursor is then invalid.
		 * TODO: Ugh, but we must not close() it, as this would unfix the pages.
		 */
		private void join(LazySamplingCursor other) {
			frontierCIDs.addAll(other.frontierCIDs);
			samplers.addAll(other.samplers);
			for(int w : other.weights)
				addWeight(w);
		}
		
	}
	
	private class LazySamplingCursor_v2 extends AbstractCursor<V> {

		public class LazyInnerSampler<V,P> implements StatefulSampler<V, P> {
			InnerNode node;
			
			@Override
			public int size() {
				return node.totalWeight();
			}

			@Override
			public List<V> tryToSample(int n) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public InnerNode getNode() {		
				return node;
			}

			boolean exhausted() {
				return false;
			}

			
		}
		
		
		
		public LazySamplingCursor_v2(List<P> iniCIDs) {
			
		}
		
		
		
		
		public List<V> tryToSample(int n) {
			
		}
		
		
		@Override
		protected boolean hasNextObject() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		protected V nextObject() {
			// TODO Auto-generated method stub
			return null;
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
	
}
