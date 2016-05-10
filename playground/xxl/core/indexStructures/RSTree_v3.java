package xxl.core.indexStructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
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
import xxl.core.util.Randoms;
import xxl.core.util.Sample;

public class RSTree_v3<K extends Comparable<K>, V, P> implements TestableMap<K, V, P> {
	/** Implementation of the RS-Tree for 1-dimensional data.
	 * 
	 * Skeleton of WBTree used, as the RSTree also needs information about the weight of the nodes.
	 * 
	 * Q: perhaps roll InsertionInfo back to SplitInfo and always do a lookup before a modificating operation
	 * 		path to node should then be buffered and therefore not incur any additional IOs.
	 * 		-> nope, keep it that way, since it might be beneficial for bacthed operations in expansions,
	 * 			and removals of multiple values wouldn't benefit from it.
	 * 
	 *   DONE Mini-Milestone 1: Implement sample buffer maintenance for insertions.
	 *   TODO Mini-Milestone 1.5: Implement QueryCursor for range queries.
	 *   TODO Mini-Milestone 2: Implement lazy sampling query cursor
	 *   
	 * @param K type of the keys
	 * @param V type of the actual data
	 * @param P type of the ContainerIDs (= CIDs)
	 */

	/** How many samples per node should be kept = parameter s. 
	 * The buffers must have between s/2 and 2*s items at all times. */
	final int samplesPerNode;
	final int samplesPerNodeLo; // std: = samplesPerNode / 2
	final int samplesPerNodeHi; // std: = samplesPerNode * 2
	final int samplesPerNodeReplenishTarget; // how full shall the buffer be made if we have to replenish it?
	
	/** RNG used for drawing samples and such. */
	Random rng;
	
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
//	int rootWeight; // for now don't track this information

	/** --- Constructors & Initialisation ---
	- All mandatory arguments are put into the constructor.
	- The container gets initialized during a later call to <tt>initialize</tt> as we 
		implement the <tt>NodeConverter</tt> functionality once again (like in xxl) as inner class of this tree class.
	*/
	public RSTree_v3(int samplesPerNode, int branchingParam, int leafLo, int leafHi, Function<V, K> getKey) {
		super();
		this.samplesPerNode = samplesPerNode;
		this.branchingParam = branchingParam;		
		this.getKey = getKey;
		
		this.leafLo = leafLo;
		this.leafHi = leafHi;
		
		//- setting defaults
		this.branchingLo = branchingParam / 2;
		this.branchingHi = branchingParam * 2;
		this.samplesPerNodeLo = samplesPerNode / 2;
		this.samplesPerNodeHi = samplesPerNode * 2;
		this.samplesPerNodeReplenishTarget = samplesPerNode;		
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

	/**
	 * Generalization of SplitInfo class which is used to report the result of an
	 * operation in a subtree. This can either be the information that and how the child has split (~ SplitInfo),
	 * or the count of the entries that got changed (think of removals or insertions in duplicate free trees) which
	 * is needed to maintain aggregate meta-information. 
	 */
	class InsertionInfo {
		boolean isSplit = false;
		int weightNew = 0;
		// TODO: for single insertions we don't need the new weight, but only the info if it was
//		boolean insertionSuccessful = true;  
		
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
		
		public boolean isInner() {
			return !isLeaf();
		}
		
		public abstract InsertionInfo insert(V value, P thisCID, int level);

		public abstract List<V> drainSamples(int amount);

		public abstract int totalWeight();

//		public abstract List<V> buildSampleBuffer(int d);
	}
	
	public class InnerNode extends Node {		
		protected List<K> separators;
		protected List<P> pagePointers;
		protected List<Integer> childWeights;
		
		/** The list of samples kept in this node. */
		List<V> samples;
		
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
			return samples.size() < samplesPerNode / 2;			
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
			// OPT somehow prevent recalculation every time.
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
				// .. this can only be done after insertion on leaf level as only then it's clear how the weight was affected.
				childWeights.set(pos, childInsertInfo.weightNew);
			}
			
			// container.unfix(nextCID); // release lock on the childs memory			
			
			//- maintain sample buffer (which acts similiar to a reservoir sample on the passing values)
			int curWeight = totalWeight();
			if(hasSampleBuffer() && curWeight - oldWeight > 0) { // insertion actually took place 
				/* Replace every item currently present in the sample buffer with probability 1 / curWeight 
				 * with the newly inserted item.
				 * This is like it is described in the paper, but perhaps we can improve on this? // QUE
				 */
				double p = 1.0 / (double)curWeight;
				for (int i = 0; i < samples.size(); i++)
					if(rng.nextDouble() < p)
						samples.set(i, value);
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
			int splitPos = pagePointers.size() / 2; // as we now work with B-Tree splitting just split in the middle.
			
			InnerNode newode = new InnerNode();
			
			//- split separators
			// separators[splitPos] becomes the separator between the offspring 
			newode.separators = HUtil.splitOffRight(separators, splitPos, new ArrayList<K>());
			K offspringSeparator = separators.remove(splitPos-1);
			
			//- split pointers and weights
			newode.pagePointers = HUtil.splitOffRight(pagePointers, splitPos, new ArrayList<P>());			
			newode.childWeights = HUtil.splitOffRight(childWeights, splitPos, new ArrayList<Integer>());
			
			//-- recalculate resulting weights again
			int weightLeft = this.childWeights.stream().reduce(0, (x,y) -> x+y);
			int weightRight = newode.childWeights.stream().reduce(0, (x,y) -> x+y);
			
			//-- distribute samples among the resulting nodes
			// this is more complicated now as we have to distinguish whether the resulting nodes still have buffers attached
			if(this.samples != null) {
				List<V> samplesLeft = new LinkedList<V>();
				List<V> samplesRight = new LinkedList<V>();
				for(V sample : samples) {
					if(getKey.apply(sample).compareTo(offspringSeparator) >= 0)
						samplesRight.add(sample);
					else
						samplesLeft.add(sample);
				}
				
				//- attach filtered samples to new nodes if they should have an attached sample buffer
				// OPT: could we do something instead of trashing the samples when they aren't needed?
				if(weightLeft > 2*samplesPerNode) {
					this.samples = samplesLeft;
					this.repairSampleBuffer();
				} else
					this.samples = null;
				
				if(weightRight > 2*samplesPerNode) {
					newode.samples = samplesRight;
					newode.repairSampleBuffer();
				} else
					newode.samples = null;
			}
			
			//- put new node into Container
			P newodeCID = container.insert(newode);
			
			return new InsertionInfo(newodeCID, offspringSeparator, weightLeft, weightRight);			
		}
		
		
		
		
		/** "BuildSamples" from the paper. This is only used for batch filling the sample buffers (?!). 
		 * Therefore it would only be useful if the base tree woudln't be built incrementally but is batch loaded
		 * without maintaining the buffers.
		 *  
		 * Is NOT: Replenishing of a sample buffer by draining from the childs' buffers. Might invoke recursive replenishing.
		 * 
		 * Ok, so what are we supposed to do here exactly?
		 * --> Q: What number of samples should we aim for? 
		 * 			avg = s, min = s / 2 or max = s * 2
		 *		  Or should we make it dependent on how many "excess" samples are readily available in the child nodes?
		 *		--> Algorithm description 
		 * --> Q: Draw how much from which child?
		 * 			Imo, we can only draw accordingly to the subtree sizes of the childs to get a correct sample.
		 * 			We must do this probabilistic to get a correct sample for the subtree.
		 * 
		 * @param d amount of excess samples we want to generate for parent nodes.
		 */
//		public List<V> buildSampleBuffer(int toYield) {
//			int toDraw = toYield;
//			if(samples.size() < samplesPerNode) { // buffer is so empty we want to fill it too
//				toDraw += 2*samplesPerNode - samples.size(); 
//			}
//			
//			//-- determining how much samples we need from each child
//			ArrayList<Integer> nSamplesPerChild = Randoms.multinomialDist(childWeights, d, rng);
//			
//			//-- fetch samples
//			LinkedList<V> newSamples = new LinkedList<V>();
//			for (int i = 0; i < pagePointers.size(); i++) {
//				Node child = container.get(pagePointers.get(i));
//				List<V> gotSamples = child.buildSampleBuffer(nSamplesPerChild.get(i));
//			}
//			
//			//-- yield some and save some in buffer
//			// this amounts to a WoR sample of all the available samples here
//			// TODO
//			return excessSamples;
//		}
		
		
		/**
		 * Checks for a underflow in the sample buffer and repairs it.
		 * Repairing for InnerNodes is done by draining samples from the child nodes.
		 */
		public void repairSampleBuffer() {
			if(sampleUnderflow()) {				
				int toDraw = samplesPerNodeReplenishTarget - samples.size();
				samples.addAll(fetchSamplesFromChildren(toDraw));
			}
		}
		
		@Override
		public List<V> drainSamples(int amount) {
			if(samples.size() - amount < samplesPerNodeLo) { // we have to refill, this includes the case where amount > samples.size()
				int toRedraw = amount + samplesPerNodeReplenishTarget - samples.size();
				samples.addAll(fetchSamplesFromChildren(toRedraw));				
			}
			List<V> toYield = Sample.worRemove(samples, amount, rng);
			return toYield;
		}
		
		public List<V> fetchSamplesFromChildren(int amount) {
			List<V> fetched = new LinkedList<V>();
			//-- determining how much samples we need from each child
			ArrayList<Integer> nSamplesPerChild = Randoms.multinomialDist(childWeights, amount, rng);
			
			//-- fetch samples
			LinkedList<V> newSamples = new LinkedList<V>();
			for (int i = 0; i < pagePointers.size(); i++) {
				Node child = container.get(pagePointers.get(i));
				List<V> fetchedFromChild = child.drainSamples(nSamplesPerChild.get(i));
				// .. and put them in sample buffer
				fetched.addAll(fetchedFromChild); // OPT perhaps do random permutation here
			}
			
			return fetched;
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
			int insertPos = HUtil.binFindES(new MappedList<V,K>(values, FunctionsJ8.toOldFunction(getKey)), key);
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
			// K separator = getKey.apply(newode.values.get(0)); // is this responsible for not finding all inserted values?
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

		/** Draws a WR-sample (with replacement (!)) from the underlying values. */
		public List<V> drainSamples(int amount) {
			return Sample.wrKeep(values, amount, rng);
		}

		public int totalWeight() {
			return values.size();
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
	// FIXME: serialize sample buffers, too.
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
			root.values = new ArrayList<V>();
			root.values.add(value);
//			rootWeight = 1;
			rootHeight = 0;
			rootCID = container.insert(root);
			return;
		} else {
			InsertionInfo insertionInfo = container.get(rootCID).insert(value, rootCID, rootHeight);			
			
			if(insertionInfo.isSplit) { // new root
				InnerNode newroot = new InnerNode();
				
				newroot.separators = new ArrayList<K>();
				newroot.separators.add(insertionInfo.separator);
				
				newroot.pagePointers = new ArrayList<P>();
				newroot.pagePointers.add(rootCID);
				newroot.pagePointers.add(insertionInfo.newnodeCID);
				
				newroot.childWeights = new ArrayList<Integer>();
				newroot.childWeights.add(insertionInfo.weightLeft);
				newroot.childWeights.add(insertionInfo.weightRight);
				
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
