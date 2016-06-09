package xxl.core.indexStructures;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.function.Function;

import xxl.core.collections.MappedList;
import xxl.core.collections.containers.CastingContainer;
import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.TypeSafeContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.cursors.AbstractCursor;
import xxl.core.functions.FunJ8;
import xxl.core.indexStructures.RSTree1D.InsertionInfo;
import xxl.core.indexStructures.RSTree1D.LeafNode;
import xxl.core.indexStructures.RSTree1D.ReallyLazySamplingCursor;
import xxl.core.indexStructures.WRSTree1D.ReallyLazySamplingCursor.InnerSampler;
import xxl.core.indexStructures.WRSTree1D.ReallyLazySamplingCursor.ProtoSampler;
import xxl.core.indexStructures.WRSTree1D.ReallyLazySamplingCursor.Sampler;
import xxl.core.indexStructures.WRSTree1D.ReallyLazySamplingCursor.SamplingResult;
import xxl.core.indexStructures.WRSTree1D.ReallyLazySamplingCursor.UnbufferedSampler;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.profiling.ProfilingCursor;
import xxl.core.util.CopyableRandom;
import xxl.core.util.HUtil;
import xxl.core.util.Interval;
import xxl.core.util.Pair;
import xxl.core.util.Randoms;
import xxl.core.util.Sample;
import xxl.core.util.Triple;

public class WRSTree1D<K extends Comparable<K>, V, P> implements SamplableMap<K, V> {
	/** First Implementation of the weight balanced RS-Tree for 1-dimensional data. */

	/** How many samples per node should be kept = parameter s. 
	 * The buffers must have between s/2 and 2*s items at all times. */
//		final int samplesPerNode;
	final int samplesPerNodeLo; // std: = samplesPerNode / 2
	final int samplesPerNodeHi; // std: = samplesPerNode * 2
	final int samplesPerNodeReplenishTarget; // how full shall the buffer be made if we have to replenish it? // std = samplesPerNodeHi
	
	/** RNG used for drawing samples and such. Use {@link #setRNG} to set it. */
	CopyableRandom rng;
	
	/** The branching parameter <b>tA</b> == the fanout.			<br> 
	 * Following inequalities hold:									<br>
	 *																<br>
	 * Weight of an inner node N =: w(N) on level l:				<br>
	 * 		1/2 * tK * (tA ** l) < w(N) < 2 * tK * (tA ** l)		<br>
	 *																<br>
	 * Number of entries in an inner node N =: e(N) on level l: 	<br>
	 * 		1/4 * tA < e(N) < 4 * tA 
	 */
	final int branchingParam;
	final int branchingLoBound, branchingHiBound;	
	
	/** The leaf parameter <b>tK</b>, determining the amount of entries a leaf can contain.	<br>
	 * Following inequalities hold:									<br>
	 * 																<br>
	 * Number of entries in a leaf L =: e(L): 						<br>
	 * 		tK <= e(L) < 2*tK 										<br>
	 */
	final int leafParam;
	final int leafLo, leafHi; // [inclusive, exclusive[	
	
	/** Ubiquitious getKey function which maps from values (V) to keys (K). */
	public Function<V, K> getKey;

	/** Container of the tree (and everything). 
	 * This is a ConvertableContainer which spits out nodes. */
	public TypeSafeContainer<P, Node> container;

	/** The NodeConverter incorporated in the container.
	 * Used for saving/loading of the tree. */
	NodeConverter nodeConverter;
	
	/** ContainerID of the root. */
	P rootCID;

	/** Remember how high the tree is... */
	int rootHeight;

	/** Domain of the keys. */
	Interval<K> universe;
	
	/** --- Constructors & Initialisation ---
	- All mandatory arguments are put into the constructor.
	- The container gets initialized during a later call to <tt>initialize</tt> as we 
		implement the <tt>NodeConverter</tt> functionality once again (like in XXL) as inner class of this tree class.
	*/
	public WRSTree1D(int branchingParam, int leafParam, int samplesPerNodeLo, int samplesPerNodeHi, Interval<K> universe, Function<V, K> getKey) {
		this.universe = universe;
		this.samplesPerNodeLo = samplesPerNodeLo;
		this.samplesPerNodeHi = samplesPerNodeHi;
		this.branchingParam = branchingParam;
		this.leafParam = leafParam;
		this.getKey = getKey;
		
		//- old fixed values from RSTree: no changes on leaf insertion logic needed with those
		this.leafLo = leafParam / 2;
		this.leafHi = leafParam * 2 - 1; // RSTree defines upper bound inclusive, WBTree not. "-1" to keep it compatible.
		
		//- unused
		this.branchingLoBound = branchingParam / 4 + 1;
		this.branchingHiBound = branchingParam * 4 - 1;
		
		//- defaults
		this.samplesPerNodeReplenishTarget = this.samplesPerNodeHi;
		this.rng = new CopyableRandom();
	}

	/** Loads a whole RSTree - that is in addition to the data, the correct parameters (branching factor, etc.)
	 * 		of the tree from a metadata file. Just some things have to be given which can't be serialized or
	 * 		determine the types.  
	 * @param metaDataFilename The absolute path to the metaDataFileName. 
	 * @param containerFactory Function which builds a container from an absolute filename. 
	 * 		Allows exchanging of different wrapping containers in between.
	 * 		Default use: containerFactory = BlockFileContainer::new 
	 * @throws IOException
	 */
//	protected static <K extends Comparable<K>, V, P> WRSTree_copyImpl<K, V, P> loadFromMetaData(
//			String metaDataFilename, 
//			Function<String, Container> containerFactory,  
//			Converter<K> keyConverter, 
//			Converter<V> valueConverter,
//			Function<V,K> getKey) throws IOException {
//		//-- open the metaData-file
//		if(!new File(metaDataFilename).exists())
//			throw new FileNotFoundException("Metadata not found at: \""+ metaDataFilename +"\".");
//		FileInputStream metaDataFileStream = new FileInputStream(metaDataFilename);
//		DataInput metaData = new DataInputStream(metaDataFileStream);
//		
//		//-- load the raw container
//		String dataFileName = metaData.readUTF();
//		// there is no file without suffix, so dont check this here
//		/* if(!new File(dataFileName).exists()) {
//			metaDataFileStream.close();
//			throw new FileNotFoundException("Container files couldn't be loaded from: \""+ dataFileName +"\".");
//		} */
//			
//		Container rawContainer = containerFactory.apply(dataFileName);
//				
//		//-- read the constructor/topological parameters
//		Interval<K> universe = Interval.getConverter(keyConverter).read(metaData);
//		int samplesPerNodeLo = metaData.readInt();
//		int samplesPerNodeHi = metaData.readInt();
//		int branchingLo = metaData.readInt();
//		int branchingHi = metaData.readInt();
//		int leafLo = metaData.readInt();
//		int leafHi = metaData.readInt();
//		
//		//-- construct and initialize the tree
//		WRSTree_copyImpl<K, V, P> instance = new WRSTree_copyImpl<K, V, P>(universe, samplesPerNodeLo, samplesPerNodeHi, branchingLo, branchingHi, leafLo, leafHi, getKey);
//		instance.initialize_buildContainer(rawContainer, keyConverter, valueConverter);
//		
//		//- read state parameters
//		P rootCID = (P) rawContainer.objectIdConverter().read(metaData);
//		int rootHeight = metaData.readInt();
//		CopyableRandom rng = new CopyableRandom(); new ConvertableConverter().read(metaData, rng);
//		
//		//- .. and force them into the instance
//		instance.rootCID = rootCID;
//		instance.rootHeight = rootHeight;
//		instance.setRNG(rng);
//		
//		//-- finish
//		metaDataFileStream.close();
//		return instance;
//	}
//	
//	/** Saves a tree to a metadata file. 
//	 * NOTE: The tree is not usable afterwards anymore as we have to explicitly call the close() method of the container
//	 * to make it write its metadata file.
//	 * 
//	 * @param metaDataFilename
//	 * @param dataFileName abolute path to the container backing this tree.
//	 * @throws IOException 
//	 */
//	protected void writeToMetaData(
//			String metaDataFilename, 
//			String dataFileName,
//			Converter<K> keyConverter, 
//			Converter<V> valueConverter
//			) throws IOException {
//		//-- open the metaData file
//		if(new File(metaDataFilename).exists()) {
//			System.out.println("Warning: metadata file \""+ metaDataFilename +"\" already exists.");
//			System.out.println("Deleting old metadata file.");
//			// throw new FileAlreadyExistsException(metaDataFilename);
//			new File(metaDataFilename).delete();
//		}		
//		FileOutputStream metaDataFileStream = new FileOutputStream(metaDataFilename, false);
//		DataOutput metaData = new DataOutputStream(metaDataFileStream);
//		
//		//- write the container file prefix
//		// there is no file without suffix, so dont check this here
//		/* if(!new File(dataFileName).exists()) {
//			metaDataFileStream.close();
//			throw new FileNotFoundException("No container files found at: \""+ dataFileName +"\".");
//		} */ 
//
//		container.close(); // CHECK: to get the container to write its metadata we need to close it :/
//		metaData.writeUTF(dataFileName);
//
//		//-- write the constructor/topological parameters
//		Interval.getConverter(keyConverter).write(metaData, universe);
//		metaData.writeInt(samplesPerNodeLo);
//		metaData.writeInt(samplesPerNodeHi);
//		metaData.writeInt(branchingLo);
//		metaData.writeInt(branchingHi);
//		metaData.writeInt(leafLo);
//		metaData.writeInt(leafHi);
//		
//		//-- write state parameters
//		container.objectIdConverter().write(metaData, rootCID);
//		metaData.writeInt(rootHeight);
//		new ConvertableConverter<CopyableRandom>().write(metaData, rng);
//
//		metaDataFileStream.close();
//	}
	
	
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

	public void initialize_withReadyContainer(TypeSafeContainer<P, Node> container) {
		this.container = container;
	}

	/** For repeatable results in testing. */
	public void setRNG(CopyableRandom rng) {
		this.rng = rng;
	}
	
	/**
	 * Insertion. 
	 */
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
			
			if(insertionInfo.isSplit) { // new root - actually create a new root "manually"
				InnerNode newroot = new InnerNode();
				
				newroot.ranges = new ArrayList<Interval<K>>(branchingHiBound);
				//- calculate new ranges
				Pair<Interval<K>, Interval<K>> newRanges = universe.split(insertionInfo.separator, true);				
				newroot.ranges.add(newRanges.getElement1());
				newroot.ranges.add(newRanges.getElement2());
				//- adjust the rest
				newroot.pagePointers = new ArrayList<P>(branchingHiBound);
				newroot.pagePointers.add(rootCID);
				newroot.pagePointers.add(insertionInfo.newnodeCID);
				
				newroot.childWeights = new ArrayList<Integer>(branchingHiBound);
				newroot.childWeights.add(insertionInfo.weightLeft);
				newroot.childWeights.add(insertionInfo.weightRight);
				
				// fill the root node with samples again
				if(newroot.totalWeight() > samplesPerNodeHi) {
					newroot.samples = new LinkedList<V>();
					newroot.repairSampleBuffer();
				}
				
				rootCID = container.insert(newroot);
				rootHeight++;
			}
			
		}		
	}

	/**
	 * Lookup.
	 */
	// TODO: problems when there are more than leafHi duplicates of one key k. These could get saved in a leaf with range ]k,k] = 0
	//			which would never get sampled.
	public List<V> get(K key) {
		if(rootCID == null) return new LinkedList<V>(); // tree empty
		
		int level = rootHeight;
		P nodeCID = rootCID;
		
		while(level > 0) {
			// nodeCID = ((InnerNode) container.get(nodeCID)).chooseSubtrees(key);
			// restrict us to one path for now
			nodeCID = ((InnerNode) container.get(nodeCID)).chooseFirstSubtreeCID(key);
			level--;
		}		
		LeafNode lnode = (LeafNode) container.get(nodeCID);
		
		return HUtil.getAll(lnode.lookupIdxs(key), lnode.values);
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
	 * Generalization of SplitInfo class which is used to report the result of an
	 * operation in a subtree. This can either be the information that and how the child has split (~ SplitInfo),
	 * or the count of the entries that got changed (think of removals, or insertions in duplicate free trees) which
	 * is needed to maintain aggregate meta-information. 
	 */
	class InsertionInfo {
		boolean isSplit;
		int weightNew = 0;
		// OPT: for single insertions we don't need the new weight, but only the info if it was
		// 			boolean insertionSuccessful = true;  
		
		P newnodeCID = null;
//			Interval<K> rangeLeft = null, rangeRight = null;
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

		public abstract List<V> allValues();
	}
	
	public class InnerNode extends Node {		
		public List<Interval<K>> ranges;
		public List<P> pagePointers;
		public List<Integer> childWeights;
		
		/** The list of samples kept in this node. */
		public LinkedList<V> samples;
		
		public boolean isLeaf() { return false; }
		
		/** A node should have a sample buffer attached if its weight is greater than 2*s,
		 * but what's exactly the rationale behind this? Doesn't the other parameters like leafCapacity
		 * and branchingParam also be taken into account?
		 */
		public boolean hasSampleBuffer() { return samples != null; }

		public boolean sampleUnderflow() {
			return samples.size() < samplesPerNodeLo;			
		}

		public Integer chooseFirstSubtreeIdx_Interval(Interval<K> query) {
			for(int i=0; i < ranges.size(); i++)
				if(ranges.get(i).intersects(query))
					return i;
			return null;
		}
		
		public P chooseFirstSubtreeCID(K key) {
			return pagePointers.get(chooseFirstSubtreeIdx_Interval(new Interval<K>(key)));
		}

		@Override
		public int totalWeight() {
			// OPT: somehow prevent recalculation every time.
			return childWeights.stream().reduce(0, (x,y) -> x+y);			
		}

		/** Abstract the determination of choosing which child node to insert a record into. */
		protected Integer findInsertionPos(K key) {
			// return the index of the first range which could contain the key 
			for(int i=0; i < ranges.size(); i++)
				if(ranges.get(i).contains(key))
					return i;
			return null;
		}
		
		public InsertionInfo insert(V value, P thisCID, int levelUnused) {
			K key = getKey.apply(value);
			
			//- insert in sublevel
			int pos = findInsertionPos(key);
			P nextCID = pagePointers.get(pos);
			Node nextNode = container.get(nextCID);
			int oldWeight = totalWeight(); 
					
			InsertionInfo childInsertInfo = nextNode.insert(value, nextCID, levelUnused-1); //== RECURSION ==
			
			if(childInsertInfo.isSplit) { // a split occured in child and we need to update the directory
				// calculate the new ranges
				Interval<K> oldRange = ranges.get(pos);
				Interval<K> rangeLeft = new Interval<K>(oldRange.lo, oldRange.loIn, childInsertInfo.separator, true); // CHECK
				Interval<K> rangeRight = new Interval<K>(childInsertInfo.separator, false, oldRange.hi, oldRange.hiIn);
				ranges.set(pos, rangeLeft);
				ranges.add(pos+1, rangeRight);
				
				// adjust the rest
				pagePointers.add(pos+1, childInsertInfo.newnodeCID);
				childWeights.set(pos  , childInsertInfo.weightLeft);
				childWeights.add(pos+1, childInsertInfo.weightRight);
			} else { // update weight of child
				// .. this can only be done after insertion on leaf level, as only then it's clear how the weight was affected.
				childWeights.set(pos, childInsertInfo.weightNew);
			}
			
			//- maintain sample buffer (which acts similiar to a reservoir sample on the passing values)
			int curWeight = totalWeight();
			boolean insertionTookPlace = curWeight - oldWeight > 0;
			if(insertionTookPlace) {
				if(hasSampleBuffer()) {  
					/* Replace every item currently present in the sample buffer with probability 1 / curWeight 
					 * with the newly inserted item.
					 * QUE: This is like it is described in the paper, but perhaps we can improve on this? */ 
					double p = 1.0 / (double)curWeight;
					for(ListIterator<V> sampleIter = samples.listIterator(); sampleIter.hasNext(); ) {
						sampleIter.next();
						if(rng.nextDouble() < p)
							sampleIter.set(value);
					}
				} else if(curWeight > samplesPerNodeHi) { // we currently have no sample buffer but we should have one! 
					samples = new LinkedList<V>();
					repairSampleBuffer();
				}
			}
			
			//- check for split here
			InsertionInfo insertionInfo = null;
			if(weightOverflow(curWeight, levelUnused))
				insertionInfo = split(levelUnused);
			else
				insertionInfo = new InsertionInfo(totalWeight());
			
			container.update(thisCID, this);
			
			return insertionInfo;
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

		public InsertionInfo split(int level) {			
			//- determine split position
			//-- assume that the entry ranges are ordered and return the first split which satisfies the weight constraints
			int targetWeight = HUtil.intPow(branchingParam, level) * leafParam;
			Triple<Integer, Integer, Integer> splitLocalization = determineSplitposition(targetWeight);
			
			int splitPos = splitLocalization.getElement1() + 1;
			int calcedWeightLeft = splitLocalization.getElement2(); 
			int calcedWeightRight = splitLocalization.getElement3();
			
			InnerNode newode = new InnerNode();
			
			//- split separators
			// separators[splitPos] becomes the separator between the offspring 
			newode.ranges = HUtil.splitOffRight(ranges, splitPos, new ArrayList<Interval<K>>()); // CHECK LinkedList seems to make more sense here
			Interval<K> rangeLeft = ranges.stream().reduce(Interval<K>::union).get();
			Interval<K> rangeRight = newode.ranges.stream().reduce(Interval<K>::union).get();
			
			//- split pointers and weights
			newode.pagePointers = HUtil.splitOffRight(pagePointers, splitPos, new ArrayList<P>());			
			newode.childWeights = HUtil.splitOffRight(childWeights, splitPos, new ArrayList<Integer>());
			
			//-- recalculate resulting weights again
			int weightLeft = this.childWeights.stream().reduce(0, (x,y) -> x+y);
			int weightRight = newode.childWeights.stream().reduce(0, (x,y) -> x+y);
			
			// .. and check again calculated weights
			assert weightLeft == calcedWeightLeft;
			assert weightRight == calcedWeightRight;
			
			//-- distribute samples among the resulting nodes
			// we have to distinguish whether the resulting nodes still have buffers attached
			if(this.samples != null) {
				LinkedList<V> samplesLeft = new LinkedList<V>();
				LinkedList<V> samplesRight = new LinkedList<V>();
				
				for(V sample : samples) {
					K key = getKey.apply(sample);
					assert !(rangeLeft.contains(key) && rangeRight.contains(key)) : "Unexpected inclusion of sample in both parts of a split.";
					assert rangeLeft.contains(key) || rangeRight.contains(key) : "Sample can't be fitted into childs of split anymore.";
					if(rangeLeft.contains(key))
						samplesLeft.add(sample);
					else
						samplesRight.add(sample);
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
			
			// for now still return only the separator to be compliant with LeafNodes which can't determine their own boundaries.
			assert rangeLeft.hiIn;
			return new InsertionInfo(newodeCID, rangeLeft.hi, weightLeft, weightRight);
		}
		
		/** Returns all values in the subtree originating from this node. */
		public List<V> allValues() {
			LinkedList<V> allVals = new LinkedList<V>();
			for(P childCID : pagePointers)
				allVals.addAll(container.get(childCID).allValues());
			return allVals;
		}
		
		/**
		 * Checks for a underflow in the sample buffer and repairs it.
		 * Repairing for InnerNodes is done by draining samples from the child nodes.
		 * OPT: only called from xxl.core.indexStructures.WRSTree_copyImpl.InnerNode.split() -> inline?
		 */
		protected void repairSampleBuffer() {
			if(sampleUnderflow()) {				
				int toDraw = samplesPerNodeReplenishTarget - samples.size();
				refillSamplesFromChildren(toDraw); 
			}
		}
		
		@Override
		protected List<V> drainSamples(int amount) {
			if(hasSampleBuffer()) {
				if(samples.size() - amount < samplesPerNodeLo) { // we have to refill, this includes the case where amount > samples.size()
					int toRedraw = amount + samplesPerNodeReplenishTarget - samples.size();
					refillSamplesFromChildren(toRedraw);				
				}
				
				// as refillSamplesFromChildren now does the permutation we can return the first elements
				List<V> toYield = HUtil.splitOffLeft(samples, amount, new LinkedList<V>());
				return toYield;
			} else { // just sample from the values in the subtree if we are unbuffered.
				// CHECK: does this result in the right probabilities?
				return Sample.wrKeep(allValues(), amount, rng);
			}
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
		public List<V> values;

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
		 * Or at least tries to do a split as close to the middle as possible, cause duplicate keys might get in the way. 
		 */
		public InsertionInfo split() {
			//- find good splitting position, this is more complicated as expected because of duplicates.
			int targetPos = values.size() / 2;
			K separator = getKey.apply(values.get(targetPos));
			
			int sepLeftPos = targetPos;
			while(sepLeftPos > 1 && separator.compareTo(getKey.apply(values.get(sepLeftPos-1))) == 0)
				sepLeftPos--;			
			int sepRightPos = targetPos;
			while(sepRightPos < values.size() && separator.compareTo(getKey.apply(values.get(sepRightPos))) == 0)
				sepRightPos++;
			
			int separatorPos;
			if(targetPos - sepLeftPos <= sepRightPos - targetPos)
				separatorPos = sepLeftPos;
			else
				separatorPos = sepRightPos;
			
			//- build new node
			LeafNode newode = new LeafNode();
			
			int remLeft = separatorPos, remRight = values.size() - remLeft;
			newode.values = HUtil.splitOffRight(values, remLeft, new ArrayList<V>());
			K usedSeparator = getKey.apply(values.get(values.size()-1));
			
			//- put new node into Container
			P newodeCID = container.insert(newode);
			
			return new InsertionInfo(newodeCID, separator, remLeft, remRight);
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

		/** Draws a WR-sample (with replacement (!)) from the underlying values. */
		protected List<V> drainSamples(int amount) {
			return Sample.wrKeep(values, amount, rng);
		}

		public int totalWeight() {
			return values.size();
		}

		/** Returns all values in the subtree originating from this node. */
		public List<V> allValues() {
			return values;
		}
	}
	
	
	/**
	 * Converter for the nodes of a RS-tree. <br>
	 * Altough it is implemented as nested class it does not depend <b>directly</b> on any instance variables.
	 * (Only the created <tt>Node</tt> instances have an - again indirect - depency on the enclosing instance.)
	 * Instead it encapsulates all needed converters and hides them from the tree class (as the tree actually has 
	 * no use for calling them directly.<br>
	 * If one wants finer control over the constructed <tt>ConverterContainer</tt>, this class can be instantiated
	 * by <tt>RSTree1D<K,V,P>.NodeConverter nodeConverter = tree.new NodeConverter(...)</tt>. 
	 * 
	 * @see RSTree1D#initialize_withReadyContainer(TypeSafeContainer)
	 */
	@SuppressWarnings("serial")
	public class NodeConverter extends Converter<Node> {
	
		Converter<K> keyConverter;
		Converter<V> valueConverter;
		Converter<P> cidConverter;
		Converter<Interval<K>> rangeConverter;
		
		public NodeConverter(Converter<K> keyConverter, Converter<V> valueConverter, Converter<P> cidConverter) {
			super();
			this.keyConverter = keyConverter;
			this.valueConverter = valueConverter;
			this.cidConverter = cidConverter;
			this.rangeConverter = Interval.getConverter(keyConverter);
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
			node.ranges = new LinkedList<Interval<K>>();
			for(int i=0; i < nChildren; i++) { // only #childs-1 separators!
				node.ranges.add(rangeConverter.read(dataInput));
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
			for (Interval<K> range : node.ranges) {
				rangeConverter.write(dataOutput, range);
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

	/* TODO: we need different range queries for R-trees:
	 * 		- Q intersects R
	 * 		- Q contains R
	 * 		- Q is contained in R
	 */
	
//	/** Executes a range query of the interval [lo (inclusive), hi (exclusive)[ */
//	@Override
//	public ProfilingCursor<V> rangeQuery(K lo, K hi){
//		return new QueryCursor(new Interval<K>(lo, true, hi, false));
//	}

	/** Executes a range query of the given query interval, whose exact parameters can be specified. */
	@Override
	public ProfilingCursor<V> rangeQuery(Interval<K> query){
		return new QueryCursor(query);
	}


	/* TODO: we need different range queries for R-trees:
	 * 		- Q intersects R
	 * 		- Q contains R
	 * 		- Q is contained in R
	 */
	
	/** A query cursor for simple range queries.
	 * 
	 * We won't subclass xxl.core.indexStructures.QueryCursor here as it is 
	 * designed for queries over trees which inherit from xxl.core.indexStructures.Tree.
	 */
	public class QueryCursor extends AbstractCursor<V> implements ProfilingCursor<V> {
		/** Query interval. */
		final Interval<K> query;
		
		/** Profiling information: nodes touched. */
		Set<Pair<Integer, P>> p_nodesTouched;		
		/** The heights on which the query was started. Is always initialised to rootHeight and doesn't change.
		 * 		Needed for profiling, though. */
		int startlevel;
		
		/** Path of expanded nodeCIDs */
		Stack<P> sNodes; // container.get(sNodes.peek()) =: current node
		/** Path of chosen index in corresponding node in sNodes. */
		Stack<Integer> sIdx; // sIdx.peek() =: current index
		
		/** Single precomputed value. */
		V precomputed;
		
		private QueryCursor(Interval<K> query, P startNode, int startlevel) {
			super();
			//- query
			this.query = query;
			//- profiling
			p_nodesTouched = new HashSet<Pair<Integer,P>>();
			this.startlevel = startlevel;
			//- state
			sNodes = new Stack<P>();
			sNodes.push(startNode);
			sIdx = new Stack<Integer>();
			precomputed = null; // the next value to spit out
		}
		
		public QueryCursor(Interval<K> query) {
			this(query, rootCID, rootHeight);
		}
		
		@Override
		public Pair<Map<Integer,Integer>, Map<Integer, Integer>> getProfilingInformation() {
			// process the profiling information
			Map<Integer, Integer> touchedByLevel = new TreeMap<Integer, Integer>();
			for(Pair<Integer, P> nodeId : p_nodesTouched) {
				int l = nodeId.getElement1();
				touchedByLevel.putIfAbsent(l, 0);
				touchedByLevel.put(l, touchedByLevel.get(l)+1);
			}
			
			Map<Integer, Integer> prunedByLevel = new TreeMap<Integer, Integer>();			
			
			return new Pair<Map<Integer, Integer>, Map<Integer, Integer>>(touchedByLevel, prunedByLevel);
		}
	
		/** Marks the node currently on the head of the stack as touched for profiling. */
		private void markTouched() {
			p_nodesTouched.add(new Pair<Integer, P>(startlevel - sNodes.size() + 1, sNodes.peek()));
		}
		
		/** Finds the path to the first entry and locks its nodes in the buffer of the container. */
		@Override
		public void open() {
			// get the current node and lock it in the buffer
			if(sNodes.peek() == null) return; // happens when tree is empty
			Node curNode = container.get(sNodes.peek(), false); // this should always be the root if we don't descend from a different node
			markTouched();
			
			while(curNode.isInner()) {
				InnerNode curINode = (InnerNode) curNode;  
				
				// find the index of the next childnode
				int nextPos = curINode.chooseFirstSubtreeIdx_Interval(query);
				sIdx.push(nextPos);
				
				// descend to next node
				P nextPID = curINode.pagePointers.get(nextPos);
				sNodes.push(nextPID);
				curNode = container.get(sNodes.peek(), false);
				markTouched();
			}
			
			// now our node is a leaf and we just need to find the starting position			
			LeafNode curLNode = (LeafNode) curNode;
			
			// find starting position
			List<K> mappedList = new MappedList<V,K>(curLNode.values, FunJ8.toOld(getKey));			
			int pos = HUtil.binFindSE(mappedList, query.lo); // CHECK!!
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
			markTouched();
			
			while(curNode.isInner()) {
				InnerNode curINode = (InnerNode) curNode;
				// set the index of the current node
				sIdx.push(0);
				
				P nextPID = curINode.pagePointers.get(sIdx.peek());
				sNodes.push(nextPID);
				curNode = container.get(sNodes.peek(), false);
				markTouched();
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
			if(sNodes.peek() == null) return false; // happens when tree is empty
			LeafNode curLNode = (LeafNode) container.get(sNodes.peek());
			sIdx.push(sIdx.pop() + 1); // = increment counter			
	
			if(sIdx.peek() >= curLNode.values.size()) { // we need to switch to the node which has the next values
				if(switchToNextNode())
					// fetch the updated leaf node again, that is the state change incured by switchToNextNode() 
					curLNode = (LeafNode) container.get(sNodes.peek());
				else  
					return false; // hit the right border of the index structure				
			}
			
			precomputed = curLNode.values.get(sIdx.peek());
			if(!query.contains(getKey.apply(precomputed))) {
				assert query.locate(getKey.apply(precomputed)) > 0;
				return false; // hit the high border
			}
			
			return true;
		}
	
		@Override
		protected V nextObject() {
			return precomputed;
		}
	
	}

	/** Executes a sampling range query of the interval [lo (inclusive), hi (inclusive)] */
	public ProfilingCursor<V> samplingRangeQuery(K lo, K hi, int samplingBatchSize){
		Interval<K> query = new Interval<K>(lo, hi);
		return new ReallyLazySamplingCursor(query, samplingBatchSize, rootCID, rootHeight, universe, container.get(rootCID).totalWeight());
	}

	/** The really lazy (and a bit desoriented) sampling cursor from the paper.
		 * It now allows to batch sampling tries. * 
		 */
		// TODO: load all nodes which will be pruned from disk. This needs to be optimized!
		/*		-> Workaround: InnerSamplers now only load their associated node on demand (when they actually
		 * 				get sampled) and not on construction time like before. */ 
		// TODO: counts of inspected nodes are not quite right.
		// TODO: UnbufferedSamplers only get counted as 1 node, although a whole subtree 
		//		might need to get loaded for their initialisation.
		public class ReallyLazySamplingCursor extends AbstractCursor<V> implements ProfilingCursor<V> {
			/** the query */
			Interval<K> query;
			/** batch size */
			int batchSize;
			/** Precomputed values for the cursor. */
			LinkedList<V> precomputed = new LinkedList<V>();
	
			/** Profiling information: nodes touched. */
			Set<Pair<Integer, P>> p_nodesTouched;		
			/** Profiling information: nodes excluded from search because of being disjoint with the query. */
			Set<Pair<Integer, P>> p_nodesPruned;
			
			/** List of Samplers of the nodes in frontier. 
			 * A Sampler encapsulates the state information that describes the process of sampling from a node. 
			 * It also saves the effective weight of a sampled node. */ 
			List<Sampler> samplers;
			
			
			/** Constructor for child cursors. (called from InnerSampler.tryToSample() */
			public ReallyLazySamplingCursor(Interval<K> query, int batchSize, 
					List<Sampler> samplers, Set<Pair<Integer, P>> p_nodesTouched, Set<Pair<Integer, P>> p_nodesPruned) {
				super();
				this.query = query;
				this.batchSize = batchSize;
				this.samplers = samplers;
				this.p_nodesTouched = p_nodesTouched;
				this.p_nodesPruned = p_nodesPruned;
			}
			
			/** Constructor for root sampler. Called from RSTree.samplingRangeQuery() */ 
			public ReallyLazySamplingCursor(Interval<K> query, int batchSize, P initialCID, int initialLevel, Interval<K> range, int initialWeight) {
				this(query, batchSize, 
						new LinkedList<Sampler>(), 			// samplers
						new HashSet<Pair<Integer,P>>(),		// p_nodesPruned
						new HashSet<Pair<Integer,P>>()		// p_nodesTouched
						);
				Sampler firstSampler = new ProtoSampler(initialCID, initialLevel, range, initialWeight);
				samplers.add(firstSampler);
			}
	
			/** Returns condensed profiling information: number of nodes touched/pruned per level */ 
			public Pair<Map<Integer,Integer>, Map<Integer, Integer>> getProfilingInformation() {
	//			System.out.println("- touched: "+ p_nodesTouched); // debug
	//			System.out.println("- pruned: "+ p_nodesPruned); // debug			
	
				// process the profiling information			
				Map<Integer, Integer> touchedByLevel = new TreeMap<Integer, Integer>();
				for(Pair<Integer, P> nodeId : p_nodesTouched) {
					int level = nodeId.getElement1();
					touchedByLevel.putIfAbsent(level, 0);
					touchedByLevel.put(level, touchedByLevel.get(level)+1);
				}
				
				Map<Integer, Integer> prunedByLevel = new TreeMap<Integer, Integer>();
				for(Pair<Integer, P> nodeId : p_nodesPruned) {
					int level = nodeId.getElement1();
					prunedByLevel.putIfAbsent(level, 0);
					prunedByLevel.put(level, prunedByLevel.get(level)+1);
				}
				
				return new Pair<Map<Integer, Integer>, Map<Integer, Integer>>(touchedByLevel, prunedByLevel);
			}
			
			/** Performs a batched trial of n draws. The amount of samples generated will typically be much less. */
			public List<V> tryToSample(int n) {
				List<V> samplesObtained = new LinkedList<V>();
	
				//- get the weights of the samplers
				List<Integer> weights = new ArrayList<Integer>(samplers.size());
				for(Sampler sampler : samplers)
					weights.add(sampler.weight());
				//- determine how many to draw from which
				List<Integer> toDraw = Randoms.multinomialDist(weights, n, rng);
				
				//- fetch accordingly from each sampler and save the results
				List<SamplingResult> results = new ArrayList<SamplingResult>(samplers.size());
				for(int i=0; i < toDraw.size(); i++) {
					// .. and check on nodes which would get sampled whether they are disjoint with the query
					// this late checking enables us to adhere to the paper.
					SamplingResult res = null;
					Sampler sampler = samplers.get(i);
					if(toDraw.get(i) > 0 && !query.intersects(sampler.getRange()) ) {
						p_nodesPruned.add(samplers.get(i).getNodeIdentifier());
						res = new SamplingResult();
					} else {					
						res = samplers.get(i).tryToSample(toDraw.get(i));
					}
					results.add(res);
					samplesObtained.addAll(res.samplesObtained);
				}
				
				//- perform the batch updating
				int i = 0, removed = 0;
				for(SamplingResult res : results) {				
					if(res.replacementNeeded) {
						samplers.remove(i - removed);
						removed++;
						
						if(res.replacee != null) {
							samplers.addAll(res.replacee.samplers);
						}
					}
					i++;
				}
				
				return samplesObtained;
			}
			
			//-------------------- AbstractCursor concretization
			@Override
			protected boolean hasNextObject() {
				while(precomputed.isEmpty()) {
					if(samplers.isEmpty()) return false;
					precomputed.addAll(tryToSample(batchSize));
				}			
				return true;
			}
	
			@Override
			protected V nextObject() {
				return precomputed.remove();
			}
	
	
			/** Factory for Samplers - respectively different initialisation of
			 * subclasses depending on the node contents. */
			public Sampler createRealSampler(P nodeCID, int level, Interval<K> range) {
				Node node = container.get(nodeCID);
				if (node.isInner()) {
					InnerNode inode = (InnerNode) node;
					if (inode.hasSampleBuffer())
						return new InnerSampler(nodeCID, level, range);
					else {
						/* Mind that we can only create InnerNodes with buffers in the general case
						 * (that means Insertions and Deletions) if:
						 * "branchingLo * leafLo < samplesPerNodeHi". 
						 * (Except for the root where "2 * leafLo < samplesPerNodeHi")
						 * 
						 * If our tree is built only from insertions, then this amounts too:
						 * "branchingHi / 2 * leafHi / 2 < samplesPerNodeHi" (for non-roots)
						 */
						return new UnbufferedSampler(nodeCID, level, range);
					}
				} else {
					return new UnbufferedSampler(nodeCID, level, range);
				}
			}
	
			public abstract class Sampler {
				P nodeCID;
				int level;
				Interval<K> range;
							
				public Sampler(P nodeCID, int level, Interval<K> range) {
					super();
					this.nodeCID = nodeCID;
					this.level = level;
					this.range = range;
				}
	
				public abstract SamplingResult tryToSample(int n);
				
				public abstract int weight();
				
				public Pair<Integer, P> getNodeIdentifier() {
					return new Pair<Integer, P>(level, nodeCID);
				}
	
				public Interval<K> getRange() {
					return range;
				}
			}
	
			public class ProtoSampler extends Sampler /* implements Decorator<Sampler> */ {
				Sampler realSampler = null;
				int savedWeight;
				public ProtoSampler(P nodeCID, int level, Interval<K> range, int savedWeight) {
					super(nodeCID, level, range);
					this.range = range;
					
					this.savedWeight = savedWeight;
				}			
				
				@Override
				public int weight() {
					if(realSampler == null)
						return savedWeight;
					else
						return realSampler.weight();
				}
				
				@Override
				public ReallyLazySamplingCursor.SamplingResult tryToSample(int n) {
					if(n == 0) {
						List<V> samplesObtained = new LinkedList<V>();
						return new SamplingResult(samplesObtained);
					} else {
						if(realSampler == null) {						
							realSampler = createRealSampler(nodeCID, level, range);
						}
						return realSampler.tryToSample(n);
					}
				}
				
			}
	
			/** We need to keep track of the state of this sampler so meticulously to adhere to the paper, because it is 
			 * (probably meant to be) evaluated lazily like this, and otherwise the batched probabilities would get skewed.
			 * 
			 * Imagine a frontier with nodes a_1, a_2, ... . T(a_1) is a big tree which only contains a really small 
			 * single relevant part T(b). (And the samples buffer of a_1 is as good as exhausted.)  
			 * On a batched sample of Frontier = (a_1, ..., a_n) 
			 * T(b) would get sampled with probability:
			 * p_1 = |P(a_1)| / sum(|P(a_1)|, ..., |P(a_n)|)
			 * This is incorrect as T(b) should only get sampled with probability:
			 * p_2 = |P(b)| / sum(|P(a_1)|, ..., |P(a_n)|)
			 * Which might make a major difference for |P(b)| << |P(a_1)|
			 * 
			 * Also this incorrectness would cascade.
			 */
			public class UnbufferedSampler extends Sampler {
				List<V> uncategorized;
				ArrayList<V> keepers;
				
				/** Constructs an UnbufferedSampler which is essentially an object to keep track of the state of 
				 * sampling from an InnerNode without attached sample buffer, or a LeafNode. 
				 * See (referencepaper) Algorithm 1: lines 7-11
				 * 
				 * We deviate from the description in the paper a bit, by loading all values of a subtree eagerly
				 * on the first attempt to sample from an UnbufferedSampler.
				 * But this should usually not matter much (hopefully).
				 */
				public UnbufferedSampler(P nodeCID, int level, Interval<K> range) {
					super(nodeCID, level, range);
					this.range = range;
					
					this.uncategorized = new LinkedList<V>();
					//- fetch the values yourself - replacement for allValues (this is a right-to-left DFS traversal of the subtree)
					LinkedList<Pair<Integer, P>> dfsQueue = new LinkedList<Pair<Integer, P>>();
					dfsQueue.add(new Pair<Integer, P>(level, nodeCID));
					
					while(!dfsQueue.isEmpty()) {
						Pair<Integer, P> nodeId = dfsQueue.pop();
						int curnodeLevel = nodeId.getElement1(); P curnodeCID = nodeId.getElement2();
												
						Node curNode = container.get(curnodeCID);						
						p_nodesTouched.add(nodeId); // mark as touched
						
						if(curNode.isLeaf()) {
							LeafNode lNode = (LeafNode) curNode;
							uncategorized.addAll(lNode.values);
						} else {
							InnerNode iNode = (InnerNode) curNode;
							assert !iNode.hasSampleBuffer();
							
							for(P nextNodeCID : iNode.pagePointers) {
								dfsQueue.add(new Pair<Integer, P>(curnodeLevel+1, nextNodeCID));
							}
						}
					}
					
					this.keepers = new ArrayList<V>(uncategorized.size());
				}
	
				
				
				/** This only occassionally produces a sample. Might fail as long as we have uncategorized elements left. */
				/*protected V trySample1() {		
					int x = rng.nextInt(uncategorized.size() + keepers.size());
					return trySample1(x);
				}*/
				
				/** This only occassionally produces a sample. Might fail as long as we have uncategorized elements left. */
				protected V trySample1(int x) {		
					if(x < uncategorized.size()) { // categorize new one
						V sample = uncategorized.remove(x);
						if(query.contains(getKey.apply(sample))) {
							keepers.add(sample);
							return sample;
						} else
							return null;
					} else {
						return keepers.get(x - uncategorized.size());
					}	
				}
				
				/** Batched sampling of n elements where the the effective weight of the node is only updated afterwards. */ 
				public SamplingResult tryToSample(int n) {
					LinkedList<V> samplesObtained = new LinkedList<V>();
					
					int oldAvailable = uncategorized.size() + keepers.size();
	
					for(int i=0; i < n; i++) {
						int x = rng.nextInt(oldAvailable);
						if(x < uncategorized.size() + keepers.size()) { // otherwise we hit a element which got disabled during this run
							V sample = trySample1(x);
							if(sample != null)
								samplesObtained.add(sample);
						}				
					}
						
					if(weight() == 0) {
						assert samplesObtained.isEmpty();
						return new SamplingResult();
					}
					else
						return new SamplingResult(samplesObtained);
				}
				
				/** "Iterative" sampling which adjusts the effective weight after each draw.
				 * This does updates after each draw, which is not what we want in a batched draw. 
				 * 		-> QUE: chance for optimisation? */ 
				/*private SamplingResult tryToSample_iterative(int n) {
					LinkedList<V> samplesObtained = new LinkedList<V>();
								
					try {
						
						for (int i = 0; i < n; i++) {
							V sample = trySample1();
							if (sample != null)
								samplesObtained.add(sample);
						} 
						
					} catch (IllegalArgumentException e) { // this means that this sampler is empty						
						assert e.getMessage() == "bound must be positive";
						assert samplesObtained.isEmpty();
						
						// as we can't descent any further from unbuffered nodes, return an empty sampler list.
						return new SamplingResult();
					}
					
					return new SamplingResult(samplesObtained);
				}*/
				
				@Override
				public int weight() {
					return uncategorized.size() + keepers.size();
				}
	
			}
			
			
			public class InnerSampler extends Sampler {
				Iterator<V> sampleIter;
				int savedWeight;
				
				public InnerSampler(P nodeCID, int level, Interval<K> range) {
					super(nodeCID, level, range);
					
					// do eager initialization here, as we now have ProtoSamplers
					InnerNode inode = (InnerNode) container.get(nodeCID);				
					p_nodesTouched.add(new Pair<Integer, P>(level, nodeCID)); // mark as touched
					
					this.savedWeight = inode.totalWeight(); // OPT: could be passed from parent too ...
					
					assert inode.hasSampleBuffer();
					sampleIter = inode.samples.iterator(); 
				}
				
				@Override
				public SamplingResult tryToSample(int n) {				
					List<V> samplesObtained = new LinkedList<V>();
					
					int i = 0;
					for(; i < n && sampleIter.hasNext(); i++) {
						V sample = sampleIter.next();
						if(query.contains(getKey.apply(sample))) {
							samplesObtained.add(sample);
						}
					}
					
					if(i < n) { // we didn't find enough samples in the sample buffer
						int remaining = n - i;
						InnerNode inode = (InnerNode) container.get(nodeCID);
	
						// recursively create a new ReallyLazySamplingCursor
						List<Sampler> protoSamplers = new LinkedList<Sampler>();
						for(int j=0; j < inode.pagePointers.size(); j++) {
							protoSamplers.add(
									new ProtoSampler(inode.pagePointers.get(j), level-1, inode.ranges.get(j), inode.childWeights.get(j)));
						}
						
						ReallyLazySamplingCursor subCursor = new ReallyLazySamplingCursor(query, batchSize, protoSamplers, p_nodesTouched, p_nodesPruned);
						
						samplesObtained.addAll(subCursor.tryToSample(remaining));
						
						return new SamplingResult(samplesObtained, subCursor);
					} else {
						return new SamplingResult(samplesObtained);
					}
				}
				
				@Override
				public int weight() {
					return savedWeight;
				}
	
			}
			
			/** Field class for results of sampling from a node. */
			public class SamplingResult {
				boolean replacementNeeded;
				ReallyLazySamplingCursor replacee;
				List<V> samplesObtained;
				
				/** Inner nodes which don't need expansion or eternal unbuffered nodes. */
				public SamplingResult(List<V> samplesObtained) { 
					this.replacementNeeded = false;
					this.replacee = null;
					this.samplesObtained = samplesObtained;
				}
				
				/** For inner nodes which produced something and need expansion. */
				public SamplingResult(List<V> samplesObtained, ReallyLazySamplingCursor replacee) { 			
					this.replacementNeeded = true;
					this.replacee = replacee;
					this.samplesObtained = samplesObtained;
				}
				
				/** No overlap with the query or unbuffered nodes which got terminated. */
				public SamplingResult() { 
					this.replacementNeeded = true;
					this.replacee = null;
					this.samplesObtained = new LinkedList<V>();
				}
			}
			
			
		}

	public int weight() {
		if(rootCID == null)
			return 0;
		else				
			return container.get(rootCID).totalWeight();
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