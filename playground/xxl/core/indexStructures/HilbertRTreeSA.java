package xxl.core.indexStructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;

import com.google.common.collect.Ordering;

import xxl.core.collections.MappedList;
import xxl.core.collections.containers.CastingContainer;
import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.TypeSafeContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.functions.FunJ8;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.LongConverter;
import xxl.core.profiling.ProfilingCursor;
import xxl.core.spatial.rectangles.FixedPointRectangle;
import xxl.core.util.CopyableRandom;
import xxl.core.util.HUtil;
import xxl.core.util.Interval;
import xxl.core.util.Pair;
import xxl.core.util.Randoms;
import xxl.core.util.Sample;

public class HilbertRTreeSA<V, P> implements TestableMap<Long, V>
	// FixedPointRectangle (respectively any hypercubes) are not comparable naturally, so we can't support Comparable
	/* implements SamplableMap<FixedPointRectangle, V> */ 
{

    /** The data dimension. */
    int dimension;	
    
    /** The splitting policy: "s-to-(s+1)" */
    int splitPolicy = 1;
    
    /** Computes the bounding boxes of the elements. */
    Function<V, FixedPointRectangle> getBoundingBox;
    
    /** Space-filling curve. 
     * For arbitrary dimensional hilbert curves, perhaps see: https://github.com/aioaneid/uzaygezen
     * */
    Function<FixedPointRectangle, Long> getSFCKey;
    
//    /** Precision of the space-filling curve. */
//    long precision = 1 << 20;

//    /** Ubiquitious getKey function which maps from values (V) to keys (FixedPointRectangle). */
//	public Function<V, FixedPointRectangle> getKey;
    
    
    
	/** How many samples per node should be kept = parameter s. 
	 * The buffers must have between s/2 and 2*s items at all times. */
//	final int samplesPerNode;
	final int samplesPerNodeLo; // std: = samplesPerNode / 2
	final int samplesPerNodeHi; // std: = samplesPerNode * 2
	final int samplesPerNodeReplenishTarget; // how full shall the buffer be made if we have to replenish it? // std = samplesPerNodeHi
	
	/** RNG used for drawing samples and such. Use {@link #setRNG} to set it. */
	CopyableRandom rng;
	
	/** The branching parameter == the fanout. */
//	final int branchingParam;
	final int branchingLo, branchingHi;	
	final int leafLo, leafHi;	

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

	/** Domain of the value-keys. */
	public FixedPointRectangle universe;
	public Interval<Long> hvUniverse;
	
	/** Amount of duplicate key values allowed. 0 for no restriction. */
	final int nDuplicatesAllowed;
	
	/** --- Constructors & Initialisation ---
	- All mandatory arguments are put into the constructor.
	- The container gets initialized during a later call to <tt>initialize</tt> as we 
		implement the <tt>NodeConverter</tt> functionality once again (like in XXL) as inner class of this tree class.
	*/
	public HilbertRTreeSA(int branchingLo, int branchingHi, int leafLo, int leafHi, int samplesPerNodeLo, int samplesPerNodeHi, 
			int dimension, FixedPointRectangle universe, Function<V, FixedPointRectangle> getBoundingBox, Function<FixedPointRectangle, Long> getSFCKey, 
			int nDuplicatesAllowed) {
		
		this.samplesPerNodeLo = samplesPerNodeLo;
		this.samplesPerNodeHi = samplesPerNodeHi;
		this.branchingLo = branchingLo;
		this.branchingHi = branchingHi;
		this.leafLo = leafLo;
		this.leafHi = leafHi;
		this.dimension = dimension;
		this.universe = universe;
		this.getBoundingBox = getBoundingBox;
		this.getSFCKey = getSFCKey;
		this.nDuplicatesAllowed = nDuplicatesAllowed;
		
		// set the hilbert value universe to the most general case (OPT: initialise hvUniverse smaller)
		this.hvUniverse = new Interval<Long>(Long.MIN_VALUE, true, Long.MAX_VALUE, true);
		
		// defaults
		this.samplesPerNodeReplenishTarget = this.samplesPerNodeHi;
		this.rng = new CopyableRandom();
	}

//	/** Loads a whole RSTree - that is in addition to the data, the correct parameters (branching factor, etc.)
//	 * 		of the tree from a metadata file. Just some things have to be given which can't be serialized or
//	 * 		determine the types.  
//	 * @param metaDataFilename The absolute path to the metaDataFileName. 
//	 * @param containerFactory Function which builds a container from an absolute filename. 
//	 * 		Allows exchanging of different wrapping containers in between.
//	 * 		Default use: containerFactory = BlockFileContainer::new 
//	 * @throws IOException
//	 */
//	public static <FixedPointRectangle extends Comparable<FixedPointRectangle>, V, P> TestableMap<FixedPointRectangle, V> loadFromMetaData(
//			String metaDataFilename, 
//			Function<String, Container> containerFactory,  
//			Converter<FixedPointRectangle> hvRangeConverter, 
//			Converter<V> valueConverter,
//			Function<V,FixedPointRectangle> getKey) throws IOException {
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
//		Interval<FixedPointRectangle> universe = Interval.getConverter(hvRangeConverter).read(metaData);
//		int samplesPerNodeLo = metaData.readInt();
//		int samplesPerNodeHi = metaData.readInt();
//		int branchingLo = metaData.readInt();
//		int branchingHi = metaData.readInt();
//		int leafLo = metaData.readInt();
//		int leafHi = metaData.readInt();
//		int nDuplicatesAllowed = metaData.readInt();
//		
//		//-- construct and initialize the tree
//		HilbertRTreeSA<FixedPointRectangle, V, P> instance = new HilbertRTreeSA<FixedPointRectangle, V, P>(branchingLo, branchingHi, leafLo, leafHi, samplesPerNodeLo, samplesPerNodeHi, universe, getKey, nDuplicatesAllowed);
//		instance.initialize_buildContainer(rawContainer, hvRangeConverter, valueConverter);
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
//	public void writeToMetaData(
//			String metaDataFilename, 
//			String dataFileName,
//			Converter<FixedPointRectangle> hvRangeConverter, 
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
//		Interval.getConverter(hvRangeConverter).write(metaData, universe);
//		metaData.writeInt(samplesPerNodeLo);
//		metaData.writeInt(samplesPerNodeHi);
//		metaData.writeInt(branchingLo);
//		metaData.writeInt(branchingHi);
//		metaData.writeInt(leafLo);
//		metaData.writeInt(leafHi);
//		metaData.writeInt(nDuplicatesAllowed);
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
	 * @param hvConverter converter for the key-type FixedPointRectangle 
	 * @param valueConverter converter for the value type V
	 */
	public void initialize_buildContainer(Container rawContainer, Converter<V> valueConverter) {
		NodeConverter nodeConverter = 
				new NodeConverter(valueConverter);
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
	public boolean insert(V value) {
		if(rootCID == null) { // tree empty
			LeafNode root = new LeafNode();
			root.values.add(value);
			rootHeight = 0;
			rootCID = container.insert(root);
			return true;
		} else { // tree not empty
			Node root = getRoot();
			long lhKey = getSFCKey.apply(getBoundingBox.apply(value));
			InsertionInfo insertionInfo = root.insert(lhKey, value);

			if(!insertionInfo.insertionSuccessful) {
				return false;
			} else if(insertionInfo.newnode == null) { // normal insertion
				return true;
			} else { // insertion resulting in new root
				InnerNode rootNew = new InnerNode();
				
				rootNew.pagePointers.add(rootCID);
				rootNew.pagePointers.add(insertionInfo.newnode.pagePointer);
				
				rootNew.childWeights.add(root.totalWeight());
				rootNew.childWeights.add(insertionInfo.newnode.totalWeight());
				
				rootNew.areaRanges.add(root.getBoundingBox());
				rootNew.areaRanges.add(insertionInfo.newnode.getBoundingBox());
				
				rootNew.lhvSeparators.add(root.getLHV());
				rootNew.lhvSeparators.add(Long.MAX_VALUE);
				
				// fill the root node with samples again
				if(rootNew.shouldHaveSampleBuffer()) {
					rootNew.samples = new LinkedList<V>();
					rootNew.repairSampleBuffer();				
				}
				
				rootCID = container.insert(rootNew);
				rootHeight++;
				return true;
			}
		}
	}

	/** Lookup. */
	public List<V> get(FixedPointRectangle query) {
		if(rootCID == null)  // tree empty
			return new LinkedList<V>();
		else {
			Node root = container.get(rootCID);
			return root.get(query);
		}
	} 
		
	
	/**
	 * Generalization of SplitInfo class which is used to report the result of an
	 * operation in a subtree. This can either be the information that and how the child has split (~ SplitInfo),
	 * or the count of the entries that got changed (think of removals, or insertions in duplicate free trees) which
	 * is needed to maintain aggregate meta-information. 
	 */
	class InsertionInfo {
		boolean insertionSuccessful = false;
		Interval<Integer> idxsChanged = null;
		Node newnode = null;
	}

	//-- Factories for the different cases of insertion results
	InsertionInfo NO_INSERTION() {
		InsertionInfo insertInfo = new InsertionInfo();
		return insertInfo;
	}
	
	InsertionInfo SINGLE_UPDATE(Integer myParentIdx) {
		InsertionInfo insertInfo = new InsertionInfo();
		insertInfo.insertionSuccessful = true;
		if(myParentIdx != null) // in case of root, we don't have or need the parentIdx
			insertInfo.idxsChanged = new Interval<Integer>(myParentIdx);
		return insertInfo;
	}
	
	InsertionInfo SHARING(Interval<Integer> coopSiblingIdxs) {
		InsertionInfo insertInfo = new InsertionInfo();
		insertInfo.insertionSuccessful = true;
		insertInfo.idxsChanged = coopSiblingIdxs;
		return insertInfo;
	}
	
	InsertionInfo SPLIT(Interval<Integer> coopSiblingIdxs, Node newnode) {
		InsertionInfo insertInfo = new InsertionInfo();
		insertInfo.insertionSuccessful = true;
		insertInfo.idxsChanged = coopSiblingIdxs;
		insertInfo.newnode = newnode;
		return insertInfo;
	}

	public Node getNode(InnerNode parent, int idxInParent) {
		P nodeCID = parent.pagePointers.get(idxInParent);
		Node node = container.get(nodeCID);
		node.parent = parent;
		node.idxInParent = idxInParent;
		node.level = parent.level - 1;
		node.pagePointer = nodeCID;
		return node;
	}
	
	public Node getRoot() {
		Node root = container.get(rootCID);
		root.level = rootHeight;
		root.pagePointer = rootCID;
//		root.parent = null;
//		root.idxInParent = null;
		return root;
	}
	
	//-- Node class
	public abstract class Node {
		
		InnerNode parent;
		int level;
		Integer idxInParent;
		P pagePointer;

//		public void setEnvironment(InnerNode parent, int level) {
//			this.parent = parent;
//			this.level = level;
//		}
//		
//		public void setEnvironment(InnerNode parent, int level, Integer idxInParent) {
//			this.parent = parent;
//			this.level = level;
//			this.idxInParent = idxInParent;
//		}
//		
//		public void setEnvironment(InnerNode parent, int level, Integer idxInParent, P pagePointer) {
//			this.parent = parent;
//			this.level = level;
//			this.idxInParent = idxInParent;
//			this.pagePointer = pagePointer;
//		}
		
		//- information about the environment of the node 
		public InnerNode getParent() { return parent; }
		public int getLevel() { return level; }
		public Integer getIdxInParent() { return idxInParent; }
		
		//- node methods
		public abstract boolean isLeaf();
		public boolean isInner() { return !isLeaf(); }
		
		public abstract InsertionInfo insert(Long hvKey, V value);

		protected abstract List<V> drainSamples(int amount);

		public abstract int totalWeight();

		public abstract List<V> allValues();

		public abstract List<V> get(FixedPointRectangle query);
			
		public <N extends Node> Pair<Interval<Integer>, List<N>> getCooperatingSiblingsAndIdxs() {
			if(parent == null) { // root
				Interval<Integer> idxs = new Interval<Integer>(null);
				List<N> siblings = new LinkedList<>();
				siblings.add((N) this);
				return new Pair<Interval<Integer>, List<N>>(idxs, siblings);
			} else {
				Interval<Integer> idxs = parent.getCooperatingSiblingsIdxsFor(idxInParent);
				List<N> siblings = new LinkedList<>();
				for(int i=idxs.lo; i <= idxs.hi; i++)
					if(i == idxInParent)
						siblings.add((N) this);
					else
						siblings.add((N) getNode(parent, i));
				assert (idxs.hi - idxs.lo + 1 <= splitPolicy) && siblings.size() <= splitPolicy;
				return new Pair<Interval<Integer>, List<N>>(idxs, siblings);
			}
		}
		
//		public Interval<Integer> getCooperatingSiblingIdxs() {
//			if(parent == null) { // root
//				return new Interval<Integer>(null);
//			} else {
//				Interval<Integer> idxs = parent.getCooperatingSiblingsIdxsFor(idxInParent);
//				return idxs;
//			}
//		}
		
		public abstract FixedPointRectangle getBoundingBox();
		public abstract long getLHV();
	}
	
	public class InnerNode extends Node {		
		public List<Long> lhvSeparators;
		public List<FixedPointRectangle> areaRanges;
		public List<P> pagePointers;
		public List<Integer> childWeights;
		
		/** The list of samples kept in this node. */
		public LinkedList<V> samples;
		
		/** Constructor for an empty node, which doesn't have any entries. */
		public InnerNode() {
			this.lhvSeparators = new ArrayList<Long>(branchingHi);
			this.areaRanges = new ArrayList<FixedPointRectangle>(branchingHi);
			this.pagePointers = new ArrayList<P>(branchingHi);
			this.childWeights = new ArrayList<Integer>(branchingHi);
			this.samples = new LinkedList<V>();
		}
		
		public boolean isLeaf() { return false; }
		
		/** A node should have a sample buffer attached if its weight is greater than 2*s,
		 * but what's exactly the rationale behind this? Doesn't the other parameters like leafCapacity
		 * and branchingParam also be taken into account?
		 */
		public boolean hasSampleBuffer() { return samples != null; }
		
		// TODO: clear this up, when a node should and actually has a sample buffer... // in the other trees, too!
		public boolean shouldHaveSampleBuffer() { return totalWeight() > samplesPerNodeHi; }

		public boolean overflow() {
			return pagePointers.size() > branchingHi;
		}

		public boolean underflow() {
			return pagePointers.size() < branchingLo;
		}

		public boolean sampleUnderflow() {
			return samples.size() < samplesPerNodeLo;			
		}

		public List<Integer> chooseSubtreeIdxs(FixedPointRectangle query) {
			List<Integer> idxs = new LinkedList<Integer>(); 
			for(int i=0; i < areaRanges.size(); i++)
				if(areaRanges.get(i).overlaps(query))
					idxs.add(i);
			return idxs;
		}
		
		public List<P> chooseSubtreeCIDs(FixedPointRectangle query) {
			return HUtil.getAll(chooseSubtreeIdxs(query), pagePointers);
		}

		@Override
		public int totalWeight() {
			// OPT: somehow prevent recalculation every time.
			return childWeights.stream().reduce(0, (x,y) -> x+y);			
		}

		/** Abstract the determination of choosing which child node to insert a record into. */
		protected Integer findInsertionPos(Long hv) {
			// return the index of the first range which could contain the key
			int i;
			for(i=0; i < lhvSeparators.size(); i++)
				if(hv <= lhvSeparators.get(i))
					return i;
			return null;
		}
		
		public InsertionInfo insert(Long hvKey, V value) {
			//- insert in sublevel
			int pos = findInsertionPos(hvKey);
			Node nextNode = getNode(this, pos);
			
			InsertionInfo childInsertInfo = nextNode.insert(hvKey, value); //== RECURSION ==
			
			if(!childInsertInfo.insertionSuccessful) {
				return NO_INSERTION();
			} else {
				
				//- a split occured in child. put a new entry in the directory.
				if(childInsertInfo.newnode != null) { 
					Node node = childInsertInfo.newnode;
					node.pagePointer = container.insert(node); // save node in container
					
					int insertPos = childInsertInfo.idxsChanged.hi + 1;
					pagePointers.add(insertPos, node.pagePointer);
					// update the rest when the metainfo of the other childs is updated too, for now just fill with dummy values
					areaRanges.add(insertPos, null);
					childWeights.add(insertPos, null);
					lhvSeparators.add(insertPos-1, null); // don't modify the last lhv value
					childInsertInfo.idxsChanged.hi += 1;
				}
				
				//- update meta information for the modified nodes
				updateChildrenInfo(childInsertInfo.idxsChanged);
				
				assert Ordering.natural().isOrdered(lhvSeparators); // DEBUG
				
				
				//- maintain sample buffer (which acts similiar to a reservoir sample on the passing values)
				if(hasSampleBuffer()) {  
					/* Replace every item currently present in the sample buffer with probability 1 / curWeight 
					 * with the newly inserted item. */
					double p = 1.0 / (double)totalWeight();
					for(ListIterator<V> sampleIter = samples.listIterator(); sampleIter.hasNext(); ) {
						sampleIter.next();
						if(rng.nextDouble() < p)
							sampleIter.set(value);
					}
				} else if(shouldHaveSampleBuffer()) { // we currently have no sample buffer but we should have one! 
					samples = new LinkedList<V>();
					repairSampleBuffer();
				}

				//- check for split here
				InsertionInfo insertionInfo = null;
				if(overflow()) {
					insertionInfo = split();
					// container contents are updated inside split
				} else {
					insertionInfo = SINGLE_UPDATE(idxInParent);
					container.update(pagePointer, this); // update container contents of self
				}
				
				return insertionInfo;
			}
		}

		
		public InsertionInfo split() {
			//- fetch cooperating siblings
			Pair<Interval<Integer>, List<InnerNode>> siblingInfo = getCooperatingSiblingsAndIdxs();
			Interval<Integer> coopSiblingIdxs = siblingInfo.getElement1();
			List<InnerNode> coopSiblings = siblingInfo.getElement2();
			
			//- check whether we can avoid a split through sharing and construct the approbiate InsertionInfo object			
			int summedSizes = coopSiblings.stream().map((InnerNode x) -> x.pagePointers.size()).reduce(0, (x,y) -> x+y);
			int summedCapacity = branchingHi * coopSiblings.size();
			
			InsertionInfo insertionInfo;
			if(summedSizes > summedCapacity) { // split can't be avoided
				// create new node
				InnerNode newnode = new InnerNode();
				newnode.pagePointer = container.reserve(null); // assign a CID to the newly created node
				coopSiblings.add(newnode); // update all references last
				insertionInfo = SPLIT(coopSiblingIdxs, newnode);
			} else {
				insertionInfo = SHARING(coopSiblingIdxs);
			}
			
			//- Collect all child entries (with meta information) and samples
			List<Long> all_lhvRanges = new LinkedList<Long>();
			List<FixedPointRectangle> all_areaRanges = new LinkedList<FixedPointRectangle>();
			List<P> all_pagePointers = new LinkedList<P>();
			List<Integer> all_childWeights = new LinkedList<Integer>();
			
			LinkedList<V> all_samples = new LinkedList<V>(); // need to be redistributed afterwards
			
			for(InnerNode sibling : coopSiblings) {
				all_lhvRanges.addAll(sibling.lhvSeparators);
				all_areaRanges.addAll(sibling.areaRanges);
				all_pagePointers.addAll(sibling.pagePointers);
				all_childWeights.addAll(sibling.childWeights);
				
				all_samples.addAll(sibling.samples);
			}

			//- Redistribute child entries again
			List<Integer> sizes = HUtil.partitionInNParts(summedSizes, coopSiblings.size());
			int i = 0;
			for(InnerNode sibling : coopSiblings) {
				int curSize = sizes.get(i);
				sibling.lhvSeparators = HUtil.splitOffLeft(all_lhvRanges, curSize, new ArrayList<>(branchingHi));
				sibling.areaRanges = HUtil.splitOffLeft(all_areaRanges, curSize, new ArrayList<>(branchingHi));
				sibling.pagePointers = HUtil.splitOffLeft(all_pagePointers, curSize, new ArrayList<>(branchingHi));
				sibling.childWeights = HUtil.splitOffLeft(all_childWeights, curSize, new ArrayList<>(branchingHi));
				sibling.samples = sibling.shouldHaveSampleBuffer() ? new LinkedList<V>() : null;
				i++;
			}
			
			//-- Redistribute samples (slow and kind of hacky (OPT))
			for(V sample : all_samples) {
				for(InnerNode sibling : coopSiblings) {
					if(getSFCKey.apply(getBoundingBox.apply(sample)) <= sibling.getLHV()) { // we can add it here
						if(sibling.shouldHaveSampleBuffer()) {
							if(sibling.hasSampleBuffer() == false)
								sibling.samples = new LinkedList<V>();
							sibling.samples.add(sample);							
						}
						break;
					}
				}
			}
			
			//- check whether distribution stays ok
			for(InnerNode sibling : coopSiblings)
				assert sibling.samples == null || sibling.samples.size() <= samplesPerNodeHi;
			
			//- update container contents of all cooperating siblings
			for(InnerNode sibling : coopSiblings)
				container.update(sibling.pagePointer, sibling);
			
			//- return
			return insertionInfo;
		}
		
		protected void updateChildrenInfo(Interval<Integer> modIdxs) {
			//- update meta information for the modified nodes
			for(int modIdx = modIdxs.lo; modIdx <= modIdxs.hi; modIdx++) {
				Node node = getNode(this, modIdx);
				
				areaRanges.set(modIdx, node.getBoundingBox());
				childWeights.set(modIdx, node.totalWeight());
				// last lhv entry never gets updated as it saves the value in for the partition in the parent node
				if(modIdx < lhvSeparators.size() - 1) {  
					lhvSeparators.set(modIdx, node.getLHV());
				}
			}
		}
		
		
		/** Returns all values in the subtree originating from this node. */
		public List<V> allValues() {
			LinkedList<V> allVals = new LinkedList<V>();
			for(P childCID : pagePointers)
//				allVals.addAll(getNode(this, childCID).allValues());
				allVals.addAll(container.get(childCID).allValues()); // fetch without context
			return allVals;
		}
		
		/**
		 * Returns all values relevant for a given query in this' node subtree. 
		 * Needed for the sampling cursor when we have no sample buffer attached to a node.
		 * OPT: only called from xxl.core.indexStructures.HilbertRTreeSA.SamplingCursor.addToFrontier(P) -> inline?
		 */
//		protected List<V> relevantValues(Interval<FixedPointRectangle> query) {
//			List<V> allValues = new LinkedList<V>(); // OPT use something which allows for O(1) concatenation
//			for(int i : relevantChildIdxs(query)) {
//				Node child = container.get(pagePointers.get(i));
//				allValues.addAll(child.relevantValues(query));
//			}
//			return allValues;
//		}
//		
//		/**
//		 * Returns the indices of all childs relevant for a given query. 
//		 */
//		/**
//		 * @param query
//		 * @return
//		 */
//		protected List<Integer> relevantChildIdxs(Interval<FixedPointRectangle> query) {
//			List<Integer> relChilds = new LinkedList<Integer>(); // OPT use something which allows for O(1) concatenation
//			for (int i = 0; i < ranges.size(); i++)
//				if(ranges.get(i).intersects(query))
//					relChilds.add(i);
//			return relChilds;
//		}
//		
//		/**
//		 * @param query
//		 * @return
//		 */
//		protected List<P> relevantChildCIDs(Interval<FixedPointRectangle> query) {
//			return HUtil.getAll(relevantChildIdxs(query), pagePointers);
//		}
		
		/**
		 * Checks for a underflow in the sample buffer and repairs it.
		 * Repairing for InnerNodes is done by draining samples from the child nodes.
		 * OPT: only called from xxl.core.indexStructures.HilbertRTreeSA.InnerNode.split() -> inline?
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
				Node child = getNode(this, i);
				List<V> fetchedFromChild = child.drainSamples(nSamplesPerChild.get(i));
				// .. and put them in sample buffer
				samples.addAll(fetchedFromChild);
			}
			
			//-- permute the newly built sample buffer (= O(#samples))
			Sample.permute(samples, rng);
		}

		/** Lookup. */
		public List<V> get(FixedPointRectangle query) {
			List<V> results = new LinkedList<V>();
			for(P nextNodeCID : chooseSubtreeCIDs(query)) {
//				Node nextNode = getNode(this, nextNodeCID);
				Node nextNode = container.get(nextNodeCID); // fetch without context
				results.addAll(nextNode.get(query));
			}		
			return results;
		}

		
		public FixedPointRectangle getBoundingBox() {
			Iterator<FixedPointRectangle> areaRangesIter = areaRanges.iterator();
			FixedPointRectangle bbox = (FixedPointRectangle) areaRangesIter.next().clone();
			while(areaRangesIter.hasNext())
				bbox.union(areaRangesIter.next());
			return bbox;
		}
		
		public long getLHV() {
			return lhvSeparators.get(lhvSeparators.size() - 1);
		}

		public Interval<Integer> getCooperatingSiblingsIdxsFor(int idx) {
			int rExtend = splitPolicy / 2; 
			int lExtend = splitPolicy - rExtend - 1;
			
			int rBound = idx + rExtend;
			if(rBound >= pagePointers.size()) {
				lExtend += rBound - (pagePointers.size() - 1);
				rBound = pagePointers.size() - 1;
			}
			
			int lBound = idx - lExtend;
			if(lBound < 0) lBound = 0;
			
			return new Interval<Integer>(lBound, rBound);
		}
		
		
	}

	public class LeafNode extends Node {
		public List<V> values;
		
		public LeafNode() {
			values = new ArrayList<V>(leafHi);
		}

		public boolean isLeaf() { return true; }
		
		public boolean overflow() {
			return values.size() > leafHi;
		}

		public boolean underflow() {
			return values.size() < leafLo;
		}
		
		@Override
		public List<V> get(FixedPointRectangle query) {
			List<V> results = new LinkedList<V>();
			for(V value : values)
				if(getBoundingBox.apply(value).overlaps(query))
					results.add(value);
			return results;
		}

		public InsertionInfo insert(Long lhKey, V value) {
			//- find insertion position
			int insertPos = HUtil.binFindES(
					new MappedList<V,Long>(values, FunJ8.toOld(v -> getSFCKey.apply(getBoundingBox.apply(v)))), 
					lhKey);
			//- check for duplicates (but this time it's not the actual key, but the hilbert value)
			if(nDuplicatesAllowed > 0) {
				int nDupsFound = 0;
				for(int i = insertPos; i > 0 && getSFCKey.apply(getBoundingBox.apply(values.get(i-1))).compareTo(lhKey) == 0; i--)
					nDupsFound++;
				if(nDupsFound >= nDuplicatesAllowed) {
					System.out.println("duplicate rejected ("+ nDupsFound +" present of: "+ lhKey +" / "+ value); // DEBUG
					return NO_INSERTION(); // return early
				}
			}
			
			//- insert new element
			values.add(insertPos, value);
			
			//- check for split
			InsertionInfo insertInfo = null;
			if(overflow()) {
				insertInfo = split();
				// container contents are updated inside split
			} else {
				insertInfo = SINGLE_UPDATE(idxInParent);
				// update container contents of self
				container.update(pagePointer, this);
			}
			
			return insertInfo;
		}

		public InsertionInfo split() {
			//- fetch cooperating siblings
			Pair<Interval<Integer>, List<LeafNode>> siblingInfo = getCooperatingSiblingsAndIdxs();
			Interval<Integer> coopSiblingIdxs = siblingInfo.getElement1();
			List<LeafNode> coopSiblings = siblingInfo.getElement2();
			
			//- check whether we can avoid a split through sharing and construct the approbiate InsertionInfo object
			int summedSizes = coopSiblings.stream().map((LeafNode x) -> x.values.size()).reduce(0, (x,y) -> x+y);
			int summedCapacity = leafHi * coopSiblings.size();
			
			InsertionInfo insertionInfo;
			if(summedSizes > summedCapacity) { // split can't be avoided
				// create new node
				LeafNode newnode = new LeafNode();
				newnode.pagePointer = container.reserve(null); // assign a CID to the newly created node
				coopSiblings.add(newnode); // update all references last
				insertionInfo = SPLIT(coopSiblingIdxs, newnode);
			} else {
				insertionInfo = SHARING(coopSiblingIdxs);
			}
			
			//- Collect all relevant values			
			List<V> all_values = new LinkedList<V>();
			for(LeafNode sibling : coopSiblings)
				all_values.addAll(sibling.values);

			// assert that the collected values are ordered
			assert Ordering.natural().isOrdered( new MappedList<V, Long>(all_values, FunJ8.toOld(getSFCKey.compose(getBoundingBox)))); // DEBUG
			
			//- Redistribute them again
			List<Integer> sizes = HUtil.partitionInNParts(summedSizes, coopSiblings.size());
			int i = 0;
			for(LeafNode sibling : coopSiblings) {
				int curSize = sizes.get(i);
				sibling.values = HUtil.splitOffLeft(all_values, curSize, new ArrayList<V>(leafHi));
				i++;
			}
			
			//- update container contents of all cooperating siblings
			for(LeafNode sibling : coopSiblings)
				container.update(sibling.pagePointer, sibling);
			
			return insertionInfo;
		}
		
		
		
//		/**
//		 * Splits the leaf in the middle.
//		 * Or at least tries to do a split as close to the middle as possible, cause duplicate keys might get in the way. 
//		 */
//		public InsertionInfo split() {
//			//- find good splitting position, this is more complicated because of duplicates.
//			int targetPos = values.size() / 2;
//			FixedPointRectangle separator = getSFCKey.apply(values.get(targetPos));
//			
//			int sepLeftPos = targetPos;
//			while(sepLeftPos > 1 && separator.compareTo(getSFCKey.apply(values.get(sepLeftPos-1))) == 0)
//				sepLeftPos--;			
//			int sepRightPos = targetPos;
//			while(sepRightPos < values.size() && separator.compareTo(getSFCKey.apply(values.get(sepRightPos))) == 0)
//				sepRightPos++;
//			
//			int separatorPos;
//			if(targetPos - sepLeftPos <= sepRightPos - targetPos)
//				separatorPos = sepLeftPos;
//			else
//				separatorPos = sepRightPos;
//			
//			//- build new node
//			LeafNode newode = new LeafNode();
//			
//			int remLeft = separatorPos, remRight = values.size() - remLeft;
//			newode.values = HUtil.splitOffRight(values, remLeft, new ArrayList<V>());
//			FixedPointRectangle usedSeparator = getSFCKey.apply(values.get(values.size()-1));
//			
//			//- put new node into Container
//			P newodeCID = container.insert(newode);
//			
//			return new InsertionInfo(newodeCID, usedSeparator, remLeft, remRight);
//		}
		
//		/**
//		 * Splits the leaf in the middle.
//		 * Wrong version with insufficient handling for duplicates.
//		 */
//		public InsertionInfo split() {
//			LeafNode newode = new LeafNode();
//			int separatorPos = values.size() / 2;
//			FixedPointRectangle separator = getKey.apply(values.get(separatorPos));
//			
//			int remLeft = values.size() / 2;
//			int remRight = values.size() - remLeft;
//			
//			newode.values = HUtil.splitOffRight(values, remLeft, new ArrayList<V>());
//			FixedPointRectangle separator = getKey.apply(values.get(values.size()-1));
//			
//			//- put new node into Container
//			P newodeCID = container.insert(newode);
//			
//			return new InsertionInfo(newodeCID, separator, remLeft, remRight);
//		}
		
		public FixedPointRectangle getBoundingBox() {
			//= foldr union $ map getBoundingBox values
			Iterator<FixedPointRectangle> areaRangesIter = 
					new Mapper<V, FixedPointRectangle>(FunJ8.toOld(getBoundingBox), values.iterator());
			FixedPointRectangle bbox = (FixedPointRectangle) areaRangesIter.next().clone();
			while(areaRangesIter.hasNext())
				bbox.union(areaRangesIter.next());
			return bbox;
		}

		public long getLHV() {
			//= getSFCKey $ getBoundingBox $ last values
			return getSFCKey.apply(getBoundingBox.apply(values.get(values.size() - 1)));
		}
		
//		/**
//		 * Returns the indices of the found values. 
//		 * @param key key to look for
//		 * @return list of indices i with "getKey(values[i]) == key" 
//		 */
//		public List<Integer> lookupIdxs(FixedPointRectangle key) {
//			List<Integer> idx = new LinkedList<Integer>();
//
//			List<FixedPointRectangle> mappedList = new MappedList<V,FixedPointRectangle>(values, FunJ8.toOld(getSFCKey));
//			int pos = HUtil.binFindSE(mappedList, key); // get starting position by binary search
//			while(pos < values.size() && key.compareTo(getSFCKey.apply(values.get(pos))) == 0) {
//				idx.add(pos);
//				pos++;
//			}				
//			
//			return idx;
//		}

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
	 * by <tt>HilbertRTreeSA<FixedPointRectangle,V,P>.NodeConverter nodeConverter = tree.new NodeConverter(...)</tt>. 
	 * 
	 * @see HilbertRTreeSA#initialize_withReadyContainer(TypeSafeContainer)
	 */
	@SuppressWarnings("serial")
	public class NodeConverter extends Converter<Node> {

		Converter<Long> hvConverter;
		Converter<FixedPointRectangle> areaConverter;
		
		Converter<V> valueConverter;
		
		public NodeConverter(Converter<V> valueConverter) {
			super();
			this.valueConverter = valueConverter;
			this.hvConverter = LongConverter.DEFAULT_INSTANCE;
			this.areaConverter = new ConvertableConverter<FixedPointRectangle>(FunJ8.toOld( () -> new FixedPointRectangle(dimension) ));
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

			// -- read hilbert-value ranges
			for(int i=0; i < nChildren; i++) {
				node.lhvSeparators.add(hvConverter.read(dataInput));
			}
			
			// -- read area ranges
			for(int i=0; i < nChildren; i++) {
				node.areaRanges.add(areaConverter.read(dataInput));
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

			// -- write hilbert value ranges 
			for (Long hvSep : node.lhvSeparators) {
				hvConverter.write(dataOutput, hvSep);				
			}
			
			// -- write area ranges
			for (FixedPointRectangle areaRange : node.areaRanges) {
				areaConverter.write(dataOutput, areaRange);				
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
	
//	/** Executes a range query of the interval [lo (inclusive), hi (exclusive)[ */
//	@Override
//	public ProfilingCursor<V> rangeQuery(FixedPointRectangle lo, FixedPointRectangle hi){
//		return new HVQueryCursor(new Interval<FixedPointRectangle>(lo, true, hi, false));
//	}
	
	/** Executes a range query of the given query interval, whose exact parameters can be specified. */
	public ProfilingCursor<V> areaRangeQuery(FixedPointRectangle query){
		return new AreaQueryCursor(query);
	}
	
	public class AreaQueryCursor extends AbstractCursor<V> implements ProfilingCursor<V> {
		FixedPointRectangle query;
		LinkedList<Pair<Integer, P>> candidateNodes = new LinkedList<Pair<Integer, P>>();
		/** Profiling information: nodes touched. */
		Set<Pair<Integer, P>> p_nodesTouched = new TreeSet<Pair<Integer,P>>();
		LinkedList<V> precomputed = new LinkedList<V>();
		
		public AreaQueryCursor(FixedPointRectangle query) {
			super();
			this.query = query;
			this.candidateNodes.add(new Pair<Integer, P>(rootHeight, rootCID));
		}

		@Override
		protected boolean hasNextObject() {
			if(!precomputed.isEmpty())
				return true;
			else if(candidateNodes.isEmpty())
				return false;
			else {
				Pair<Integer, P> cand = candidateNodes.pop();
				int level = cand.getElement1(); P nodeCID = cand.getElement2();
				Node node = container.get(nodeCID);
				p_nodesTouched.add(cand);

				// expand one step
				if(node.isLeaf()) {
					precomputed.addAll(((LeafNode) node).get(query));
				} else {
					List<P> nextCIDs = ((InnerNode) node).chooseSubtreeCIDs(query);
					ListIterator<P> nextCIDiter = nextCIDs.listIterator(nextCIDs.size());
					while(nextCIDiter.hasPrevious())
						candidateNodes.addFirst(new Pair<>(level-1, nextCIDiter.previous()));
				}
				return hasNextObject();
			}
		}
		
		@Override
		protected V nextObject() {
			return precomputed.pop();
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

	}
	
	
	/** Executes a range query of the given query interval, whose exact parameters can be specified. */
	public ProfilingCursor<V> hvRangeQuery(Interval<Long> query){
		return new HVQueryCursor(query);
	}
	
	
	
	/** A query cursor for simple range queries.
	 * 
	 * We won't subclass xxl.core.indexStructures.QueryCursor here as it is 
	 * designed for queries over trees which inherit from xxl.core.indexStructures.Tree.
	 */
	public class HVQueryCursor extends AbstractCursor<V> implements ProfilingCursor<V> {
		/** Query interval. */
		final Interval<Long> query;
		
		/** Profiling information: nodes touched. */
		Set<Pair<Integer, P>> p_nodesTouched;		
		/** The heights on which the query was started. Is always initialised to rootHeight and doesn't change.
		 * 		Needed for profiling, though. */
		int startlevel;
		
		/** Path of expanded nodeCIDs */
		Stack<P> sNodeCIDs; // container.get(sNodes.peek()) =: current node
		/** Path of chosen index in corresponding node in sNodes. */
		Stack<Integer> sIdx; // sIdx.peek() =: current index
		
		/** Single precomputed value. */
		V precomputed;
		
		int valuesProduced = 0; // DEBUG
		LinkedList<LeafNode> leafsExpanded = new LinkedList<>();
		
		private HVQueryCursor(Interval<Long> query, P startNode, int startlevel) {
			super();
			//- query
			this.query = query;
			//- profiling
			p_nodesTouched = new HashSet<Pair<Integer,P>>();
			this.startlevel = startlevel;
			//- state
			sNodeCIDs = new Stack<P>();
			sNodeCIDs.push(startNode);
			sIdx = new Stack<Integer>();
			precomputed = null; // the next value to spit out
		}
		
		public HVQueryCursor(Interval<Long> query) {
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
			p_nodesTouched.add(new Pair<Integer, P>(startlevel - sNodeCIDs.size() + 1, sNodeCIDs.peek()));
		}
		
		/** Finds the path to the first entry and locks its nodes in the buffer of the container. */
		@Override
		public void open() {
			// get the current node and lock it in the buffer
			if(sNodeCIDs.peek() == null) return; // happens when tree is empty
			Node curNode = container.get(sNodeCIDs.peek(), false); // this should always be the root if we don't descend from a different node
			markTouched();
			
			while(curNode.isInner()) {
				InnerNode curINode = (InnerNode) curNode;  
				
				// find the index of the next childnode
				// Integer InnerNode.chooseFirstSubtreeIdx_Interval(query); functionality
				int nextPos = 0;
				for(; nextPos < curINode.lhvSeparators.size(); nextPos++) {
					long lhv = curINode.lhvSeparators.get(nextPos);
					if(query.lo < lhv || (query.lo == lhv && query.loIn))
						break;
				}
				
				sIdx.push(nextPos);
				
				// descend to next node
				P nextPID = curINode.pagePointers.get(nextPos);
				sNodeCIDs.push(nextPID);
				curNode = container.get(sNodeCIDs.peek(), false);
				markTouched();
			}
			
			// now our node is a leaf and we just need to find the starting position			
			LeafNode curLNode = (LeafNode) curNode;
			leafsExpanded.add(curLNode); // DEBUG
			
			// find starting position
			List<Long> mappedList = 
					new MappedList<V,Long>(curLNode.values, FunJ8.toOld(getSFCKey.compose(getBoundingBox)));			
			int pos = HUtil.binFindSE(mappedList, query.lo);
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
			while(!sNodeCIDs.empty())
				container.unfix(sNodeCIDs.pop());
			super.close();
		}
		
		private void descendToSmallest() {
			// get the current node and fix it in the buffer
			Node curNode = container.get(sNodeCIDs.peek(), false);			
			markTouched();
			
			while(curNode.isInner()) {
				InnerNode curINode = (InnerNode) curNode;
				// set the index of the current node
				sIdx.push(0);
				
				P nextPID = curINode.pagePointers.get(sIdx.peek());
				sNodeCIDs.push(nextPID);
				curNode = container.get(sNodeCIDs.peek(), false);
				markTouched();
			}
			
			// set the index in the leaf node too
			sIdx.push(0);
		}
		
		private boolean switchToNextNode() {
			// OPT: would perhaps be clearer if not recursive.
			
			// release the active node and index and unfix from the buffer 
			container.unfix(sNodeCIDs.pop()); 
			sIdx.pop();
			
			if(sNodeCIDs.empty()) // recursion exit, no value-next node can be found = right border of tree
				return false;
			
			// get the right brother from the parent node if present..
			InnerNode pNode = (InnerNode) container.get(sNodeCIDs.peek());
			sIdx.push(sIdx.pop() + 1); // increment counter		
			if(sIdx.peek() < pNode.pagePointers.size()) { // switch over if we have a right brother
				sNodeCIDs.push(pNode.pagePointers.get(sIdx.peek()));
				descendToSmallest();
				return true;
			} else { // ..if not call myself recursively, that means looking for a brother of the parent				
				return switchToNextNode();
			}
		}
	
		/* We just need to precompute the value here, all the other logic is handled by AbstractCursor. */ 
		protected boolean hasNextObject() {		
			if(sNodeCIDs.peek() == null) return false; // happens when tree is empty
			LeafNode curLNode = (LeafNode) container.get(sNodeCIDs.peek());
			sIdx.push(sIdx.pop() + 1); // = increment counter			
	
			if(sIdx.peek() >= curLNode.values.size()) { // we need to switch to the node which has the next values
				if(switchToNextNode())
					// fetch the updated leaf node again, that is the state change incured by switchToNextNode() 
					curLNode = (LeafNode) container.get(sNodeCIDs.peek());
				else  
					return false; // hit the right border of the index structure				
			}
			
			leafsExpanded.add(curLNode); // DEBUG
			
			precomputed = curLNode.values.get(sIdx.peek());
			if(!query.contains(getSFCKey.compose(getBoundingBox).apply(precomputed))) {
				if(! (query.locate(getSFCKey.compose(getBoundingBox).apply(precomputed)) > 0) ) {
					List<Long> mappedList = new MappedList<V, Long>(curLNode.values, FunJ8.toOld(getSFCKey.compose(getBoundingBox)));
					assert false;
				};
				return false; // hit the high border
			}
			valuesProduced++;
			return true;
		}
	
		@Override
		protected V nextObject() {
			return precomputed;
		}
	
	}
	
	/** Executes a sampling range query on the area <tt>query</tt> */
	public ProfilingCursor<V> samplingRangeQuery(FixedPointRectangle query, int samplingBatchSize){
		return new ReallyLazySamplingCursor(query, samplingBatchSize, rootCID, rootHeight, universe, container.get(rootCID).totalWeight());
	}

	/** The really lazy (and a bit desoriented) sampling cursor from the paper.
	 * It now allows to batch sampling tries. * 
	 */
	// TODO: counts of inspected nodes are not quite right.
	public class ReallyLazySamplingCursor extends AbstractCursor<V> implements ProfilingCursor<V> {
		/** the query */
		FixedPointRectangle query;
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
		
		
		/** Constructor for child cursors. (called from InnerSampler.tryToSample()) */
		public ReallyLazySamplingCursor(FixedPointRectangle query, int batchSize, 
				List<Sampler> samplers, Set<Pair<Integer, P>> p_nodesTouched, Set<Pair<Integer, P>> p_nodesPruned) {
			super();
			this.query = query;
			this.batchSize = batchSize;
			this.samplers = samplers;
			this.p_nodesTouched = p_nodesTouched;
			this.p_nodesPruned = p_nodesPruned;
		}
		
		/** Constructor for root sampler. Called from RSTree.samplingRangeQuery() */ 
		public ReallyLazySamplingCursor(FixedPointRectangle query, int batchSize, P initialCID, int initialLevel, 
				FixedPointRectangle range, int initialWeight) {
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
				if(toDraw.get(i) > 0 && !query.overlaps(sampler.getRange()) ) {
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
				List<V> nextBatch = tryToSample(batchSize); // permutation needed 
				precomputed.addAll(Arrays.asList(Sample.permute(nextBatch, rng)));
			}			
			return true;
		}

		@Override
		protected V nextObject() {
			return precomputed.remove();
		}


		/** Factory for Samplers - respectively different initialisation of
		 * subclasses depending on the node contents. */
		public Sampler createRealSampler(P nodeCID, int level, FixedPointRectangle range) {
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
			FixedPointRectangle range;
						
			public Sampler(P nodeCID, int level, FixedPointRectangle range) {
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

			public FixedPointRectangle getRange() {
				return range;
			}
		}

		public class ProtoSampler extends Sampler /* implements Decorator<Sampler> */ {
			Sampler realSampler = null;
			int savedWeight;
			public ProtoSampler(P nodeCID, int level, FixedPointRectangle range, int savedWeight) {
				super(nodeCID, level, range);
				
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
			public SamplingResult tryToSample(int n) {
				if(n == 0) {
					List<V> samplesObtained = new LinkedList<V>();
					return new SamplingResult(samplesObtained);
				} else {
					if(realSampler == null)						
						realSampler = createRealSampler(nodeCID, level, range);
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
			public UnbufferedSampler(P nodeCID, int level, FixedPointRectangle range) {
				super(nodeCID, level, range);
				
				this.uncategorized = new LinkedList<V>();
				//- fetch the values yourself - replacement for allValues (this is a right-to-left DFS traversal of the subtree)
				//		-> this way we can keep track of the count of nodes loaded.
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
					if(query.overlaps(getBoundingBox.apply(sample))) {
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
			 * This does updates after each draw, which is not what we want in a batched draw. */ 
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
			
			public InnerSampler(P nodeCID, int level, FixedPointRectangle range) {
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
					if(query.overlaps(getBoundingBox.apply(sample)))
						samplesObtained.add(sample);
				}
				
				if(i < n) { // we didn't find enough samples in the sample buffer
					int remaining = n - i;
					InnerNode inode = (InnerNode) container.get(nodeCID);

					// recursively create a new ReallyLazySamplingCursor
					List<Sampler> protoSamplers = new LinkedList<Sampler>();
					for(int j=0; j < inode.pagePointers.size(); j++) {
						protoSamplers.add(
								new ProtoSampler(inode.pagePointers.get(j), level-1, inode.areaRanges.get(j), inode.childWeights.get(j)));
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

	public int totalWeight() {
		if(rootCID == null)
			return 0;
		else				
			return container.get(rootCID).totalWeight();
	}
	
	

	//-------------------------------------------------------------------------------
	//--- TestableMap<Long, V> interface 
	//-------------------------------------------------------------------------------
	@Override
	public int height() { return rootHeight; }

	@Override
	public Function<V, Long> getGetKey() {
		return getSFCKey.compose(getBoundingBox);
	}
	
	@Override
	public List<V> get(Long key) {
		return Cursors.toList(hvRangeQuery(new Interval<Long>(key)));
	}
	
	@Override
	public ProfilingCursor<V> rangeQuery(Interval<Long> query) {
		return hvRangeQuery(query);
	}




	
}
