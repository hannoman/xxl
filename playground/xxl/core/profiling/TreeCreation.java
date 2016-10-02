package xxl.core.profiling;

import java.io.FileNotFoundException;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.uzaygezen.core.BitVector;
import com.google.uzaygezen.core.BitVectorFactories;
import com.google.uzaygezen.core.CompactHilbertCurve;

import xxl.core.collections.containers.CastingContainer;
import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.TypeSafeContainer;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.cursors.Cursor;
import xxl.core.functions.FunJ8;
import xxl.core.indexStructures.HilbertRTreeSA;
import xxl.core.indexStructures.RSTree1D;
import xxl.core.indexStructures.WRSTree1D;
import xxl.core.indexStructures.WbRS_HilbertRTreeInh;
import xxl.core.indexStructures.HilbertRTreeSA.NodeConverter;
import xxl.core.indexStructures.old.WBTree;
import xxl.core.io.LRUBuffer;
import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.Converters;
import xxl.core.io.converters.DoubleConverter;
import xxl.core.io.converters.FixedSizeConverter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.io.converters.MeasuredFixedSizeConverter;
import xxl.core.profiling.interfaces.Testable1DMap;
import xxl.core.spatial.rectangles.FixedPointRectangle;
import xxl.core.util.CopyableRandom;
import xxl.core.util.Interval;
import xxl.core.util.Pair;
import xxl.core.util.PairConverterFixedSized;
import xxl.core.util.SpatialHelpers;


public class TreeCreation {

	/** General fill method which just takes its values from a data generating cursor. 
	 * Returns a memory map for comparisons against the resulting data structure. */
	public static <K extends Comparable<K>, V> NavigableMap<K, List<V>> fillTestableMap(
			Testable1DMap<K, V> tree, 
			int AMOUNT, 
			Cursor<V> dataCursor,
			Function<V, K> getKey,
			int nDuplicatesAllowed // to be coherent with the tree implementation
			) {
		//-- comparison structure
		TreeMap<K, List<V>> compmap = new TreeMap<K, List<V>>();
		
		//-- Insertion - generate test data		
		System.out.println("-- Insertion test: Generating "+ AMOUNT +" random test data points");
	
		int successfulInsertions = 0;
		for (int i = 1; i <= AMOUNT; i++) {
//			System.out.println("Insertion \t"+ i +"\t ..."); // debug
			V value = dataCursor.next();
			K key = getKey.apply(value);
			boolean insertionSuccessful = tree.insert(value);
			if(insertionSuccessful) successfulInsertions++;
			compmap.putIfAbsent(key, new LinkedList<V>());
			List<V> valueList = compmap.get(key);
			if(nDuplicatesAllowed > 0 && valueList.size() < nDuplicatesAllowed)
				valueList.add(value);
				
			if (i % (AMOUNT / 10) == 0) {
				System.out.print((i / (AMOUNT / 100)) + "%, ");
				System.out.println("inserted: "+ value);
			}
		}
		
		System.out.println("Successful insertions: "+ successfulInsertions);
		System.out.println("Resulting tree height: " + tree.height());
		return compmap;
	}

	/** General fill method which just takes its values from a data generating cursor. 
	 * Returns a memory map for comparisons against the resulting data structure. */
	public static <K extends Comparable<K>, V, P> NavigableMap<K, List<V>> fillTestableMap_RS(
			Testable1DMap<K, V> tree, 
			int AMOUNT, 
			Cursor<V> dataCursor,
			Function<V, K> getKey
			) {
		//-- comparison structure
		TreeMap<K, List<V>> compmap = new TreeMap<K, List<V>>();
		
		//-- Insertion - generate test data		
		System.out.println("-- Insertion test: Generating "+ AMOUNT +" random test data points");
	
//		int newWeightByWeight = tree.weight();
//		int newWeightByValues = Cursors.count(tree.rangeQuery(tree.universe));
		
		for (int i = 1; i <= AMOUNT; i++) {					
			V value = dataCursor.next();
			K key = getKey.apply(value);
			
//			//- check whether the items really get inserted
//			int oldWeightByValues = newWeightByValues;
//			int oldWeightByWeight = newWeightByWeight;
			tree.insert(value);
//			newWeightByWeight = tree.weight();
//			newWeightByValues = Cursors.count(tree.rangeQuery(tree.universe));
//			assert newWeightByWeight > oldWeightByWeight;
//			assert newWeightByValues > oldWeightByValues;
			
			
			//--
			compmap.putIfAbsent(key, new LinkedList<V>());
			compmap.get(key).add(value);  
			if (i % (AMOUNT / 10) == 0) {
				System.out.print((i / (AMOUNT / 100)) + "%, ");
				System.out.println("inserted: "+ value);
			}
		}
		
		System.out.println("Resulting tree weight (by weight): "+ tree.totalWeight());
		System.out.println("Resulting tree height: " + tree.height());
		return compmap;
	}
	
	private static WBTree<Integer, Integer, Long> createWBTree(String testFile) {
		
		WBTree<Integer, Integer, Long> tree = new WBTree<Integer, Integer, Long>(
				10, 										// leafParam
				5, 											// branchingParam
				(x -> x)); 									// getKey

		Converter<Integer> keyConverter = IntegerConverter.DEFAULT_INSTANCE;
		Converter<Integer> valueConverter = IntegerConverter.DEFAULT_INSTANCE;
		Container treeRawContainer = new BlockFileContainer(testFile, BLOCK_SIZE);			
		
		//-- Initialization with container creation inside the tree
		tree.initialize_buildContainer(treeRawContainer, keyConverter, valueConverter);		
		
		System.out.println("Initialization of the tree finished.");
		return tree;
	}
	
	/** Creates a RSTree with fixed branching parameters and block size. Leaf and sample parameters are set space-optimal
	 * with respect to them. */
	public static RSTree1D<Integer, Pair<Integer, Double>, Long> createRSTree_specific_branching(
			String testFile, int BLOCK_SIZE, int branchingParamLo, int branchingParamHi, CopyableRandom rng, 
			Integer nDuplicatesAllowed) {
		Container treeRawContainer = new BlockFileContainer(testFile, BLOCK_SIZE);
		
		FixedSizeConverter<Integer> keyConverter = IntegerConverter.DEFAULT_INSTANCE;		
		FixedSizeConverter<Pair<Integer,Double>> valueConverter = 
				new PairConverterFixedSized<Integer, Double>(IntegerConverter.DEFAULT_INSTANCE, DoubleConverter.DEFAULT_INSTANCE);
		FixedSizeConverter<Interval<Integer>> rangeConverter = Interval.getConverter(keyConverter);
		
		//-- estimating parameters for the tree
		//- fill leafes optimal
		int leafHi = (BLOCK_SIZE - BooleanConverter.SIZE - IntegerConverter.SIZE) / valueConverter.getSerializedSize();
		int leafLo = (int) Math.ceil((double)leafHi / 4.0);
		if(nDuplicatesAllowed == null) nDuplicatesAllowed = leafHi;
		
		//- branching param is fixed
		
		//- determine how much is left for samples
		int innerSpaceLeft = BLOCK_SIZE;
		innerSpaceLeft -= BooleanConverter.SIZE; // node type indicator
		innerSpaceLeft -= IntegerConverter.SIZE; // amount of child nodes
		innerSpaceLeft -= rangeConverter.getSerializedSize() * branchingParamHi; // ranges of children
		innerSpaceLeft -= treeRawContainer.objectIdConverter().getSerializedSize() * branchingParamHi; // childCIDs 
		innerSpaceLeft -= IntegerConverter.SIZE * branchingParamHi; // weights
		
		innerSpaceLeft -= IntegerConverter.SIZE; // amount of samples present
		//- set sample param for the remaining space optimal
		int samplesPerNodeHi = innerSpaceLeft / valueConverter.getSerializedSize();
		int samplesPerNodeLo = samplesPerNodeHi / 4;		
		
		System.out.println("Initializing tree with parameters: ");
		System.out.println("\t block size: \t"+ BLOCK_SIZE);
		System.out.println("\t branching: \t"+ branchingParamLo +" - "+ branchingParamHi);
		System.out.println("\t leafentries: \t"+ leafLo +" - "+ leafHi);
		System.out.println("\t samples: \t"+ samplesPerNodeLo +" - "+ samplesPerNodeHi);

		RSTree1D<Integer, Pair<Integer, Double>, Long> tree = 
				new RSTree1D<Integer, Pair<Integer,Double>, Long>(
						branchingParamLo, 
						branchingParamHi, 
						leafLo, 
						leafHi, 
						samplesPerNodeLo, 
						samplesPerNodeHi, 
						new Interval<Integer>(Integer.MIN_VALUE, Integer.MAX_VALUE), 
						((Pair<Integer, Double> x) -> x.getFirst()),
						nDuplicatesAllowed
					);
		//-- set the PRNG state
		tree.setRNG(rng);
		//-- Initialization with container creation inside the tree
		tree.initialize_buildContainer(treeRawContainer, keyConverter, valueConverter);		
		
		System.out.println("Initialization of the tree finished.");
		return tree;
	}
	
	/** Creates a RSTree with fixed branching parameters and block size. Leaf and sample parameters are set space-optimal
	 * with respect to them. */
	public static RSTree1D<Integer, Pair<Integer, Double>, Long> createRSTree_specific_samples(
			String testFile, int BLOCK_SIZE, int samplesPerNodeLo, int samplesPerNodeHi, CopyableRandom rng, 
			Integer nDuplicatesAllowed) {
		Container treeRawContainer = new BlockFileContainer(testFile, BLOCK_SIZE);
		
		FixedSizeConverter<Integer> keyConverter = IntegerConverter.DEFAULT_INSTANCE;		
		FixedSizeConverter<Pair<Integer,Double>> valueConverter = 
				new PairConverterFixedSized<Integer, Double>(IntegerConverter.DEFAULT_INSTANCE, DoubleConverter.DEFAULT_INSTANCE);
		FixedSizeConverter<Interval<Integer>> rangeConverter = Interval.getConverter(keyConverter);
		
		//-- estimating parameters for the tree
		//- fill leafes optimal
		int leafHi = (BLOCK_SIZE - BooleanConverter.SIZE - IntegerConverter.SIZE) / valueConverter.getSerializedSize();
		int leafLo = (int) Math.ceil((double)leafHi / 4.0);
		if(nDuplicatesAllowed == null) nDuplicatesAllowed = leafHi;
		
		//- sample param is fixed
		
		//- determine how much space is left for samples
		int innerSpaceLeft = BLOCK_SIZE;
		innerSpaceLeft -= BooleanConverter.SIZE; // node type indicator
		innerSpaceLeft -= IntegerConverter.SIZE; // amount of samples
		innerSpaceLeft -= valueConverter.getSerializedSize() * samplesPerNodeHi; // samples
		
		innerSpaceLeft -= IntegerConverter.SIZE; // amount of childs present
		int spacePerIndexEntry = rangeConverter.getSerializedSize() 						// key
							   + treeRawContainer.objectIdConverter().getSerializedSize()	// CID
							   + IntegerConverter.SIZE;										// weight 
				
		//- set branching param fixed
		int branchingParamHi = innerSpaceLeft / spacePerIndexEntry;
		int branchingParamLo = branchingParamHi / 4;
		
		System.out.println("Initializing tree with parameters: ");
		System.out.println("\t block size: \t"+ BLOCK_SIZE);
		System.out.println("\t branching: \t"+ branchingParamLo +" - "+ branchingParamHi);
		System.out.println("\t leafentries: \t"+ leafLo +" - "+ leafHi);
		System.out.println("\t samples: \t"+ samplesPerNodeLo +" - "+ samplesPerNodeHi);

		RSTree1D<Integer, Pair<Integer, Double>, Long> tree = 
				new RSTree1D<Integer, Pair<Integer,Double>, Long>(
						branchingParamLo, 
						branchingParamHi, 
						leafLo, 
						leafHi, 
						samplesPerNodeLo, 
						samplesPerNodeHi, 
						new Interval<Integer>(Integer.MIN_VALUE, Integer.MAX_VALUE), 
						((Pair<Integer, Double> x) -> x.getFirst()),
						nDuplicatesAllowed
					);
		//-- set the PRNG state
		tree.setRNG(rng);
		//-- Initialization with container creation inside the tree
		tree.initialize_buildContainer(treeRawContainer, keyConverter, valueConverter);		
		
		System.out.println("Initialization of the tree finished.");
		return tree;
	}


	/** Tries to set the tree parameters so that actually unbuffered inner nodes can emerge. 
	 * See {@link xxl.core.indexStructures.RSTree1D.ReallyLazySamplingCursor.createSampler(P)}
	 * 
	 * Note: Does not set the PRNG-state of the tree.
	 */
//	public static <K extends Comparable<K>,V> Testable1DMap<K, V> createRSTree_withInnerUnbufferedNodes(
//		String testFile, int BLOCK_SIZE, int branchingParamLoWish, int branchingParamHiWish, CopyableRandom rng, Integer nDuplicatesAllowed) {
//		Container treeRawContainer = new BlockFileContainer(testFile, BLOCK_SIZE);
//		
//		FixedSizeConverter<Integer> keyConverter = IntegerConverter.DEFAULT_INSTANCE;		
//		FixedSizeConverter<Pair<Integer,Double>> valueConverter = 
//				new PairConverterFixedSized<Integer, Double>(IntegerConverter.DEFAULT_INSTANCE, DoubleConverter.DEFAULT_INSTANCE);
//		FixedSizeConverter<Interval<Integer>> rangeConverter = Interval.getConverter(keyConverter);
//		
//		//-- estimating parameters for the tree
//		//- fill leafes optimal
////		int leafHi = (BLOCK_SIZE - BooleanConverter.SIZE - IntegerConverter.SIZE) / valueConverter.getSerializedSize();
//		int leafHi = 10;
//		int leafLo = (int) Math.ceil((double)leafHi / 4.0);
//		if(nDuplicatesAllowed == null) nDuplicatesAllowed = leafHi;
//		
//		//- set branching param fixed
////		int branchingParamHi = 5;
////		int branchingParamLo = (int) Math.ceil((double)branchingParamHi / 4.0);
//		int branchingParamHi = branchingParamHiWish;
//		int branchingParamLo = branchingParamLoWish;
//		
//		//- determine how much is left for samples
//		int innerSpaceLeft = BLOCK_SIZE;
//		innerSpaceLeft -= BooleanConverter.SIZE; // node type indicator
//		innerSpaceLeft -= IntegerConverter.SIZE; // amount of child nodes
//		innerSpaceLeft -= rangeConverter.getSerializedSize() * branchingParamHi; // ranges of children
//		innerSpaceLeft -= treeRawContainer.objectIdConverter().getSerializedSize() * branchingParamHi; // childCIDs 
//		innerSpaceLeft -= IntegerConverter.SIZE * branchingParamHi; // weights
//		
//		innerSpaceLeft -= IntegerConverter.SIZE; // amount of samples present
//		//- set sample param for the remaining space optimal
//		int samplesPerNodeHi = innerSpaceLeft / valueConverter.getSerializedSize();
//		int samplesPerNodeLo = samplesPerNodeHi / 4;		
//		
//		System.out.println("Initializing tree with parameters: ");
//		System.out.println("\t block size: \t"+ BLOCK_SIZE);
//		System.out.println("\t branching: \t"+ branchingParamLo +" - "+ branchingParamHi);
//		System.out.println("\t samples: \t"+ samplesPerNodeLo +" - "+ samplesPerNodeHi);
//		System.out.println("\t leafentries: \t"+ leafLo +" - "+ leafHi);
//	
//		RSTree1D<Integer, Pair<Integer, Double>, Long> tree = 
//				new RSTree1D<Integer, Pair<Integer,Double>, Long>(
//						branchingParamLo, // universe
//						branchingParamHi, 
//						leafLo, 
//						leafHi, 
//						samplesPerNodeLo, 
//						samplesPerNodeHi, 
//						new Interval<Integer>(Integer.MIN_VALUE, Integer.MAX_VALUE), 
//						((Pair<Integer, Double> x) -> x.getFirst()),
//						nDuplicatesAllowed
//					);
//		
//		//-- set the PRNG state
//		tree.setRNG(rng);
//		//-- Initialization with container creation inside the tree
//		tree.initialize_buildContainer(treeRawContainer, keyConverter, valueConverter);		
//		
//		
//		System.out.println("Initialization of the tree finished.");
//		return tree;
//	}

	public static WRSTree1D<Integer, Pair<Integer, Double>, Long> createWRSTree(
		String testFile, int BLOCK_SIZE, int branchingParam, Integer leafParam, CopyableRandom rng, Integer nDuplicatesAllowed) {
		Container treeRawContainer = new BlockFileContainer(testFile, BLOCK_SIZE);
		
		FixedSizeConverter<Integer> keyConverter = IntegerConverter.DEFAULT_INSTANCE;		
		FixedSizeConverter<Pair<Integer,Double>> valueConverter = 
				new PairConverterFixedSized<Integer, Double>(IntegerConverter.DEFAULT_INSTANCE, DoubleConverter.DEFAULT_INSTANCE);
		FixedSizeConverter<Interval<Integer>> rangeConverter = Interval.getConverter(keyConverter);
		
		//-- estimating parameters for the tree
		//- fill leafes optimal
		if(leafParam == null) { // auto set
			int leafsMaxFitting = (BLOCK_SIZE - BooleanConverter.SIZE - IntegerConverter.SIZE) / valueConverter.getSerializedSize();
			leafParam = (leafsMaxFitting + 1) / 2;
		}
		int leafLoBound = leafParam / 2;
		int leafHiBound = leafParam * 2 - 1;
		if(nDuplicatesAllowed == null) nDuplicatesAllowed = leafHiBound;
		
		//- set branching param fixed
		int branchingLoBound = branchingParam / 4 + 1;
		int branchingHiBound = branchingParam * 4 - 1;
		
		
		//- determine how much is left for samples
		int innerSpaceLeft = BLOCK_SIZE;
		innerSpaceLeft -= BooleanConverter.SIZE; // node type indicator
		innerSpaceLeft -= IntegerConverter.SIZE; // amount of child nodes
		innerSpaceLeft -= rangeConverter.getSerializedSize() * branchingHiBound; // ranges of children
		innerSpaceLeft -= treeRawContainer.objectIdConverter().getSerializedSize() * branchingHiBound; // childCIDs 
		innerSpaceLeft -= IntegerConverter.SIZE * branchingHiBound; // weights
		
		innerSpaceLeft -= IntegerConverter.SIZE; // amount of samples present
		//- set sample param for the remaining space optimal
		int samplesPerNodeHi = innerSpaceLeft / valueConverter.getSerializedSize();
		int samplesPerNodeLo = samplesPerNodeHi / 4;		
		
		System.out.println("Initializing tree with parameters: ");
		System.out.println("\t block size: \t"+ BLOCK_SIZE);
		System.out.println("\t branching:\t tA: "+ branchingParam +" ~ ("+ branchingLoBound +" - "+ branchingHiBound +")");
		System.out.println("\t leafentries:\t tK: "+ leafParam +" ~ ("+ leafLoBound +" - "+ leafHiBound +")");
		System.out.println("\t samples:\t "+ samplesPerNodeLo +" - "+ samplesPerNodeHi);

		WRSTree1D<Integer, Pair<Integer, Double>, Long> tree = 
				new WRSTree1D<Integer, Pair<Integer,Double>, Long>(
						branchingParam, // universe
						leafParam, 
						samplesPerNodeLo, 
						samplesPerNodeHi, 
						new Interval<Integer>(Integer.MIN_VALUE, Integer.MAX_VALUE), 
						((Pair<Integer, Double> x) -> x.getFirst()),
						nDuplicatesAllowed
					);
		
		//-- set the PRNG state
		tree.setRNG(rng);
		//-- Initialization with container creation inside the tree
		tree.initialize_buildContainer(treeRawContainer, keyConverter, valueConverter);		
		
		System.out.println("Initialization of the tree finished.");
		return tree;
	}
	
	public static WbRS_HilbertRTreeInh<FixedPointRectangle, Long> createWbHilbertRSTree(
			String testFile, int BLOCK_SIZE, int branchingParam, Integer leafParam, CopyableRandom rng, int[] bitsPerDimensions, Integer nDuplicatesAllowed, int splitPolicy) throws FileNotFoundException {
			
		final int IO_BUFFER_SIZE = 10; final int NODE_BUFFER_SIZE = 0;
		// int[] bitsPerDimensions = {4,4,4};
		int dimension = bitsPerDimensions.length;
//		int splitPolicy = 1;
		
		//======= INITIALISATION
		Container treeRawContainer = new BlockFileContainer(TestUtils.resolveFilename(testFile), BLOCK_SIZE);
		Container treeContainer = IO_BUFFER_SIZE > 0 ? 
										new BufferedContainer(treeRawContainer, new LRUBuffer(IO_BUFFER_SIZE))
									  : treeRawContainer;
		
		MeasuredConverter<Interval<Long>> hvRangeConverter = 
				new MeasuredFixedSizeConverter<Interval<Long>>(Interval.getConverter(LongConverter.DEFAULT_INSTANCE));
		
		Supplier<FixedPointRectangle> fixedPointRectangleFactory = () -> new FixedPointRectangle(dimension);
		MeasuredConverter<FixedPointRectangle> areaConverter = 
				Converters.createMeasuredConverter(dimension * 2 * LongConverter.SIZE, 
						new ConvertableConverter<FixedPointRectangle>(FunJ8.toOld(fixedPointRectangleFactory))); // TODO: how to make it fixed size?
		MeasuredConverter<FixedPointRectangle> valueConverter = areaConverter; // our values are the areas
		
		//-- estimating parameters for the tree
		//- fill leafes optimal
		if(leafParam == null) { // auto set
			int leafsMaxFitting = (BLOCK_SIZE - BooleanConverter.SIZE - IntegerConverter.SIZE) / valueConverter.getMaxObjectSize();
			leafParam = (leafsMaxFitting + 1) / 2;
		}
		int leafLoBound = leafParam / 2;
		int leafHiBound = leafParam * 2 - 1;
		if(nDuplicatesAllowed == null) nDuplicatesAllowed = leafHiBound;
		
		//- set branching param fixed
		int branchingLoBound = branchingParam / 4 + 1;
		int branchingHiBound = branchingParam * 4 - 1;
		
		
		//- determine how much is left for samples
		int innerSpaceLeft = BLOCK_SIZE;
		innerSpaceLeft -= BooleanConverter.SIZE; // node type indicator
		innerSpaceLeft -= IntegerConverter.SIZE; // amount of child nodes
		innerSpaceLeft -= hvRangeConverter.getMaxObjectSize() * branchingHiBound; 	// hilbert value ranges of children
		innerSpaceLeft -= areaConverter.getMaxObjectSize() * branchingHiBound; 		// area ranges of children
		innerSpaceLeft -= treeContainer.objectIdConverter().getSerializedSize() * branchingHiBound; // childCIDs 
		innerSpaceLeft -= IntegerConverter.SIZE * branchingHiBound; // weights
		
		innerSpaceLeft -= IntegerConverter.SIZE; // amount of samples present
		//- set sample param for the remaining space optimal
		int samplesPerNodeHi = innerSpaceLeft / valueConverter.getMaxObjectSize();
		int samplesPerNodeLo = samplesPerNodeHi / 4;		
		
		//== bounding box computer
		Function<FixedPointRectangle, FixedPointRectangle> getBoundingBox = (x -> x);
		
		//== space filling curve		
		FixedPointRectangle universe = DataDistributions.universeForBitsPerDimensions(bitsPerDimensions);
		CompactHilbertCurve hilbertCurve = new CompactHilbertCurve(bitsPerDimensions);
		
		Function<FixedPointRectangle, Long> getSFCKey = new Function<FixedPointRectangle, Long>() {
			// OPT: don't repeat input-/output-var allocation
			@Override
			public Long apply(FixedPointRectangle t) {
				long[] center = SpatialHelpers.centralPoint(t);
				
				//- initialise input variables 
				BitVector[] p = new BitVector[bitsPerDimensions.length];
				for(int i = 0; i < bitsPerDimensions.length; i++) {
		        	p[i] = BitVectorFactories.OPTIMAL.apply(bitsPerDimensions[i]);
		        	p[i].copyFrom(center[i]);
				}
				
				//- initialise output variables
				BitVector chi = BitVectorFactories.OPTIMAL.apply(hilbertCurve.getSpec().sumBitsPerDimension());
				
				hilbertCurve.index(p, 0, chi);
				return chi.toExactLong();
			}
		};
		
		System.out.println("Initializing tree with parameters: ");
		System.out.println("\t block size: \t"+ BLOCK_SIZE);
		System.out.println("\t branching:\t tA: "+ branchingParam +" ~ ("+ branchingLoBound +" - "+ branchingHiBound +")");
		System.out.println("\t leafentries:\t tK: "+ leafParam +" ~ ("+ leafLoBound +" - "+ leafHiBound +")");
		System.out.println("\t samples:\t "+ samplesPerNodeLo +" - "+ samplesPerNodeHi);
		System.out.println("\t number of duplicates allowed: \t"+ nDuplicatesAllowed);
		System.out.println("\t splitPolicy: \t"+ splitPolicy +" -> "+ (splitPolicy+1));

		WbRS_HilbertRTreeInh<FixedPointRectangle, Long> tree = 
				WbRS_HilbertRTreeInh.create(
						branchingParam, 
						leafParam, 
						samplesPerNodeLo, samplesPerNodeHi, 
						bitsPerDimensions.length, 
						universe, 
						getBoundingBox, getSFCKey, 
						nDuplicatesAllowed, splitPolicy);
		
		//-- set the PRNG state
		tree.setRNG(new CopyableRandom(rng));
		//-- Initialization with container creation inside the tree
//		tree.initialize_buildContainer(treeContainer, valueConverter);
		//-- Build own Container outside the tree
		HilbertRTreeSA<FixedPointRectangle, Long>.NodeConverter nodeConverter = tree.new NodeConverter(valueConverter);
		Container nodeContainerUnbufferedUntyped = new ConverterContainer(treeContainer, nodeConverter);
		Container nodeContainerUntyped = NODE_BUFFER_SIZE > 0 ? 
				new BufferedContainer(nodeContainerUnbufferedUntyped, new LRUBuffer(NODE_BUFFER_SIZE))
			  : nodeContainerUnbufferedUntyped;
		TypeSafeContainer<Long, HilbertRTreeSA<FixedPointRectangle, Long>.Node> nodeContainer = new CastingContainer<Long, HilbertRTreeSA<FixedPointRectangle, Long>.Node>(nodeContainerUntyped);
		tree.initialize_withReadyContainer(nodeContainer);
		
		System.out.println("Initialization of the tree finished.");
		return tree;		
	}
	
//	public static WbRS_HilbertRTreeInh<FixedPointRectangle, Long> createWbHilbertRSTree_old(
//				String testFile, int BLOCK_SIZE, int branchingParam, int leafParam, CopyableRandom rng, int[] bitsPerDimensions) throws FileNotFoundException {
//			
//			final int IO_BUFFER_SIZE = 10; final int NODE_BUFFER_SIZE = 0;
//			// int[] bitsPerDimensions = {4,4,4};
//			int dimension = bitsPerDimensions.length;
//			int splitPolicy = 1;
//			
//			//======= INITIALISATION
//			Container treeRawContainer = new BlockFileContainer(TestUtils.resolveFilename(testFile), BLOCK_SIZE);
//			Container treeContainer = IO_BUFFER_SIZE > 0 ? 
//											new BufferedContainer(treeRawContainer, new LRUBuffer(IO_BUFFER_SIZE))
//										  : treeRawContainer;
//			
//			MeasuredConverter<Interval<Long>> hvRangeConverter = 
//					new MeasuredFixedSizeConverter<Interval<Long>>(Interval.getConverter(LongConverter.DEFAULT_INSTANCE));
//			
//			Supplier<FixedPointRectangle> fixedPointRectangleFactory = () -> new FixedPointRectangle(dimension);
//			MeasuredConverter<FixedPointRectangle> areaConverter = 
//					Converters.createMeasuredConverter(dimension * 2 * LongConverter.SIZE, 
//							new ConvertableConverter<FixedPointRectangle>(FunJ8.toOld(fixedPointRectangleFactory))); // TODO: how to make it fixed size?
//			MeasuredConverter<FixedPointRectangle> valueConverter = areaConverter; // our values are the areas
//			
//			//-- estimating parameters for the tree
//			//- fill leafes optimal
//			int leafHi = (BLOCK_SIZE - BooleanConverter.SIZE - IntegerConverter.SIZE) / valueConverter.getMaxObjectSize();
//			int leafLo = (int) Math.ceil((double)leafHi / 4.0);
//			
//			//- allow just as many duplicates as fit in a leaf
//			int nDuplicatesAllowed = leafHi;
//			
//			//- set branching param fixed
//			int branchingHi = branchingHiWish;
//			int branchingLo = branchingLoWish;
//			
//			//- determine how much is left for samples
//			int innerSpaceLeft = BLOCK_SIZE;
//			innerSpaceLeft -= BooleanConverter.SIZE; // node type indicator
//			innerSpaceLeft -= IntegerConverter.SIZE; // amount of child nodes
//			innerSpaceLeft -= hvRangeConverter.getMaxObjectSize() * branchingHi; 	// hilbert value ranges of children
//			innerSpaceLeft -= areaConverter.getMaxObjectSize() * branchingHi; 		// area ranges of children
//			innerSpaceLeft -= treeContainer.objectIdConverter().getSerializedSize() * branchingHi; // childCIDs 
//			innerSpaceLeft -= IntegerConverter.SIZE * branchingHi; // weights
//			
//			innerSpaceLeft -= IntegerConverter.SIZE; // amount of samples present
//			//- set sample param for the remaining space optimal
//			int samplesPerNodeHi = innerSpaceLeft / valueConverter.getMaxObjectSize();
//			int samplesPerNodeLo = samplesPerNodeHi / 4;		
//	
//			//== bounding box computer
//			Function<FixedPointRectangle, FixedPointRectangle> getBoundingBox = (x -> x);
//			
//			//== space filling curve		
//			FixedPointRectangle universe = DataDistributions.universeForBitsPerDimensions(bitsPerDimensions);
//			CompactHilbertCurve hilbertCurve = new CompactHilbertCurve(bitsPerDimensions);
//			
//			Function<FixedPointRectangle, Long> getSFCKey = new Function<FixedPointRectangle, Long>() {
//				// OPT: don't repeat input-/output-var allocation
//				@Override
//				public Long apply(FixedPointRectangle t) {
//					long[] center = SpatialHelpers.centralPoint(t);
//					
//					//- initialise input variables 
//					BitVector[] p = new BitVector[bitsPerDimensions.length];
//					for(int i = 0; i < bitsPerDimensions.length; i++) {
//			        	p[i] = BitVectorFactories.OPTIMAL.apply(bitsPerDimensions[i]);
//			        	p[i].copyFrom(center[i]);
//					}
//					
//					//- initialise output variables
//					BitVector chi = BitVectorFactories.OPTIMAL.apply(hilbertCurve.getSpec().sumBitsPerDimension());
//					
//					hilbertCurve.index(p, 0, chi);
//					return chi.toExactLong();
//				}
//			};
//			
//			//-- tree construction
//			System.out.println("Initializing tree with parameters: ");
//			System.out.println("\t block size: \t"+ BLOCK_SIZE);
//			System.out.println("\t branching: \t"+ branchingLo +" - "+ branchingHi);
//			System.out.println("\t leafentries: \t"+ leafLo +" - "+ leafHi);
//			System.out.println("\t samples: \t"+ samplesPerNodeLo +" - "+ samplesPerNodeHi);
//			System.out.println("\t number of duplicates allowed: \t"+ nDuplicatesAllowed);
//			System.out.println("\t splitPoliy: \t"+ splitPolicy +" -> "+ (splitPolicy+1));
//	
//			WbRS_HilbertRTreeInh<FixedPointRectangle, Long> tree = 
//					WbRS_HilbertRTreeInh.<FixedPointRectangle, Long>create(
//							branchingLo, branchingHi, 
//							leafLo, leafHi, 
//							samplesPerNodeLo, samplesPerNodeHi,
//							dimension,
//							universe,
//							getBoundingBox,
//							getSFCKey, 
//							nDuplicatesAllowed,
//							splitPolicy
//							);
//			
//			//-- set the PRNG state
//			tree.setRNG(new CopyableRandom(rng));
//			//-- Initialization with container creation inside the tree
//	//		tree.initialize_buildContainer(treeContainer, valueConverter);
//			//-- Build own Container outside the tree
//			HilbertRTreeSA<FixedPointRectangle, Long>.NodeConverter nodeConverter = tree.new NodeConverter(valueConverter);
//			Container nodeContainerUnbufferedUntyped = new ConverterContainer(treeContainer, nodeConverter);
//			Container nodeContainerUntyped = NODE_BUFFER_SIZE > 0 ? 
//					new BufferedContainer(nodeContainerUnbufferedUntyped, new LRUBuffer(NODE_BUFFER_SIZE))
//				  : nodeContainerUnbufferedUntyped;
//			TypeSafeContainer<Long, HilbertRTreeSA<FixedPointRectangle, Long>.Node> nodeContainer = new CastingContainer<Long, HilbertRTreeSA<FixedPointRectangle, Long>.Node>(nodeContainerUntyped);
//			tree.initialize_withReadyContainer(nodeContainer);
//			
//			System.out.println("Initialization of the tree finished.");
//			return tree;
//		}

	public static HilbertRTreeSA<FixedPointRectangle, Long> createHilbertRSTree(
			String testFile, int BLOCK_SIZE, int branchingLoWish, int branchingHiWish, CopyableRandom rng, int[] bitsPerDimensions, int splitPolicy) throws FileNotFoundException {
		
		final int IO_BUFFER_SIZE = 10; final int NODE_BUFFER_SIZE = 0;
		// int[] bitsPerDimensions = {4,4,4};
		int dimension = bitsPerDimensions.length;
//		int splitPolicy = 1;
		
		//======= INITIALISATION
		Container treeRawContainer = new BlockFileContainer(TestUtils.resolveFilename(testFile), BLOCK_SIZE);
		Container treeContainer = IO_BUFFER_SIZE > 0 ? 
										new BufferedContainer(treeRawContainer, new LRUBuffer(IO_BUFFER_SIZE))
									  : treeRawContainer;
		
		MeasuredConverter<Interval<Long>> hvRangeConverter = 
				new MeasuredFixedSizeConverter<Interval<Long>>(Interval.getConverter(LongConverter.DEFAULT_INSTANCE));
		
		Supplier<FixedPointRectangle> fixedPointRectangleFactory = () -> new FixedPointRectangle(dimension);
		MeasuredConverter<FixedPointRectangle> areaConverter = 
				Converters.createMeasuredConverter(dimension * 2 * LongConverter.SIZE, 
						new ConvertableConverter<FixedPointRectangle>(FunJ8.toOld(fixedPointRectangleFactory))); // TODO: how to make it fixed size?
		MeasuredConverter<FixedPointRectangle> valueConverter = areaConverter; // our values are the areas
		
		//-- estimating parameters for the tree
		//- fill leafes optimal
		int leafHi = (BLOCK_SIZE - BooleanConverter.SIZE - IntegerConverter.SIZE) / valueConverter.getMaxObjectSize();
		int leafLo = (int) Math.ceil((double)leafHi / 4.0);
		
		//- allow just as many duplicates as fit in a leaf
		int nDuplicatesAllowed = leafHi;
		
		//- set branching param fixed
		int branchingHi = branchingHiWish;
		int branchingLo = branchingLoWish;
		
		//- determine how much is left for samples
		int innerSpaceLeft = BLOCK_SIZE;
		innerSpaceLeft -= BooleanConverter.SIZE; // node type indicator
		innerSpaceLeft -= IntegerConverter.SIZE; // amount of child nodes
		innerSpaceLeft -= hvRangeConverter.getMaxObjectSize() * branchingHi; 	// hilbert value ranges of children
		innerSpaceLeft -= areaConverter.getMaxObjectSize() * branchingHi; 		// area ranges of children
		innerSpaceLeft -= treeContainer.objectIdConverter().getSerializedSize() * branchingHi; // childCIDs 
		innerSpaceLeft -= IntegerConverter.SIZE * branchingHi; // weights
		
		innerSpaceLeft -= IntegerConverter.SIZE; // amount of samples present
		//- set sample param for the remaining space optimal
		int samplesPerNodeHi = innerSpaceLeft / valueConverter.getMaxObjectSize();
		int samplesPerNodeLo = samplesPerNodeHi / 4;		

		//== bounding box computer
		Function<FixedPointRectangle, FixedPointRectangle> getBoundingBox = (x -> x);
		
		//== space filling curve		
		FixedPointRectangle universe = DataDistributions.universeForBitsPerDimensions(bitsPerDimensions);
		CompactHilbertCurve hilbertCurve = new CompactHilbertCurve(bitsPerDimensions);
		
		Function<FixedPointRectangle, Long> getSFCKey = new Function<FixedPointRectangle, Long>() {
			// OPT: don't repeat input-/output-var allocation
			@Override
			public Long apply(FixedPointRectangle t) {
				long[] center = SpatialHelpers.centralPoint(t);
				
				//- initialise input variables 
				BitVector[] p = new BitVector[bitsPerDimensions.length];
				for(int i = 0; i < bitsPerDimensions.length; i++) {
		        	p[i] = BitVectorFactories.OPTIMAL.apply(bitsPerDimensions[i]);
		        	p[i].copyFrom(center[i]);
				}
				
				//- initialise output variables
				BitVector chi = BitVectorFactories.OPTIMAL.apply(hilbertCurve.getSpec().sumBitsPerDimension());
				
				hilbertCurve.index(p, 0, chi);
				return chi.toExactLong();
			}
		};
		
		//-- tree construction
		System.out.println("Initializing tree with parameters: ");
		System.out.println("\t block size: \t"+ BLOCK_SIZE);
		System.out.println("\t branching: \t"+ branchingLo +" - "+ branchingHi);
		System.out.println("\t leafentries: \t"+ leafLo +" - "+ leafHi);
		System.out.println("\t samples: \t"+ samplesPerNodeLo +" - "+ samplesPerNodeHi);
		System.out.println("\t number of duplicates allowed: \t"+ nDuplicatesAllowed);
		System.out.println("\t splitPolicy: \t"+ splitPolicy +" -> "+ (splitPolicy+1));

		HilbertRTreeSA<FixedPointRectangle, Long> tree = 
				new HilbertRTreeSA<FixedPointRectangle, Long>(
						branchingLo, branchingHi, 
						leafLo, leafHi, 
						samplesPerNodeLo, samplesPerNodeHi,
						dimension,
						universe,
						getBoundingBox,
						getSFCKey, 
						nDuplicatesAllowed,
						splitPolicy
						);
		
		//-- set the PRNG state
		tree.setRNG(new CopyableRandom(rng));
		//-- Initialization with container creation inside the tree
//		tree.initialize_buildContainer(treeContainer, valueConverter);
		//-- Build own Container outside the tree
		HilbertRTreeSA<FixedPointRectangle, Long>.NodeConverter nodeConverter = tree.new NodeConverter(valueConverter);
		Container nodeContainerUnbufferedUntyped = new ConverterContainer(treeContainer, nodeConverter);
		Container nodeContainerUntyped = NODE_BUFFER_SIZE > 0 ? 
				new BufferedContainer(nodeContainerUnbufferedUntyped, new LRUBuffer(NODE_BUFFER_SIZE))
			  : nodeContainerUnbufferedUntyped;
		TypeSafeContainer<Long, HilbertRTreeSA<FixedPointRectangle, Long>.Node> nodeContainer = new CastingContainer<Long, HilbertRTreeSA<FixedPointRectangle, Long>.Node>(nodeContainerUntyped);
		tree.initialize_withReadyContainer(nodeContainer);
		
		System.out.println("Initialization of the tree finished.");
		return tree;
	}
//	public static HilbertRTreeSA<FixedPointRectangle, Long> createHilbertRSTree_old(
//			String testFile, int BLOCK_SIZE, int branchingLoWish, int branchingHiWish, CopyableRandom rng, int nDuplicatesAllowed) {
//		
//		Container treeRawContainer = new BlockFileContainer(testFile, BLOCK_SIZE);
//		
//		int dimension = 3;
//		
//		MeasuredConverter<Interval<Long>> hvRangeConverter = 
//				new MeasuredFixedSizeConverter<Interval<Long>>(Interval.getConverter(LongConverter.DEFAULT_INSTANCE));
//		MeasuredConverter<FixedPointRectangle> areaConverter = 
//				Converters.createMeasuredConverter(dimension * 2 * LongConverter.SIZE, new ConvertableConverter<FixedPointRectangle>()); // TODO: how to make it fixed size?
//		MeasuredConverter<FixedPointRectangle> valueConverter = areaConverter;
//		
//		//-- estimating parameters for the tree
//		//- fill leafes optimal
//		int leafHi = (BLOCK_SIZE - BooleanConverter.SIZE - IntegerConverter.SIZE) / valueConverter.getMaxObjectSize();
//		int leafLo = (int) Math.ceil((double)leafHi / 4.0);
//		
//		//- set branching param fixed
//		int branchingHi = branchingHiWish;
//		int branchingLo = branchingLoWish;
//		
//		//- determine how much is left for samples
//		int innerSpaceLeft = BLOCK_SIZE;
//		innerSpaceLeft -= BooleanConverter.SIZE; // node type indicator
//		innerSpaceLeft -= IntegerConverter.SIZE; // amount of child nodes
//		innerSpaceLeft -= hvRangeConverter.getMaxObjectSize() * branchingHi; 	// hilbert value ranges of children
//		innerSpaceLeft -= areaConverter.getMaxObjectSize() * branchingHi; 		// area ranges of children
//		innerSpaceLeft -= treeRawContainer.objectIdConverter().getSerializedSize() * branchingHi; // childCIDs 
//		innerSpaceLeft -= IntegerConverter.SIZE * branchingHi; // weights
//		
//		innerSpaceLeft -= IntegerConverter.SIZE; // amount of samples present
//		//- set sample param for the remaining space optimal
//		int samplesPerNodeHi = innerSpaceLeft / valueConverter.getMaxObjectSize();
//		int samplesPerNodeLo = samplesPerNodeHi / 4;		
//		
////		//-- compute optimal parameters
////		leafHi, samplesHi = inferTreeParameters(BLOCK_SIZE, branchingLoWish, branchingHiWish, 
////				leafOverhead, bytesPerValue, innerOverhead, bytesPerMeta, bytesPerCID)
//		
//		//-- tree construction
//		System.out.println("Initializing tree with parameters: ");
//		System.out.println("\t block size: \t"+ BLOCK_SIZE);
//		System.out.println("\t branching: \t"+ branchingLo +" - "+ branchingHi);
//		System.out.println("\t leafentries: \t"+ leafLo +" - "+ leafHi);
//		System.out.println("\t samples: \t"+ samplesPerNodeLo +" - "+ samplesPerNodeHi);
//
//		HilbertRTreeSA<FixedPointRectangle, Long> tree = 
//				new HilbertRTreeSA<FixedPointRectangle, Long>(
//						branchingLo, branchingHi, 
//						leafLo, leafHi, 
//						samplesPerNodeLo, samplesPerNodeHi, 
//						universe, 
//						getSFCKey, 
//						nDuplicatesAllowed
//						);
//		
//		//-- set the PRNG state
//		tree.setRNG(rng);
//		//-- Initialization with container creation inside the tree
//		tree.initialize_buildContainer(treeRawContainer, valueConverter);		
//		
//		System.out.println("Initialization of the tree finished.");
//		return tree;
//	}

//	public static void createAndSave_RSTree_pairsIntDouble(
//			String metaDataFilename, String containerPrefix, int nTuples, CopyableRandom random,
//			int KEY_LO, int KEY_HI, double VAL_LO, double VAL_HI) throws IOException {
//		RSTree1D<Integer, Pair<Integer, Double>, Long> tree = createRSTree(containerPrefix, 2048, 5, 20);
//		Cursor<Pair<Integer, Double>> dataCursor = DataDistributions.data_squarePairs(random, KEY_LO, KEY_HI, VAL_LO, VAL_HI);
//		NavigableMap<Integer, List<Pair<Integer, Double>>> compmap = TreeCreation.fillTestableMap(tree, nTuples, dataCursor, (t -> t.getElement1()));
//		
//		Converter<Integer> keyConverter = IntegerConverter.DEFAULT_INSTANCE;
//		Converter<Pair<Integer, Double>> valueConverter = new PairConverterFixedSized<Integer, Double>(IntegerConverter.DEFAULT_INSTANCE, DoubleConverter.DEFAULT_INSTANCE);
//		
//		tree.writeToMetaData(metaDataFilename, containerPrefix, keyConverter, valueConverter);
//		
//		System.out.println("-- Tree successfully written to metadata-file: \""+ metaDataFilename +"\"");
//	}
//
//	public static TestableMap<K, V> load_RSTree_pairsIntDouble(String metaDataFilename) throws IOException {
//		Converter<Integer> keyConverter = IntegerConverter.DEFAULT_INSTANCE;
//		Converter<Pair<Integer, Double>> valueConverter = new PairConverterFixedSized<Integer, Double>(IntegerConverter.DEFAULT_INSTANCE, DoubleConverter.DEFAULT_INSTANCE);
//		Function<String, Container> containerFactory = (s -> new BlockFileContainer(s));
//		Function<Pair<Integer, Double>, Integer> getKey = ((Pair<Integer, Double> x) -> x.getFirst());
//		
//		TestableMap<K, V> tree = RSTree1D.loadFromMetaData(
//				metaDataFilename, 
//				containerFactory, 
//				keyConverter, 
//				valueConverter, 
//				getKey);
//		
//		System.out.println("-- Tree successfully loaded from metadata-file: \""+ metaDataFilename +"\"");
//		
//		return tree;
//	}

}
