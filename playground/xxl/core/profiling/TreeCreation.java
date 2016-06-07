package xxl.core.profiling;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.cursors.Cursor;
import xxl.core.indexStructures.RSTree1D;
import xxl.core.indexStructures.TestableMap;
import xxl.core.indexStructures.WRSTree_copyImpl;
import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.DoubleConverter;
import xxl.core.io.converters.FixedSizeConverter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.util.CopyableRandom;
import xxl.core.util.Interval;
import xxl.core.util.Pair;
import xxl.core.util.PairConverterFixedSized;


public class TreeCreation {

	/** General fill method which just takes its values from a data generating cursor. 
	 * Returns a memory map for comparisons against the resulting data structure. */
	public static <K extends Comparable<K>, V> SortedMap<K, List<V>> fillTestableMap(
			TestableMap<K, V> tree, 
			int AMOUNT, 
			Cursor<V> dataCursor,
			Function<V, K> getKey
			) {
		//-- comparison structure
		TreeMap<K, List<V>> compmap = new TreeMap<K, List<V>>();
		
		//-- Insertion - generate test data		
		System.out.println("-- Insertion test: Generating "+ AMOUNT +" random test data points");
	
		for (int i = 1; i <= AMOUNT; i++) {					
			V value = dataCursor.next();
			K key = getKey.apply(value);
			tree.insert(value);
			compmap.putIfAbsent(key, new LinkedList<V>());
			compmap.get(key).add(value);  
			if (i % (AMOUNT / 10) == 0) {
				System.out.print((i / (AMOUNT / 100)) + "%, ");
				System.out.println("inserted: "+ value);
			}
		}
		
		System.out.println("Resulting tree height: " + tree.height());
		return compmap;
	}

	/** Creates a RSTree with fixed branching parameters and block size. Leaf and sample parameters are set space-optimal
	 * with respect to them. 

	 * Note: Does not set the PRNG-state of the tree.
	 */
	public static RSTree1D<Integer, Pair<Integer, Double>, Long> createRSTree(
			String testFile, int BLOCK_SIZE, int branchingParamLoWish, int branchingParamHiWish) {
		Container treeRawContainer = new BlockFileContainer(testFile, BLOCK_SIZE);
		
		FixedSizeConverter<Integer> keyConverter = IntegerConverter.DEFAULT_INSTANCE;		
		FixedSizeConverter<Pair<Integer,Double>> valueConverter = 
				new PairConverterFixedSized<Integer, Double>(IntegerConverter.DEFAULT_INSTANCE, DoubleConverter.DEFAULT_INSTANCE);
		FixedSizeConverter<Interval<Integer>> rangeConverter = Interval.getConverter(keyConverter);
		
		//-- estimating parameters for the tree
		//- fill leafes optimal
		int leafHi = (BLOCK_SIZE - BooleanConverter.SIZE - IntegerConverter.SIZE) / valueConverter.getSerializedSize();
		int leafLo = (int) Math.ceil((double)leafHi / 4.0);
		
		//- set branching param fixed
//		int branchingParamHi = 20;
//		int branchingParamLo = (int) Math.ceil((double)branchingParamHi / 4.0);
//		int branchingParamHi = 8;
//		int branchingParamLo = 4;
		int branchingParamHi = branchingParamHiWish;
		int branchingParamLo = branchingParamLoWish;
		
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
		System.out.println("\t samples: \t"+ samplesPerNodeLo +" - "+ samplesPerNodeHi);
		System.out.println("\t leafentries: \t"+ leafLo +" - "+ leafHi);

		RSTree1D<Integer, Pair<Integer, Double>, Long> tree = 
				new RSTree1D<Integer, Pair<Integer,Double>, Long>(
						new Interval<Integer>(Integer.MIN_VALUE, Integer.MAX_VALUE), // universe
						samplesPerNodeLo, 
						samplesPerNodeHi, 
						branchingParamLo, 
						branchingParamHi, 
						leafLo, 
						leafHi, 
						((Pair<Integer, Double> x) -> x.getFirst())
					);
				
		//-- Initialization with container creation inside the tree
		tree.initialize_buildContainer(treeRawContainer, keyConverter, valueConverter);		
		
		System.out.println("Initialization of the tree finished.");
		return tree;
	}

	public static WRSTree_copyImpl<Integer, Pair<Integer, Double>, Long> createWRSTree(
			String testFile, int BLOCK_SIZE, int branchingParamLoWish, int branchingParamHiWish) {
		Container treeRawContainer = new BlockFileContainer(testFile, BLOCK_SIZE);
		
		FixedSizeConverter<Integer> keyConverter = IntegerConverter.DEFAULT_INSTANCE;		
		FixedSizeConverter<Pair<Integer,Double>> valueConverter = 
				new PairConverterFixedSized<Integer, Double>(IntegerConverter.DEFAULT_INSTANCE, DoubleConverter.DEFAULT_INSTANCE);
		FixedSizeConverter<Interval<Integer>> rangeConverter = Interval.getConverter(keyConverter);
		
		//-- estimating parameters for the tree
		//- fill leafes optimal
		int leafHi = (BLOCK_SIZE - BooleanConverter.SIZE - IntegerConverter.SIZE) / valueConverter.getSerializedSize();
		int leafLo = (int) Math.ceil((double)leafHi / 4.0);
		
		//- set branching param fixed
//		int branchingParamHi = 20;
//		int branchingParamLo = (int) Math.ceil((double)branchingParamHi / 4.0);
//		int branchingParamHi = 8;
//		int branchingParamLo = 4;
		int branchingParamHi = branchingParamHiWish;
		int branchingParamLo = branchingParamLoWish;
		
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
		System.out.println("\t samples: \t"+ samplesPerNodeLo +" - "+ samplesPerNodeHi);
		System.out.println("\t leafentries: \t"+ leafLo +" - "+ leafHi);

		RSTree1D<Integer, Pair<Integer, Double>, Long> tree = 
				new RSTree1D<Integer, Pair<Integer,Double>, Long>(
						new Interval<Integer>(Integer.MIN_VALUE, Integer.MAX_VALUE), // universe
						samplesPerNodeLo, 
						samplesPerNodeHi, 
						branchingParamLo, 
						branchingParamHi, 
						leafLo, 
						leafHi, 
						((Pair<Integer, Double> x) -> x.getFirst())
					);
				
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
	public static RSTree1D<Integer, Pair<Integer, Double>, Long> createRSTree_withInnerUnbufferedNodes(
			String testFile, int BLOCK_SIZE, int branchingParamLoWish, int branchingParamHiWish) {
		Container treeRawContainer = new BlockFileContainer(testFile, BLOCK_SIZE);
		
		FixedSizeConverter<Integer> keyConverter = IntegerConverter.DEFAULT_INSTANCE;		
		FixedSizeConverter<Pair<Integer,Double>> valueConverter = 
				new PairConverterFixedSized<Integer, Double>(IntegerConverter.DEFAULT_INSTANCE, DoubleConverter.DEFAULT_INSTANCE);
		FixedSizeConverter<Interval<Integer>> rangeConverter = Interval.getConverter(keyConverter);
		
		//-- estimating parameters for the tree
		//- fill leafes optimal
//		int leafHi = (BLOCK_SIZE - BooleanConverter.SIZE - IntegerConverter.SIZE) / valueConverter.getSerializedSize();
		int leafHi = 10;
		int leafLo = (int) Math.ceil((double)leafHi / 4.0);
		
		//- set branching param fixed
//		int branchingParamHi = 5;
//		int branchingParamLo = (int) Math.ceil((double)branchingParamHi / 4.0);
		int branchingParamHi = branchingParamHiWish;
		int branchingParamLo = branchingParamLoWish;
		
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
		System.out.println("\t samples: \t"+ samplesPerNodeLo +" - "+ samplesPerNodeHi);
		System.out.println("\t leafentries: \t"+ leafLo +" - "+ leafHi);
	
		RSTree1D<Integer, Pair<Integer, Double>, Long> tree = 
				new RSTree1D<Integer, Pair<Integer,Double>, Long>(
						new Interval<Integer>(Integer.MIN_VALUE, Integer.MAX_VALUE), // universe
						samplesPerNodeLo, 
						samplesPerNodeHi, 
						branchingParamLo, 
						branchingParamHi, 
						leafLo, 
						leafHi, 
						((Pair<Integer, Double> x) -> x.getFirst())
					);
				
		//-- Initialization with container creation inside the tree
		tree.initialize_buildContainer(treeRawContainer, keyConverter, valueConverter);		
		
		System.out.println("Initialization of the tree finished.");
		return tree;
	}

	public static void createAndSave_RSTree_pairsIntDouble(
			String metaDataFilename, String containerPrefix, int nTuples, CopyableRandom random,
			int KEY_LO, int KEY_HI, double VAL_LO, double VAL_HI) throws IOException {
		RSTree1D<Integer, Pair<Integer, Double>, Long> tree = createRSTree(containerPrefix, 2048, 5, 20);
		Cursor<Pair<Integer, Double>> dataCursor = DataDistributions.data_squarePairs(random, KEY_LO, KEY_HI, VAL_LO, VAL_HI);
		Map<Integer, Pair<Integer, Double>> compmap = TreeCreation.fillTestableMap(tree, nTuples, dataCursor, (t -> t.getElement1()));
		
		Converter<Integer> keyConverter = IntegerConverter.DEFAULT_INSTANCE;
		Converter<Pair<Integer, Double>> valueConverter = new PairConverterFixedSized<Integer, Double>(IntegerConverter.DEFAULT_INSTANCE, DoubleConverter.DEFAULT_INSTANCE);
		
		tree.writeToMetaData(metaDataFilename, containerPrefix, keyConverter, valueConverter);
		
		System.out.println("-- Tree successfully written to metadata-file: \""+ metaDataFilename +"\"");
	}

	public static RSTree1D<Integer, Pair<Integer, Double>, Long> load_RSTree_pairsIntDouble(String metaDataFilename) throws IOException {
		Converter<Integer> keyConverter = IntegerConverter.DEFAULT_INSTANCE;
		Converter<Pair<Integer, Double>> valueConverter = new PairConverterFixedSized<Integer, Double>(IntegerConverter.DEFAULT_INSTANCE, DoubleConverter.DEFAULT_INSTANCE);
		Function<String, Container> containerFactory = (s -> new BlockFileContainer(s));
		Function<Pair<Integer, Double>, Integer> getKey = ((Pair<Integer, Double> x) -> x.getFirst());
		
		RSTree1D<Integer, Pair<Integer, Double>, Long> tree = RSTree1D.loadFromMetaData(
				metaDataFilename, 
				containerFactory, 
				keyConverter, 
				valueConverter, 
				getKey);
		
		System.out.println("-- Tree successfully loaded from metadata-file: \""+ metaDataFilename +"\"");
		
		return tree;
	}

}
