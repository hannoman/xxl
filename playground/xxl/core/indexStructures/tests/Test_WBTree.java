package xxl.core.indexStructures.tests;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.indexStructures.old.WBTree;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.util.HUtil;
import xxl.core.util.Pair;
import xxl.core.util.Triple;

public class Test_WBTree {

	public static final int BLOCK_SIZE = 1024;
//	public static final float MIN_RATIO = 0.5f;
//	public static final int BUFFER_SIZE = 10;
//	public static final int NUMBER_OF_BITS = 256;
//	public static final int MAX_OBJECT_SIZE = 78;
	public static final int NUMBER_OF_ELEMENTS = 10000;

	// shared state of the RNG
	public static Random random;
	
	
	private static WBTree<Integer, Integer, Long> createTree(String testFile) {
		
		WBTree<Integer, Integer, Long> tree = new WBTree<Integer, Integer, Long>(
				10, 										// leafParam
				5, 											// branchingParam
				(x -> x)); 									// getKey

		Converter<Integer> keyConverter = IntegerConverter.DEFAULT_INSTANCE;
		Converter<Integer> valueConverter = IntegerConverter.DEFAULT_INSTANCE;
		
		Container treeRawContainer = new BlockFileContainer(testFile, BLOCK_SIZE);			
		
		//-- Initialization with externally built Container
//		Converter<WBTree<Integer, Integer, Long>.Node> nodeConverter = 
//				tree.new NodeConverter(keyConverter, valueConverter, treeRawContainer.objectIdConverter());
//		
//		TypeSafeContainer<Long, WBTree<Integer, Integer, Long>.Node> treeContainer = 
//				new CastingContainer<Long, WBTree<Integer, Integer, Long>.Node>(
//					new ConverterContainer(treeRawContainer, nodeConverter)
//				); 
//		
//		tree.initialize_withReadyContainer(treeContainer);

		//-- Initialization with container creation inside the tree
		tree.initialize_buildContainer(treeRawContainer, keyConverter, valueConverter);		
		
		System.out.println("Initialization of the tree finished.");

		return tree;
	}

	public static void suite1(WBTree<Integer, Integer, Long> tree) {
		Map<Integer, Integer> compmap = fill(tree, NUMBER_OF_ELEMENTS);
		random = new Random(55);
		positiveLookups(tree, compmap, NUMBER_OF_ELEMENTS / 3);
		randomKeyLookups(tree, compmap, NUMBER_OF_ELEMENTS / 2);		
		rangeQueries(tree, compmap, NUMBER_OF_ELEMENTS / 20);
	}
	
	public static void suite2(WBTree<Integer, Integer, Long> tree) {
		Map<Integer, Integer> compmap = fill(tree, NUMBER_OF_ELEMENTS);
		debugRangeQueries(tree);
	}
	
	public static Map<Integer,Integer> fill(WBTree<Integer, Integer, Long> tree, int amount) {
		//-- comparison structure
		TreeMap<Integer, Integer> compmap = new TreeMap<Integer, Integer>();
		
		//-- Insertion - generate test data
		Random random = new Random(139);
		System.out.println("-- Insertion test: Generating random test data");

		for (int i = 1; i <= amount; i++) {
			int value = random.nextInt();
			tree.insert(value);
			compmap.put(tree.getKey.apply(value), value);
			if (i % (amount / 10) == 0)
				System.out.print((i / (amount / 100)) + "%, ");
		}
		
		System.out.println("Resulting tree height: " + tree.rootHeight);

		return compmap;
	}
	
	public static int positiveLookups(WBTree<Integer, Integer, Long> tree, Map<Integer,Integer> compmap, int LOOKUP_TESTS_POSITIVE) {
		// final int LOOKUP_TESTS_POSITIVE = NUMBER_OF_ELEMENTS / 3;
		
		System.out.println("-- Positive Lookups (perhaps duplicate) (#="+ LOOKUP_TESTS_POSITIVE +"):");		
		//--- Lookup test
		//-- positive lookups		
		List<Integer> containedKeys = new ArrayList<Integer>(compmap.keySet());
		int errors_positiveLookup = 0;
		for(int i=1; i <= LOOKUP_TESTS_POSITIVE; i++) {
			int keyNr = random.nextInt(containedKeys.size());
			Integer key = containedKeys.get(keyNr);
			
			List<Integer> treeAnswers = tree.get(key);
			Integer mapAnswer = compmap.get(key);
			if(!treeAnswers.contains(mapAnswer)) {
				System.out.println("Didn't find value \""+ mapAnswer +"\" for key \""+ key +"\". Only: "+ treeAnswers.toString());
				errors_positiveLookup++;
			} 
			if(treeAnswers.size() > 1) {
				System.out.println("Found multiple values for key \""+ key +"\": "+ treeAnswers.toString());
			}
		}		
		
		// System.out.println("Out of "+ LOOKUP_TESTS_POSITIVE +" (perhaps duplicate) positive lookups, failed on "+ errors_positiveLookup +" occasions.");		
		System.out.println("\tfailed: "+ errors_positiveLookup);
		
		return errors_positiveLookup;
	}
	
	public static int randomKeyLookups(WBTree<Integer, Integer, Long> tree, Map<Integer,Integer> compmap, int LOOKUP_TESTS_RANDOM) {
		System.out.println("-- Random Lookups from domain (mostly negative) (#="+ LOOKUP_TESTS_RANDOM +"):");
		
		List<Integer> containedKeys = new ArrayList<Integer>(compmap.keySet());
		//-- (mostly) negative (= random) lookups		
		int errors_randomLookup = 0;
		for(int i=1; i < LOOKUP_TESTS_RANDOM; i++) {
			Integer key = random.nextInt();
			
			List<Integer> treeAnswers = tree.get(key);
			if(!treeAnswers.isEmpty() && !containedKeys.contains(key)) {
				System.out.println("False positive: \""+ key +"\"");
				errors_randomLookup++;
			}
			else if(treeAnswers.isEmpty() && containedKeys.contains(key)) {
				System.out.println("False negative: \""+ key +"\"");
				errors_randomLookup++;
			}
		}
//		System.out.println("Out of "+ LOOKUP_TESTS_RANDOM +" random lookups, failed on "+ errors_randomLookup +" occasions.");		
		System.out.println("\tfailed: "+ errors_randomLookup);
		
		return errors_randomLookup;
	}
	
	public static void testTree(WBTree<Integer, Integer, Long> tree) throws IOException {			
		//-- comparison structure
		TreeMap<Integer, Integer> compmap = new TreeMap<Integer, Integer>();
		
		//-- Insertion - generate test data
		// Random random = new Random(139);
		System.out.println("-- Insertion test: Generating random test data");

		for (int i = 1; i <= NUMBER_OF_ELEMENTS; i++) {
			int value = random.nextInt();
			tree.insert(value);
			compmap.put(tree.getKey.apply(value), value);
			if (i % (NUMBER_OF_ELEMENTS / 10) == 0)
				System.out.print((i / (NUMBER_OF_ELEMENTS / 100)) + "%, ");
		}		
		System.out.println("Resulting tree height: " + tree.rootHeight);

		//--- Lookup test
		//-- positive lookups
		
		final int LOOKUP_TESTS_POSITIVE = NUMBER_OF_ELEMENTS / 3;
		List<Integer> containedKeys = new ArrayList<Integer>(compmap.keySet());
		int errors_positiveLookup = 0;
		for(int i=1; i <= LOOKUP_TESTS_POSITIVE; i++) {
			int keyNr = random.nextInt(containedKeys.size());
			Integer key = containedKeys.get(keyNr);
			
			List<Integer> treeAnswers = tree.get(key);
			Integer mapAnswer = compmap.get(key);
			if(!treeAnswers.contains(mapAnswer)) {
				System.out.println("Didn't find value \""+ mapAnswer +"\" for key \""+ key +"\". Only: "+ treeAnswers.toString());
				errors_positiveLookup++;
			} 
			if(treeAnswers.size() > 1) {
				System.out.println("Found multiple values for key \""+ key +"\": "+ treeAnswers.toString());
			}
		}		
		System.out.println("Out of "+ LOOKUP_TESTS_POSITIVE +" (perhaps duplicate) positive lookups, failed on "+ errors_positiveLookup +" occasions.");
		
		//-- (mostly) negative (= random) lookups
		final int LOOKUP_TESTS_RANDOM = NUMBER_OF_ELEMENTS / 2;
		int errors_randomLookup = 0;
		for(int i=1; i < LOOKUP_TESTS_RANDOM; i++) {
			Integer key = random.nextInt();
			
			List<Integer> treeAnswers = tree.get(key);
			if(!treeAnswers.isEmpty() && !containedKeys.contains(key)) {
				System.out.println("False positive: \""+ key +"\"");
				errors_randomLookup++;
			}
			else if(treeAnswers.isEmpty() && containedKeys.contains(key)) {
				System.out.println("False negative: \""+ key +"\"");
				errors_randomLookup++;
			}
		}
		System.out.println("Out of "+ LOOKUP_TESTS_RANDOM +" random lookups, failed on "+ errors_randomLookup +" occasions.");

		/* Old code: not used as we still miss the required functionality to test. ^^ */
//		//-- Delete test
//		random = new Random(42);
//		System.out.println("-- Remove Test:");
//		int error = 0;
//		for (int i = 0; i < 10; i++) {
//			BigInteger rem = new BigInteger(NUMBER_OF_BITS, random).negate();
//			BigInteger fou = (BigInteger) tree.remove(rem);
//			if (fou != null)
//				error++;
//		}
//		if (error > 0)
//			System.err.println("false positive: " + error);
//		error = 0;
//		for (int i = 0; i < NUMBER_OF_ELEMENTS * .1 - 10; i++) {
//			BigInteger rem = new BigInteger(NUMBER_OF_BITS, random);
//			BigInteger fou = (BigInteger) tree.remove(rem);
//			if (!rem.equals(fou))
//				error++;
//		}
//		if (error > 0)
//			System.err.println("false negative: " + error);
//
//		//-- Query test
//		BPlusTree.KeyRange region = (BPlusTree.KeyRange) tree.rootDescriptor();
//		BigInteger min = (BigInteger) region.minBound();
//		BigInteger max = (BigInteger) region.maxBound();
//		BigInteger temp = max.subtract(min).divide(new BigInteger("10"));
//		System.out.println("-- Query Test:");
//		BigInteger minQR = min.add(temp);
//		BigInteger maxQR = minQR.add(temp);
//		System.out.println("Query: [" + minQR + ", " + maxQR + "]");
//		Iterator<?> results = tree.rangeQuery(minQR, maxQR);
//		System.out.println("Responses number: " + Cursors.count(results));
//		System.out.println("End.");
		
	}

	public static void main(String[] args) throws Exception {
		random = new Random(139);
		
		//--- find a nice filename
		String fileName;
		if (args.length > 0) { // custom container file
			fileName = args[0];
		} else { // std container file
			String CONTAINER_FILE_PREFIX = "wb_tree_test01";
			String test_data_dirname = "temp_data";
			System.out.println("No filename as program parameter found. Using standard: \"" + "<project dir>\\" + test_data_dirname + "\\"
					+ CONTAINER_FILE_PREFIX + "\"");

			// and the whole thing in short
			Path curpath = Paths.get("").toAbsolutePath();
			if (!curpath.resolve(test_data_dirname).toFile().exists()) {
				System.out.println("Error: Couldn't find \"" + test_data_dirname + "\" directory.");
				return;
			}
			fileName = curpath.resolve("temp_data").resolve(CONTAINER_FILE_PREFIX).toString();
			System.out.println("resolved to: \"" + fileName + "\".");

		}

		//--- run the actual tests
		
//		WBTree<Integer, Integer, Long> wbTree = createTree(fileName);		
//		testTree(wbTree);
		
		WBTree<Integer, Integer, Long> wbTree = createTree(fileName);		
		suite1(wbTree);
//		suite2(wbTree);
	}

	public static Triple<Integer, Integer, Integer> rangeQueries(WBTree<Integer, Integer, Long> tree, Map<Integer,Integer> compmap, int RANGE_QUERY_TESTS) {
		// final int RANGE_QUERY_TESTS = 1;
		//-- rangeQuery tests
		System.out.println("-- RangeQuery Tests (#="+ RANGE_QUERY_TESTS +"):");
		
		ArrayList<Integer> containedKeys = new ArrayList<Integer>(compmap.keySet());
		containedKeys.sort(null);
		
		int error_false_positive = 0;
		int error_false_negative = 0;
		int error_both = 0;
		
		for(int i=1; i <= RANGE_QUERY_TESTS; i++) {
			Integer lo = random.nextInt();
			Integer hi = random.nextInt();
			if(lo > hi) { int tmp = lo; lo = hi; hi = tmp; }
			long possKeys = (long)hi - (long)lo + 1;
			
//			System.out.println("Range Query #"+ i +": "+ lo +" - "+ hi +" (#possKeys: "+ possKeys +"): ");
	
			//-- execute the query
			Cursor<Integer> cur = tree.rangeQuery(lo, hi);			
			List<Integer> tRes = new ArrayList<Integer>(Cursors.toList(cur));
			
//			System.out.println("T-result (#="+ tRes.size() +"): "+ tRes);
			
			//-- Test current query
			int e_negatives = 0;
			int e_positives = 0;
			
			//-- Tests for false positives
			for(Integer tVal : tRes)
				if(!compmap.containsKey(tVal)) e_positives++;
	
			//--- Computing the comparison-result
			int compLoIdx = HUtil.binFindES(containedKeys, lo);
			int compHiIdx = HUtil.binFindES(containedKeys, hi);
			while(containedKeys.get(compHiIdx) == hi) compHiIdx++; // skip duplicates
			List<Integer> cRes = containedKeys.subList(compLoIdx, compHiIdx);
			
//			System.out.println("C-result (#="+ cRes.size() +"): "+ cRes);			
			
			//-- Test for false negatives
			for(Integer cVal : cRes)
				if(Collections.binarySearch(tRes, cVal) < 0) e_negatives++;		
	
			//- classify error case
			if(e_negatives > 0 || e_positives > 0) {
				System.out.println("\tErronous: #rsize: "+ tRes.size() +"; #compsize: "+ cRes.size() +"; #missing: "+ e_negatives +"; #too much: "+ e_positives);
				if(e_negatives > 0 && e_positives > 0) error_both++;
				else if(e_negatives > 0) error_false_negative++;
				else if(e_positives > 0) error_false_positive++;
			}
			
		}		
		
		System.out.println("\tToo big results:   "+ error_false_positive);
		System.out.println("\tToo small results: "+ error_false_negative);
		System.out.println("\tBoth sided errors: "+ error_both);
		return new Triple<Integer, Integer, Integer>(error_false_positive, error_false_negative, error_both);
	}

	/**
	 * For debugging purposes: creates a tree with consecutive keys {1, ..., AMOUNT} 
	 */
	public static Triple<Integer, Integer, Integer> debugRangeQueries(WBTree<Integer, Integer, Long> tree) {
		TreeMap<Integer, Integer> compmap = new TreeMap<Integer, Integer>();
		
		//-- Insertion - generate test data
		System.out.println("-- Insertion test: Generating random test data");
		
		int AMOUNT = 1000;
		int domLo = 1; int domHi = AMOUNT;
		for (int i = 1; i <= AMOUNT; i++) {
			int value = i;
			tree.insert(value);
			compmap.put(tree.getKey.apply(value), value);
			if (i % (AMOUNT / 10) == 0)
				System.out.print((i / (AMOUNT / 100)) + "%, ");
		}		
		System.out.println("Resulting tree height: " + tree.rootHeight);

	
		ArrayList<Integer> containedKeys = new ArrayList<Integer>(compmap.keySet());
		containedKeys.sort(null);
	
		//---- Testing		
		int error_false_positive = 0;
		int error_false_negative = 0;
		int error_both = 0;
		
		int RANGE_QUERY_TESTS = 20;
		for(int i=1; i <= RANGE_QUERY_TESTS; i++) {
			Integer lo = random.nextInt(domHi-domLo+1)+domLo;
			Integer hi = random.nextInt(domHi-domLo+1)+domLo;
			if(lo > hi) { int tmp = lo; lo = hi; hi = tmp; }
			long possKeys = (long)hi - (long)lo + 1;

			// System.out.println("Range Query #"+ i +": "+ lo +" - "+ hi +" (#possKeys: "+ possKeys +"): ");
			
			//-- execute the query
			Cursor<Integer> cur = tree.rangeQuery(lo, hi);			
			List<Integer> tRes = new ArrayList<Integer>(Cursors.toList(cur));
			
			// System.out.println("C-result (#="+ results.size() +"): "+ results);
			
			//-- Test current query
			int e_negatives = 0;
			int e_positives = 0;
			
			//-- Tests for false positives
			for(Integer tVal : tRes)
				if(!compmap.containsKey(tVal)) e_positives++;

			//--- Computing the comparison-result
			int compLoIdx = HUtil.binFindES(containedKeys, lo);
			int compHiIdx = HUtil.binFindES(containedKeys, hi);
			while(containedKeys.get(compHiIdx) == hi) compHiIdx++; // skip duplicates
			List<Integer> cRes = containedKeys.subList(compLoIdx, compHiIdx+1);
			
			// System.out.println("C-result (#="+ compRes.size() +"): "+ compRes);			
			
			//-- Test for false negatives
			for(Integer cVal : cRes) {
				int pos = Collections.binarySearch(tRes, cVal);
				if(pos < 0) // <==> not found
					e_negatives++;					
			}
			
			if(e_negatives > 0 || e_positives > 0) {
				System.out.println("\tErronous: #rsize: "+ tRes.size() +"; #compsize: "+ cRes.size() +"; #missing: "+ e_negatives +"; #too much: "+ e_positives);
				if(e_negatives > 0 && e_positives > 0) error_both++;
				else if(e_negatives > 0) error_false_negative++;
				else if(e_positives > 0) error_false_positive++;
			}
			
		}		
		
		System.out.println("Too big results:   "+ error_false_positive);
		System.out.println("Too small results: "+ error_false_negative);
		System.out.println("Both sided errors: "+ error_both);
		return new Triple<Integer, Integer, Integer>(error_false_positive, error_false_negative, error_both);
	}
}
