package xxl.core.indexStructures;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Taker;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.util.HUtil;
import xxl.core.util.Pair;
import xxl.core.util.QuickTime;
import xxl.core.util.Triple;


/**
 * Sanity checks for self-implemented maps (= trees). 
 * 
 * TODO: enable handling of duplicates in comparison map (change type from Map<K,V> to Map<K,Set<V>>)
 */
public class Test_TestableMap {

	public static final int BLOCK_SIZE = 1024;
//	public static final float MIN_RATIO = 0.5f;
//	public static final int BUFFER_SIZE = 10;
//	public static final int NUMBER_OF_BITS = 256;
//	public static final int MAX_OBJECT_SIZE = 78;
	public static final int NUMBER_OF_ELEMENTS = 10000;

	/** Shared state of the RNG. Instanciated Once. */  
	public static Random random = new Random(42);	
	
	private static WBTreeSA_v3<Integer, Integer, Long> createWBTree(String testFile) {
		
		WBTreeSA_v3<Integer, Integer, Long> tree = new WBTreeSA_v3<Integer, Integer, Long>(
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
	
	private static RSTree_v3<Integer, Integer, Long> createRSTree(String testFile) {
		
		RSTree_v3<Integer, Integer, Long> tree = new RSTree_v3<Integer, Integer, Long>(
				20, 											// samplesPerNode
				10, 											// branchingParam
				25, 											// leafLo
				100, 											// leafHi
				(x -> x));										// getKey

		Converter<Integer> keyConverter = IntegerConverter.DEFAULT_INSTANCE;
		Converter<Integer> valueConverter = IntegerConverter.DEFAULT_INSTANCE;
		
		Container treeRawContainer = new BlockFileContainer(testFile, BLOCK_SIZE);			
		
		//-- Initialization with container creation inside the tree
		tree.initialize_buildContainer(treeRawContainer, keyConverter, valueConverter);		
		
		System.out.println("Initialization of the tree finished.");
		return tree;
	}

	public static void suite1(TestableMap<Integer, Integer, Long> tree) {
		Map<Integer, Integer> compmap = fill(tree, NUMBER_OF_ELEMENTS);
		random = new Random(55);
		positiveLookups(tree, compmap, NUMBER_OF_ELEMENTS / 3);
		randomKeyLookups(tree, compmap, NUMBER_OF_ELEMENTS / 3);
		rangeQueries(tree, compmap, NUMBER_OF_ELEMENTS / 20);
	}
	
	public static void suite2(TestableMap<Integer, Integer, Long> tree) {
		Map<Integer, Integer> compmap = fill(tree, NUMBER_OF_ELEMENTS);
		random = new Random(55);
		positiveLookups(tree, compmap, NUMBER_OF_ELEMENTS / 3);
		randomKeyLookups(tree, compmap, NUMBER_OF_ELEMENTS / 2);		
		rangeQueries(tree, compmap, NUMBER_OF_ELEMENTS / 20);
	}
	
	public static void s_approxQueries(RSTree_v3<Integer, Integer, Long> tree) {
		Map<Integer, Integer> compmap = fill(tree, NUMBER_OF_ELEMENTS);
		random = new Random(55);
//		rangeQueries(tree, compmap, NUMBER_OF_ELEMENTS / 20);
		samplingTest(tree, compmap, 100);
	}
	
	public static void suite3(WBTreeSA_v3<Integer, Integer, Long> tree) {
		Map<Integer, Integer> compmap = fill(tree, NUMBER_OF_ELEMENTS);
		debugRangeQueries(tree);
	}
	
	public static Map<Integer,Integer> fill(TestableMap<Integer, Integer, Long> wbTree, int amount) {
		//-- comparison structure
		TreeMap<Integer, Integer> compmap = new TreeMap<Integer, Integer>();
		
		//-- Insertion - generate test data
		Random random = new Random(139);
		System.out.println("-- Insertion test: Generating random test data");

		for (int i = 1; i <= amount; i++) {
			int value = random.nextInt();
			wbTree.insert(value);
			compmap.put(wbTree.getGetKey().apply(value), value);
			if (i % (amount / 10) == 0)
				System.out.print((i / (amount / 100)) + "%, ");
		}
		
		System.out.println("Resulting tree height: " + wbTree.height());

		return compmap;
	}
	
	public static int positiveLookups(TestableMap<Integer, Integer, Long> tree, Map<Integer,Integer> compmap, int LOOKUP_TESTS_POSITIVE) {
		// final int LOOKUP_TESTS_POSITIVE = NUMBER_OF_ELEMENTS / 3;
		long timeStart = System.nanoTime();
		
		System.out.println("-- Positive Lookups (perhaps duplicate) (#="+ LOOKUP_TESTS_POSITIVE +"):");		
		//--- Lookup test
		//-- positive lookups		
		ArrayList<Integer> containedKeys = new ArrayList<Integer>(compmap.keySet());
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
		
		long timeElapsed = System.nanoTime() - timeStart;
		System.out.println("\ttime: "+ String.format("%8.2fms", ((double) timeElapsed / 1000000)) 
						  +"\t\tper item: "+ String.format("%5.2fms", ((double) timeElapsed / 1000000 / LOOKUP_TESTS_POSITIVE)));
		
		return errors_positiveLookup;
	}
	
	public static int randomKeyLookups(TestableMap<Integer, Integer, Long> tree, Map<Integer,Integer> compmap, int LOOKUP_TESTS_RANDOM) {
		long tsFunc = System.nanoTime();
		long ttQuery = 0;
		long ttCompMap = 0;
		
		System.out.println("-- Random Lookups from domain (mostly negative) (#="+ LOOKUP_TESTS_RANDOM +"):");		
		
		//-- (mostly) negative (= random) lookups		
		int errors_randomLookup = 0;
		for(int i=1; i <= LOOKUP_TESTS_RANDOM; i++) {
			Integer key = random.nextInt();
			
			long tsQuerySingle = System.nanoTime();
				List<Integer> treeAnswers = tree.get(key);
			ttQuery += System.nanoTime() - tsQuerySingle;
			
			long tsCompMapSingle = System.nanoTime();
				boolean compMapAnswer = compmap.containsKey(key);
			ttCompMap += System.nanoTime() - tsCompMapSingle;
			
			if(!treeAnswers.isEmpty() && !compMapAnswer) {
				System.out.println("False positive: \""+ key +"\"");
				errors_randomLookup++;
			}
			else if(treeAnswers.isEmpty() && compMapAnswer) {
				System.out.println("False negative: \""+ key +"\"");
				errors_randomLookup++;
			}
		}
//		System.out.println("Out of "+ LOOKUP_TESTS_RANDOM +" random lookups, failed on "+ errors_randomLookup +" occasions.");		
		System.out.println("\tfailed: "+ errors_randomLookup);
		
		long ttFunc = System.nanoTime() - tsFunc;
		System.out.println("\ttotal time: "+ String.format("%8.2fms", ((double) ttFunc / 1000000)) 
						  +"\t\tper item: "+ String.format("%5.2fms", ((double) ttFunc / 1000000 / LOOKUP_TESTS_RANDOM)));		
		System.out.println("\ttree query time: "+ String.format("%8.2fms", ((double) ttQuery / 1000000)) 
						  +"\t\tper item: "+ String.format("%5.2fms", ((double) ttQuery / 1000000 / LOOKUP_TESTS_RANDOM)));
		System.out.println("\tcomp query time: "+ String.format("%8.2fms", ((double) ttCompMap / 1000000)) 
		  					+"\t\tper item: "+ String.format("%5.2fms", ((double) ttCompMap / 1000000 / LOOKUP_TESTS_RANDOM)));
		
		return errors_randomLookup;
	}
	
	public static Triple<Integer, Integer, Integer> rangeQueries(TestableMap<Integer, Integer, Long> tree, Map<Integer,Integer> compmap, int RANGE_QUERY_TESTS) {
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

	public static Triple<Integer, Integer, Integer> samplingTest(
			RSTree_v3<Integer, Integer, Long> tree, 
			Map<Integer,Integer> compmap, 
			int SAMPLING_QUERY_TESTS) {
			final int SAMPLE_SIZE = 100;
			//-- rangeQuery tests
			System.out.println("-- SamplingQuery Tests (#="+ SAMPLING_QUERY_TESTS +"):");
			
			ArrayList<Integer> containedKeys = new ArrayList<Integer>(compmap.keySet());
			containedKeys.sort(null);
			
			int error_false_positive = 0;
			int error_false_negative = 0;
			int error_both = 0;
			
			for(int i=1; i <= SAMPLING_QUERY_TESTS; i++) {
				Integer lo = random.nextInt();
				Integer hi = random.nextInt();
				if(lo > hi) { int tmp = lo; lo = hi; hi = tmp; }
				long possKeys = (long)hi - (long)lo + 1;
				
				System.out.println("Range Query #"+ i +": "+ lo +" - "+ hi +" (#possKeys: "+ possKeys +"): ");
		
				//-- execute the query
				Cursor<Integer> sampCur = new Taker<Integer>(tree.samplingRangeQuery(lo, hi), SAMPLE_SIZE);			
				List<Integer> tRes = new ArrayList<Integer>(Cursors.toList(sampCur));
				
				System.out.println("T-result (#="+ tRes.size() +"): "+ tRes);
				
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
				
				System.out.println("C-result (#="+ cRes.size() +"): "+ cRes);			
								
				//- classify error case
				if(e_negatives > 0 || e_positives > 0) {
					System.out.println("\tErronous: #rsize: "+ tRes.size() +"; #compsize: "+ cRes.size() +"; #missing: "+ e_negatives +"; #too much: "+ e_positives);
					if(e_negatives > 0 && e_positives > 0) error_both++;
					else if(e_negatives > 0) error_false_negative++;
					else if(e_positives > 0) error_false_positive++;
				}
				
			}		
			
			System.out.println("\tToo big results:   "+ error_false_positive);
			return new Triple<Integer, Integer, Integer>(error_false_positive, error_false_negative, error_both);
		}

	public static void testTree(WBTreeSA_v3<Integer, Integer, Long> tree) throws IOException {			
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
		//--- find a nice filename
		String fileName;
		if (args.length > 0) { // custom container file
			fileName = args[0];
		} else { // std container file
			String CONTAINER_FILE_PREFIX = "test_map_test01";
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
//		WBTreeSA_v3<Integer, Integer, Long> tree = createWBTree(fileName);
		RSTree_v3<Integer, Integer, Long> tree = createRSTree(fileName);		
//		suite1(tree);
//		suite2(tree);
		s_approxQueries(tree);
	}

	/**
	 * For debugging purposes: creates a tree with consecutive keys {1, ..., AMOUNT} 
	 */
	public static Triple<Integer, Integer, Integer> debugRangeQueries(WBTreeSA_v3<Integer, Integer, Long> tree) {
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
