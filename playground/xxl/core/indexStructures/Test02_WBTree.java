package xxl.core.indexStructures;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import xxl.core.collections.containers.CastingContainer;
import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.TypeSafeContainer;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.IntegerConverter;

public class Test02_WBTree {

	public static final int BLOCK_SIZE = 1024;
//	public static final float MIN_RATIO = 0.5f;
//	public static final int BUFFER_SIZE = 10;
//	public static final int NUMBER_OF_BITS = 256;
//	public static final int MAX_OBJECT_SIZE = 78;
	public static final int NUMBER_OF_ELEMENTS = 10000;

	private static WBTreeSA_v3<Integer, Integer, Long> createTree(String testFile) {
		
		WBTreeSA_v3<Integer, Integer, Long> tree = new WBTreeSA_v3<Integer, Integer, Long>(
				10, 										// leafParam
				5, 											// branchingParam
				(x -> x)); 									// getKey

		Converter<Integer> keyConverter = IntegerConverter.DEFAULT_INSTANCE;
		Converter<Integer> valueConverter = IntegerConverter.DEFAULT_INSTANCE;
		
		Container treeRawContainer = new BlockFileContainer(testFile, BLOCK_SIZE);			
		
		//-- Initialization with externally built Container
//		Converter<WBTreeSA_v3<Integer, Integer, Long>.Node> nodeConverter = 
//				tree.new NodeConverter(keyConverter, valueConverter, treeRawContainer.objectIdConverter());
//		
//		TypeSafeContainer<Long, WBTreeSA_v3<Integer, Integer, Long>.Node> treeContainer = 
//				new CastingContainer<Long, WBTreeSA_v3<Integer, Integer, Long>.Node>(
//					new ConverterContainer(treeRawContainer, nodeConverter)
//				); 
//		
//		tree.initialize_withReadyContainer(treeContainer);

		//-- Initialization with container creation inside the tree
		tree.initialize_buildContainer(treeRawContainer, keyConverter, valueConverter);		
		
		System.out.println("Initialization of the tree finished.");

		return tree;
	}

	public static void testTree(WBTreeSA_v3<Integer, Integer, Long> tree) throws IOException {
		//-- comparison structure
		TreeMap<Integer, Integer> compmap = new TreeMap<Integer, Integer>();
		
		//-- Insertion - generate test data
		Random random = new Random(139);
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

		WBTreeSA_v3<Integer, Integer, Long> wbTree = createTree(fileName);		
		testTree(wbTree);
	}
}
