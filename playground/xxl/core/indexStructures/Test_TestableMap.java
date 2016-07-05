package xxl.core.indexStructures;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import com.google.uzaygezen.core.BitVector;
import com.google.uzaygezen.core.BitVectorFactories;
import com.google.uzaygezen.core.CompactHilbertCurve;

import xxl.core.collections.containers.CastingContainer;
import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.TypeSafeContainer;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Taker;
import xxl.core.cursors.sources.DiscreteRandomNumber;
import xxl.core.functions.FunJ8;
import xxl.core.indexStructures.HilbertRTreeSA.Node;
import xxl.core.indexStructures.HilbertRTreeSA.NodeConverter;
import xxl.core.io.LRUBuffer;
import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.Converters;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.io.converters.MeasuredFixedSizeConverter;
import xxl.core.profiling.DataDistributions;
import xxl.core.profiling.TestUtils;
import xxl.core.profiling.TreeCreation;
import xxl.core.spatial.rectangles.FixedPointRectangle;
import xxl.core.util.CopyableRandom;
import xxl.core.util.HUtil;
import xxl.core.util.Interval;
import xxl.core.util.ListJoinOuter3way;
import xxl.core.util.ListJoinOuter3way.JoinResult;
import xxl.core.util.Pair;
import xxl.core.util.SpatialHelpers;
import xxl.core.util.Triple;
import xxl.core.util.random.JavaDiscreteRandomWrapper;

/**
 * Sanity checks for self-implemented maps (= trees). 
 * 
 * TODO: enable handling of duplicates in comparison map (change type from Map<K,V> to Map<K,Set<V>>)
 * 
 * Mini-Milestone 1: migrate sampling tests to Test_ApproxQueries.java
 */
public class Test_TestableMap {

	public static final int BLOCK_SIZE = 2048;
//	public static final float MIN_RATIO = 0.5f;
//	public static final int BUFFER_SIZE = 10;
//	public static final int NUMBER_OF_BITS = 256;
//	public static final int MAX_OBJECT_SIZE = 78;
	public static final int NUMBER_OF_ELEMENTS = 10000;
	public static final int BATCH_SAMPLE_SIZE_DEFAULT = 20;
	public static final int IO_BUFFER_SIZE = 0;
	public static final int NODE_BUFFER_SIZE = 1;
	
	public static final int KEY_LO = 0, KEY_HI = 100000;
	public static final double VAL_LO = 0, VAL_HI = ((double)KEY_HI * (double)KEY_HI + (double)KEY_HI);

	/** Shared state of the RNG. Instanciated Once. */  
	public static CopyableRandom random;	
	
	public static <K extends Comparable<K>, V> int positiveLookups(
			TestableMap<K, V> tree, NavigableMap<K, List<V>> compmap, int LOOKUP_TESTS_POSITIVE) {
		// final int LOOKUP_TESTS_POSITIVE = NUMBER_OF_ELEMENTS / 3;
		long ttFullFunc = System.nanoTime();
		long ttTree = 0;
		long ttCompMap = 0;
		
		System.out.println("========================================================================");
		System.out.println("================== Positive Lookups (perhaps duplicate) (#="+ LOOKUP_TESTS_POSITIVE +"):");
		System.out.println("========================================================================");
		
		//--- Lookup test
		//-- positive lookups		
		ArrayList<K> containedKeys = new ArrayList<K>(compmap.keySet());
		int errors_positiveLookup = 0;
		for(int i=1; i <= LOOKUP_TESTS_POSITIVE; i++) {
			int keyNr = random.nextInt(containedKeys.size());
			K key = containedKeys.get(keyNr);
			
			// List<V> treeAnswers = tree.get(key);
			long tsTree = System.nanoTime();
				List<V> treeAnswers = Cursors.toList(tree.rangeQuery(new Interval<K>(key)));
			ttTree += System.nanoTime() - tsTree;
			
			long tsCompMap = System.nanoTime();
				List<V> mapAnswers = compmap.get(key);
			ttCompMap += System.nanoTime() - tsCompMap;
			//-- compute the difference between the resulsts
			JoinResult<V> difference = ListJoinOuter3way.join3way(treeAnswers, mapAnswers);

			
			if(!difference.rightAnti.isEmpty()) {
				System.out.print("#"+ i +":\t ");
				System.out.println("FAILED.");				
				System.out.println("-- query: \n\t"+ key);
				System.out.println("-- tree result (# = "+ treeAnswers.size() +"): \n\t"+ treeAnswers);
				System.out.println("-- comp result (# = "+ mapAnswers.size() +"): \n\t"+ mapAnswers);
				System.out.println("-- false negatives (# = "+ difference.rightAnti.size() +"): \n\t"+ difference.rightAnti);
				
				errors_positiveLookup++;
			} else {
//				System.out.print("#"+ i +":\t ");
//				System.out.println("OK.");
//				System.out.println("-- query: \n\t"+ key);
//				System.out.println("-- tree result (# = "+ treeAnswers.size() +"):: \n\t"+ treeAnswers);
//				System.out.println("-- comp result (# = "+ mapAnswers.size() +"):: \n\t"+ mapAnswers);
			}
			
		}		
		
		// System.out.println("Out of "+ LOOKUP_TESTS_POSITIVE +" (perhaps duplicate) positive lookups, failed on "+ errors_positiveLookup +" occasions.");
		System.out.println("\n\n========================================");
		System.out.println("\tfailed: "+ errors_positiveLookup +"/"+ LOOKUP_TESTS_POSITIVE);
		
		ttFullFunc = System.nanoTime() - ttFullFunc;
		System.out.println("\ttotal time: "+ String.format("%8.2fms", ((double) ttFullFunc / 1000000)) 
						  +"\t\tper item: "+ String.format("%5.2fms", ((double) ttFullFunc / 1000000 / LOOKUP_TESTS_POSITIVE)));		
		System.out.println("\ttree query time: "+ String.format("%8.2fms", ((double) ttTree / 1000000)) 
						  +"\t\tper item: "+ String.format("%5.2fms", ((double) ttTree / 1000000 / LOOKUP_TESTS_POSITIVE)));
		System.out.println("\tcomp query time: "+ String.format("%8.2fms", ((double) ttCompMap / 1000000)) 
		  					+"\t\tper item: "+ String.format("%5.2fms", ((double) ttCompMap / 1000000 / LOOKUP_TESTS_POSITIVE)));
		
		return errors_positiveLookup;
	}
	
	public static <K extends Comparable<K>, V> int randomKeyLookups(
			TestableMap<K, V> tree, NavigableMap<K, List<V>> compmap, int LOOKUP_TESTS_RANDOM, Cursor<K> testKeysCursor) {
		long ttFullFunc = System.nanoTime();
		long ttTree = 0;
		long ttCompMap = 0;
		
		System.out.println("========================================================================");
		System.out.println("================== Random Lookups from domain (mostly negative) (#="+ LOOKUP_TESTS_RANDOM +"):");
		System.out.println("========================================================================");
		
		//-- (mostly) negative (= random) lookups		
		int errors_randomLookup = 0;
		int nonEmptyQueries = 0;
		for(int i=1; i <= LOOKUP_TESTS_RANDOM; i++) {
			K key = testKeysCursor.next();
			
			long tsTree = System.nanoTime(); // TIMING
			List<V> treeAnswers = tree.get(key);
//			List<V> treeAnswers = Cursors.toList(tree.rangeQuery(new Interval<K>(key)));
			ttTree += System.nanoTime() - tsTree; // TIMING
			
			long tsCompMap = System.nanoTime(); // TIMING
			List<V> mapAnswers = compmap.getOrDefault(key, new LinkedList<V>());
			if(!mapAnswers.isEmpty()) nonEmptyQueries++;
			ttCompMap += System.nanoTime() - tsCompMap; // TIMING
			
			//-- compute the difference between the resulsts
			JoinResult<V> difference = ListJoinOuter3way.join3way(treeAnswers, mapAnswers);
			
			if(!difference.leftAnti.isEmpty() || !difference.rightAnti.isEmpty()) {
				System.out.print("#"+ i +":\t ");
				System.out.println("FAILED.");
				System.out.println("-- query: \n\t"+ key);
				System.out.println("-- tree result (# = "+ treeAnswers.size() +"):: \n\t"+ treeAnswers);
				System.out.println("-- comp result (# = "+ mapAnswers.size() +"):: \n\t"+ mapAnswers);

				if(!difference.leftAnti.isEmpty()) {
					System.out.println("-- false positives (# = "+ difference.leftAnti.size() +"): \n\t"+ difference.leftAnti);
				}
				if(!difference.rightAnti.isEmpty()) {					
					System.out.println("-- false negatives (# = "+ difference.rightAnti.size() +"): \n\t"+ difference.rightAnti);
				}
				errors_randomLookup++;
			} else {
//				System.out.print("#"+ i +":\t ");
//				System.out.println("OK.");
			}
			
		}
//		System.out.println("Out of "+ LOOKUP_TESTS_RANDOM +" random lookups, failed on "+ errors_randomLookup +" occasions.");
		System.out.println("\tnon empty queries: "+ nonEmptyQueries);
		System.out.println("\tfailed: "+ errors_randomLookup +"/"+ LOOKUP_TESTS_RANDOM);
		
		ttFullFunc = System.nanoTime() - ttFullFunc;
		System.out.println("\ttotal time: "+ String.format("%8.2fms", ((double) ttFullFunc / 1000000)) 
						  +"\t\tper item: "+ String.format("%5.2fms", ((double) ttFullFunc / 1000000 / LOOKUP_TESTS_RANDOM)));		
		System.out.println("\ttree query time: "+ String.format("%8.2fms", ((double) ttTree / 1000000)) 
						  +"\t\tper item: "+ String.format("%5.2fms", ((double) ttTree / 1000000 / LOOKUP_TESTS_RANDOM)));
		System.out.println("\tcomp query time: "+ String.format("%8.2fms", ((double) ttCompMap / 1000000)) 
		  					+"\t\tper item: "+ String.format("%5.2fms", ((double) ttCompMap / 1000000 / LOOKUP_TESTS_RANDOM)));
		
		return errors_randomLookup;
	}
	
	
	public static <K extends Comparable<K>, V> Triple<Integer, Integer, Integer> rangeQueries(
			TestableMap<K, V> tree, NavigableMap<K, List<V>> compmap, int RANGE_QUERY_TESTS, Cursor<K> testKeysCursor) {
		// final int RANGE_QUERY_TESTS = 1;
		//-- rangeQuery tests
		long ttFullFunc = System.nanoTime();
		long ttTree = 0;
		long ttCompMap = 0;
		
		System.out.println("========================================================================");
		System.out.println("================== RangeQuery Tests (#="+ RANGE_QUERY_TESTS +"):");
		System.out.println("========================================================================");
		int error_false_positive = 0;
		int error_false_negative = 0;
		int error_both = 0;
		int nonEmptyQueries = 0;
		
		for(int i=1; i <= RANGE_QUERY_TESTS; i++) {
			//- determine query interval
			K lo = testKeysCursor.next();
			K hi = testKeysCursor.next();
			if(lo.compareTo(hi) > 0) { K tmp = lo; lo = hi; hi = tmp; }
			
//			System.out.println("Range Query #"+ i +": \t\t"+ lo +" - \t"+ hi +""); // DEBUG
	
			//-- execute the query on the tree
			Interval<K> query = new Interval<K>(lo, true, hi, false);
			long tsTree = System.nanoTime(); // TIMING
			Cursor<V> treeResultCursor = tree.rangeQuery(query);
			List<V> treeAnswers = new ArrayList<V>(Cursors.toList(treeResultCursor));
			ttTree += System.nanoTime() - tsTree; // TIMING
				
			//-- execute the query on the comparison map
			long tsCompMap = System.nanoTime(); // TIMING			
			NavigableMap<K, List<V>> comparisonResultMap = compmap.subMap(lo, true, hi, false);
			if(!comparisonResultMap.isEmpty()) nonEmptyQueries++;
			//- flatten
			List<V> mapAnswers = new LinkedList<V>();
			for(Map.Entry<K, List<V>> entry : comparisonResultMap.entrySet()) {
				mapAnswers.addAll(entry.getValue());
			}
			ttCompMap += System.nanoTime() - tsCompMap; // TIMING
			
			//-- Test current query
			int e_negatives = 0;
			int e_positives = 0;
			
			//-- compute the difference between the resulsts
			JoinResult<V> difference = ListJoinOuter3way.join3way(treeAnswers, mapAnswers);
			
			if(!difference.leftAnti.isEmpty() || !difference.rightAnti.isEmpty()) {
				System.out.print("#"+ i +":\t ");
				System.out.println("FAILED.");
				System.out.println("-- query: \n\t"+ query);
				System.out.println("-- tree result (# = "+ treeAnswers.size() +"): \n\t"+ treeAnswers);
				System.out.println("-- comp result (# = "+ mapAnswers.size() +"): \n\t"+ mapAnswers);

				if(!difference.leftAnti.isEmpty()) {
					System.out.println("-- false positives (# = "+ difference.leftAnti.size() +"): \n\t"+ difference.leftAnti);
					e_positives += difference.leftAnti.size();
				}
				if(!difference.rightAnti.isEmpty()) {
					System.out.println("-- false negatives (# = "+ difference.rightAnti.size() +"): \n\t"+ difference.rightAnti);
					e_negatives += difference.rightAnti.size();
				}
			} else {
//				System.out.print("#"+ i +":\t ");
//				System.out.println("OK.");
			}
			
			//- classify error case
			if(e_negatives > 0 || e_positives > 0) {
				System.out.println("\tErronous: #rsize: "+ treeAnswers.size() +"; #compsize: "+ 
									mapAnswers.size() +"; #missing: "+ e_negatives +"; #too much: "+ e_positives);
				if(e_negatives > 0 && e_positives > 0) error_both++;
				else if(e_negatives > 0) error_false_negative++;
				else if(e_positives > 0) error_false_positive++;
			}			
		}	
		
		System.out.println("\tnon empty queries: "+ nonEmptyQueries);
		System.out.println("\tToo big results:   "+ error_false_positive);
		System.out.println("\tToo small results: "+ error_false_negative);
		System.out.println("\tBoth sided errors: "+ error_both);
		
		ttFullFunc = System.nanoTime() - ttFullFunc;
		System.out.println("\ttotal time: "+ String.format("%8.2fms", ((double) ttFullFunc / 1000000)) 
						  +"\t\tper item: "+ String.format("%5.2fms", ((double) ttFullFunc / 1000000 / RANGE_QUERY_TESTS)));		
		System.out.println("\ttree query time: "+ String.format("%8.2fms", ((double) ttTree / 1000000)) 
						  +"\t\tper item: "+ String.format("%5.2fms", ((double) ttTree / 1000000 / RANGE_QUERY_TESTS)));
		System.out.println("\tcomp query time: "+ String.format("%8.2fms", ((double) ttCompMap / 1000000)) 
		  					+"\t\tper item: "+ String.format("%5.2fms", ((double) ttCompMap / 1000000 / RANGE_QUERY_TESTS)));
		
		return new Triple<Integer, Integer, Integer>(error_false_positive, error_false_negative, error_both);
	}

	
	/** Um die Regions-Anfragen auf dem Hilbert-R-Tree zu testen... */
//	public static <K, V, P> Triple<Integer, Integer, Integer> spatialQueries(
	public static <K, V, P> Pair<TreeMap<Long, List<Long>>, TreeMap<Long, List<Long>>> spatialQueries(
			HilbertRTreeSA<V, P> tree, NavigableMap<K, List<V>> compmap, int SPATIAL_QUERY_TESTS, Cursor<FixedPointRectangle> testKeysCursor,
			Function<V, FixedPointRectangle> getBoundingBox) {
		long ttFullFunc = System.nanoTime();
		long ttTree = 0;
		long ttCompMap = 0;
		TreeMap<Long, List<Long>> timesPerQuerySizesTree = new TreeMap<Long, List<Long>>();
		TreeMap<Long, List<Long>> timesPerQuerySizesCompMap = new TreeMap<Long, List<Long>>();
		
		System.out.println("========================================================================");
		System.out.println("================== SpatialQuery Tests (#="+ SPATIAL_QUERY_TESTS +"):");
		System.out.println("========================================================================");
		int error_false_positive = 0;
		int error_false_negative = 0;
		int error_both = 0;
		int nonEmptyQueries = 0;
		
		//-- as our original insertion keys bear no influence on the spatial query, transform our map into a list of values
		LinkedList<V> valueList = new LinkedList<V>();
		for(List<V> valsForHV : compmap.values()) valueList.addAll(valsForHV);
		
//		System.out.println("valueSet: "+ valueList); // DEBUG		
//		System.out.println("#size: "+ valueList.size()); // DEBUG
		
		for(int i=1; i <= SPATIAL_QUERY_TESTS; i++) {
			//- determine query interval
			FixedPointRectangle queryArea = testKeysCursor.next();
			
//			System.out.println("Range Query #"+ i +": \t\t"+ queryArea); // DEBUG
	
			//-- execute the query on the tree
			long tsTree = System.nanoTime(); // TIMING
			Cursor<V> treeResultCursor = tree.areaRangeQuery(queryArea);
			List<V> treeAnswers = new ArrayList<V>(Cursors.toList(treeResultCursor));
			tsTree = System.nanoTime() - tsTree; // TIMING
			ttTree += tsTree; // TIMING
			
			//-- execute the query on the comparison structe (which has no structure, therefore is pretty slow.)
			long tsCompMap = System.nanoTime(); // TIMING
			List<V> mapAnswers = new LinkedList<V>();
			for(V value : valueList) {
				if(getBoundingBox.apply(value).overlaps(queryArea))
					mapAnswers.add(value);
			}
			tsCompMap = System.nanoTime() - tsCompMap; // TIMING
			ttCompMap += tsCompMap; // TIMING
			
			if(!mapAnswers.isEmpty()) nonEmptyQueries++;
			//- save in "histogram"
			long querySize = mapAnswers.size();
			timesPerQuerySizesTree.putIfAbsent(querySize, new LinkedList<>());
			timesPerQuerySizesCompMap.putIfAbsent(querySize, new LinkedList<>());
			timesPerQuerySizesTree.get(querySize).add(tsTree);
			timesPerQuerySizesCompMap.get(querySize).add(tsCompMap);			
			
			//-- Test current query
			int e_negatives = 0;
			int e_positives = 0;
			
			//-- compute the difference between the resulsts
			JoinResult<V> difference = ListJoinOuter3way.join3way(treeAnswers, mapAnswers);
			
			if(!difference.leftAnti.isEmpty() || !difference.rightAnti.isEmpty()) {
				System.out.print("#"+ i +":\t ");
				System.out.println("FAILED.");
				System.out.println("-- query: \n\t"+ queryArea);
				System.out.println("-- tree result (# = "+ treeAnswers.size() +"): \n\t"+ treeAnswers);
				System.out.println("-- comp result (# = "+ mapAnswers.size() +"): \n\t"+ mapAnswers);

				if(!difference.leftAnti.isEmpty()) {
					System.out.println("-- false positives (# = "+ difference.leftAnti.size() +"): \n\t"+ difference.leftAnti);
					e_positives += difference.leftAnti.size();
				}
				if(!difference.rightAnti.isEmpty()) {
					System.out.println("-- false negatives (# = "+ difference.rightAnti.size() +"): \n\t"+ difference.rightAnti);
					e_negatives += difference.rightAnti.size();
				}
			} else {
//				System.out.print("#"+ i +":\t ");
//				System.out.println("OK.");
			}
			
			//- classify error case
			if(e_negatives > 0 || e_positives > 0) {
				System.out.println("\tErronous: #rsize: "+ treeAnswers.size() +"; #compsize: "+ 
									mapAnswers.size() +"; #missing: "+ e_negatives +"; #too much: "+ e_positives);
				if(e_negatives > 0 && e_positives > 0) error_both++;
				else if(e_negatives > 0) error_false_negative++;
				else if(e_positives > 0) error_false_positive++;
			}			
		}	
		
		System.out.println("\tnon empty queries: "+ nonEmptyQueries);
		System.out.println("\tToo big results:   "+ error_false_positive);
		System.out.println("\tToo small results: "+ error_false_negative);
		System.out.println("\tBoth sided errors: "+ error_both);
		
		ttFullFunc = System.nanoTime() - ttFullFunc;
		System.out.println("\ttotal time: "+ String.format("%8.2fms", ((double) ttFullFunc / 1000000)) 
						  +"\t\tper item: "+ String.format("%5.2fms", ((double) ttFullFunc / 1000000 / SPATIAL_QUERY_TESTS)));		
		System.out.println("\ttree query time: "+ String.format("%8.2fms", ((double) ttTree / 1000000)) 
						  +"\t\tper item: "+ String.format("%5.2fms", ((double) ttTree / 1000000 / SPATIAL_QUERY_TESTS)));
		System.out.println("\tcomp query time: "+ String.format("%8.2fms", ((double) ttCompMap / 1000000)) 
		  					+"\t\tper item: "+ String.format("%5.2fms", ((double) ttCompMap / 1000000 / SPATIAL_QUERY_TESTS)));
		
//		return new Triple<Integer, Integer, Integer>(error_false_positive, error_false_negative, error_both);
//		return "("+ timesPerQuerySizesTree.toString() +", "+ timesPerQuerySizesCompMap.toString() +")";
		return new Pair<TreeMap<Long, List<Long>>, TreeMap<Long, List<Long>>>(timesPerQuerySizesTree, timesPerQuerySizesCompMap);
	}

	
	//	public static String test_hilbertTree_fromAtoZ() throws FileNotFoundException {
	public static Pair<TreeMap<Long,List<Long>>,TreeMap<Long,List<Long>>> test_hilbertTree_fromAtoZ() throws FileNotFoundException {
		long startTime = System.nanoTime();
		int dimension = 3;
		//======= INITIALISATION
		Container treeRawContainer = new BlockFileContainer(TestUtils.resolveFilename("HilbertTest_first"), BLOCK_SIZE);
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
//		int nDuplicatesAllowed = 5;
		
		//- set branching param fixed
		int branchingHi = 20;
		int branchingLo = 5;
		
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

		//== bounding box computer // OPT: make general
		Function<FixedPointRectangle, FixedPointRectangle> getBoundingBox = (x -> x);
		
		//== space filling curve
		int[] bitsPerDimensions = {4,4,4};
		int sumBitsPerDimension = 0; for(int bd : bitsPerDimensions) sumBitsPerDimension += bd;
		final int sumBitsPerDimensionFinal = sumBitsPerDimension;
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

		HilbertRTreeSA<FixedPointRectangle, Long> tree = 
				new HilbertRTreeSA<FixedPointRectangle, Long>(
						branchingLo, branchingHi, 
						leafLo, leafHi, 
						samplesPerNodeLo, samplesPerNodeHi,
						dimension,
						universe,
						getBoundingBox,
						getSFCKey, 
						nDuplicatesAllowed
						);
		
		//-- set the PRNG state
		tree.setRNG(new CopyableRandom(random));
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

		//=== FILLING
		Cursor<FixedPointRectangle> dataCursor = DataDistributions.rectanglesRandom(random, bitsPerDimensions);
		Cursor<Long> testKeysCursor = new AbstractCursor<Long>() {
			protected boolean hasNextObject() { return true; }
			protected Long nextObject() { return random.nextLong(1 << (sumBitsPerDimensionFinal-1) /* Long.MAX_VALUE */); }
		};
//		System.out.println("testKeysCursor sample: "+ Cursors.toList(new Taker(testKeysCursor, 100))); // DEBUG
		
		NavigableMap<Long, List<FixedPointRectangle>> compmap = TreeCreation.fillTestableMap(tree, NUMBER_OF_ELEMENTS, dataCursor, 
				tree.getGetKey(), nDuplicatesAllowed);
		
		System.out.println("resulting weight: "+ tree.totalWeight() +" / "+ NUMBER_OF_ELEMENTS);
		
		//==== 1-dimensional SANITY TESTS
		positiveLookups(tree, compmap, tree.totalWeight() / 30);
		randomKeyLookups(tree, compmap, tree.totalWeight() / 30, testKeysCursor);
		rangeQueries(tree, compmap, tree.totalWeight() / 200, testKeysCursor);
		
		//=== spatial lookups
		Cursor<FixedPointRectangle> testAreaCursor = DataDistributions.rectanglesRandom(random, bitsPerDimensions);
		Pair<TreeMap<Long, List<Long>>, TreeMap<Long, List<Long>>> profResult = 
				spatialQueries(tree, compmap, tree.totalWeight() / 200, testAreaCursor, (x -> x));
		
		long timeElapsed = System.nanoTime() - startTime;
		System.out.println("-- Total time elapsed: "+ (timeElapsed / 1000000) +"ms");
		return profResult;
	}
	
	public static void main(String[] args) throws Exception {
			
	//		test_duplicateHandling(); return;
			
			//--- run the actual tests
	//		random = new CopyableRandom(122928473621795L); // 119066442596134L
			random = new CopyableRandom(); 
			System.out.println("seed: "+ random.getSeed());
			
			Pair<TreeMap<Long, List<Long>>, TreeMap<Long, List<Long>>> profResult = test_hilbertTree_fromAtoZ();
			System.out.println(profResult.toString());
			/*
			Cursor<Integer> testKeysCursor = new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(new CopyableRandom(random), 10000));
			int nDuplicatesAllowed = 40;
			
	//		TestableMap<Integer, Pair<Integer, Double>> wrsTree = 
	//				TreeCreation.createWRSTree(TestUtils.resolveFilename("wrsTree_test101"), BLOCK_SIZE, 12, null, new CopyableRandom(random));
	// ---------------------------------------------------------
	//		block size: 	2048
	//		branching:	 tA: 12 ~ (4 - 47)
	//		leafentries:	 tK: 85 ~ (42 - 169)
	//		samples:	 20 - 83
			
			RSTree1D<Integer, Pair<Integer, Double>, Long> tree = 
					TreeCreation.createRSTree(TestUtils.resolveFilename("RSTree_sanity_16"), BLOCK_SIZE, 4, 20, 
							new CopyableRandom(random), nDuplicatesAllowed);
			
			Cursor<Pair<Integer, Double>> dataCursor = DataDistributions.iidUniformPairsIntDouble(random, KEY_LO, KEY_HI, VAL_LO, VAL_HI);
			
			NavigableMap<Integer, List<Pair<Integer, Double>>> compmap = TreeCreation.fillTestableMap(tree, NUMBER_OF_ELEMENTS, dataCursor, 
					tree.getGetKey(), nDuplicatesAllowed);
			
			System.out.println("resulting weight: "+ tree.totalWeight() +" / "+ NUMBER_OF_ELEMENTS);
			
			rangeQueries(tree, compmap, tree.totalWeight() / 50, testKeysCursor);
	//		---------------------------------------------------------		
	//		block size: 	2048
	//		branching: 	4 - 47
	//		leafentries: 	43 - 170
	//		samples: 	20 - 83
			
	//		testTree_sanityAgainstMemoryMap(rsTree, 
	//				DataDistributions.data_iidUniformPairsIntDouble(random, KEY_LO, KEY_HI, VAL_LO, VAL_HI), 		// data
	//				testKeysCursor,																  					// test data
	//				nDuplicatesAllowed
	//				);
			  
			 */
			
		}

	public static <K extends Comparable<K>, V> void testTree_sanityAgainstMemoryMap(
			TestableMap<K, V> tree, Cursor<V> dataCursor, Cursor<K> testKeysCursor, int nDuplicatesAllowed) {
		NavigableMap<K, List<V>> compmap = TreeCreation.fillTestableMap(tree, NUMBER_OF_ELEMENTS, dataCursor, tree.getGetKey(), nDuplicatesAllowed);
//		positiveLookups(tree, compmap, NUMBER_OF_ELEMENTS / 3);
//		randomKeyLookups(tree, compmap, NUMBER_OF_ELEMENTS / 3, testKeysCursor);
		rangeQueries(tree, compmap, NUMBER_OF_ELEMENTS / 50, testKeysCursor); // take long
	}

	public static void test_duplicateHandling() throws Exception {
		//--- run the actual tests
//		random = new CopyableRandom();
		random = new CopyableRandom(122928473621795L); // 119066442596134L
		System.out.println("seed: "+ random.getSeed());
		
		Cursor<Pair<Integer, Double>> dataCursor = DataDistributions.iidUniformPairsIntDouble(random, KEY_LO, KEY_HI, VAL_LO, VAL_HI);
		Cursor<Integer> testKeysCursor = new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(new CopyableRandom(random), 10000));
		int nDuplicatesAllowed = 5;
		
//		RSTree1D<Integer, Pair<Integer, Double>, Long> tree = 
//				TreeCreation.createRSTree(TestUtils.resolveFilename("RSTree_duplicate_test"), BLOCK_SIZE, 4, 47, 
//						new CopyableRandom(random), nDuplicatesAllowed);
		WRSTree1D<Integer, Pair<Integer, Double>, Long> tree = 
				TreeCreation.createWRSTree(TestUtils.resolveFilename("WRSTree_duplicate_test"), BLOCK_SIZE, 4, 47, 
						new CopyableRandom(random), nDuplicatesAllowed);
		
		NavigableMap<Integer, List<Pair<Integer, Double>>> compmap = 
				TreeCreation.fillTestableMap(tree, NUMBER_OF_ELEMENTS, dataCursor, tree.getGetKey(), nDuplicatesAllowed);
		
		System.out.println("resulting weight: "+ tree.totalWeight() +" / "+ NUMBER_OF_ELEMENTS);
		
		LinkedList<Integer> collect = Cursors.toList(tree.rangeQuery(tree.universe)).stream().map(Pair::getElement1).collect(Collectors.toCollection(LinkedList::new));
		System.out.println(collect);

	}

	public static Triple<Integer, Integer, Integer> samplingTest(RSTree1D<Integer, Integer, Long> tree,
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
			Cursor<Integer> sampCur = new Taker<Integer>(tree.samplingRangeQuery(lo, hi, BATCH_SAMPLE_SIZE_DEFAULT), SAMPLE_SIZE);			
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

	public static Triple<Integer, Integer, Integer> samplingTest_fixedPrecision_average(
		RSTree1D<Integer, Integer, Long> tree, 
		Map<Integer,Integer> compmap, 
		int FP_SAMPLING_QUERY_TESTS,
		double TARGET_PRECISION) {
	
		final int SAMPLE_SIZE = 100;
		//-- rangeQuery tests
		System.out.println("-- SamplingQuery Tests (#="+ FP_SAMPLING_QUERY_TESTS +"):");
		
		ArrayList<Integer> containedKeys = new ArrayList<Integer>(compmap.keySet());
		containedKeys.sort(null);
		
		int error_false_positive = 0;
		int error_false_negative = 0;
		int error_both = 0;
		
		for(int i=1; i <= FP_SAMPLING_QUERY_TESTS; i++) {
			Integer lo = random.nextInt();
			Integer hi = random.nextInt();
			if(lo > hi) { int tmp = lo; lo = hi; hi = tmp; }
			long possKeys = (long)hi - (long)lo + 1;
			
			System.out.println("Range Query #"+ i +": "+ lo +" - "+ hi +" (#possKeys: "+ possKeys +"): ");
	
			//-- execute the query
			Cursor<Integer> sampCur = new Taker<Integer>(tree.samplingRangeQuery(lo, hi, BATCH_SAMPLE_SIZE_DEFAULT), SAMPLE_SIZE);			
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
				int pos = Collections.binarySearch(tRes, cVal); // FIXME
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

//	public static void s2_approxQueries(RSTree1D<Integer, Integer, Long> tree) {
//		DiscreteRandomNumber dataCursor = new xxl.core.cursors.sources.DiscreteRandomNumber(new JavaDiscreteRandomWrapper(random), KEY_HI);
//		Map<Integer, Integer> compmap = TreeCreation.fillTestableMap(tree, NUMBER_OF_ELEMENTS, dataCursor, (t -> t));
//		samplingTest(tree, compmap, 100);
//	}
//
//	public static void s3_debugRangeQueries(WBTree<Integer, Integer, Long> tree) {
//		DiscreteRandomNumber dataCursor = new xxl.core.cursors.sources.DiscreteRandomNumber(new JavaDiscreteRandomWrapper(random), KEY_HI);
//		Map<Integer, Integer> compmap = TreeCreation.fillTestableMap(tree, NUMBER_OF_ELEMENTS, dataCursor, (t -> t));
//		debugRangeQueries(tree);
//	}
}
