package xxl.core.indexStructures;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.comparators.ComparableComparator;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Taker;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.functions.FunJ8;
import xxl.core.io.converters.DoubleConverter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.math.functions.AggregationFunction;
import xxl.core.math.statistics.parametric.aggregates.ConfidenceAggregationFunction;
import xxl.core.math.statistics.parametric.aggregates.StatefulAverage;
import xxl.core.profiling.DataDistributions;
import xxl.core.profiling.ProfilingCursor;
import xxl.core.profiling.TestUtils;
import xxl.core.profiling.TreeCreation;
import xxl.core.util.CopyableRandom;
import xxl.core.util.Interval;
import xxl.core.util.Interval1D;
import xxl.core.util.Pair;
import xxl.core.util.PairConverterFixedSized;
import xxl.core.util.Quadruple;
import xxl.core.util.Triple;


/**
 * Sanity checks for self-implemented maps (= trees). 
 * 
 * TODO: enable handling of duplicates in comparison map (change type from Map<K,V> to Map<K,Set<V>>)
 * DONE: where are convenient Converters for Pairs located? -> Nowhere :(
 */
public class Test_ApproxQueries {

	public static final int BLOCK_SIZE = 1024;
	public static final int NUMBER_OF_ELEMENTS = 100000;
	// Wir wollen unser Aggregat nur so weit berechnen, dass es sein Wert +/-1% zu 95% Wahrscheinlichkeit im Intervall liegt.
	// D.h. solange samplen bis das epsilon unseres Konfidenzintervalls < 1% des Aggregatwerts ist.
	public static final double INCONFIDENCE = 0.05;
	public static final double PRECISION_BOUND = 0.10;
	static final int KEY_LO = 0;
	static final int KEY_HI = 9000000; // 10000
	static final double VAL_LO = 0;
	static final double VAL_HI = (KEY_HI * KEY_HI + KEY_HI); // 100000000.0
	
	static final int BATCHSAMPLING_SIZE = 20;
	

	/** Shared state of the RNG. Instanciated Once. */  
	public static CopyableRandom random = new CopyableRandom(42);	
		
	/** Performs 100 comparisons between exact and approximate average queries. */
	public static void s_pruning(RSTree1D<Integer, Pair<Integer, Double>, Long> tree) {
		random = new CopyableRandom(55);
		Cursor<Pair<Integer, Double>> dataCursor = DataDistributions.data_squarePairs(random, KEY_LO, KEY_HI, VAL_LO, VAL_HI);
		Map<Integer, Pair<Integer, Double>> compMap = TreeCreation.fillTestableMap(tree, NUMBER_OF_ELEMENTS, dataCursor, (t -> t.getElement1()));
		approxExactComparisons(tree, PRECISION_BOUND, 100);
	}

	/** Temporary test for <tt>fill(tree)</tt> to check whether the tree gets generated correctly. */ 
	public static void s_generation(RSTree1D<Integer, Pair<Integer, Double>, Long> tree) {
		random = new CopyableRandom(55);
		Cursor<Pair<Integer, Double>> dataCursor = DataDistributions.data_squarePairs(random, KEY_LO, KEY_HI, VAL_LO, VAL_HI);
		Map<Integer, Pair<Integer, Double>> compMap = TreeCreation.fillTestableMap(tree, NUMBER_OF_ELEMENTS, dataCursor, (t -> t.getElement1()));
		for(Integer key : compMap.keySet()) {
			System.out.println(key +": "+ compMap.get(key));
		}
	}
	
	/** Computes the average of a range query once exactly and once approximately with large sample confidence < PRECISION_BOUND,
	 * then compares the amount of tuples needed. 
	 * Note that it might be possible that the SamplingCursor actually needs more tuples, as a high precision might dictate for
	 * more samples than the result set actually has.
	 * This is done N_QUERIES times. */
	public static void approxExactComparisons(RSTree1D<Integer, Pair<Integer, Double>, Long> tree, double PRECISION_BOUND, int N_QUERIES) {
		for(int i=0; i < N_QUERIES; i++) {
			// CHECK: is this a uniform distribution of intervals?
			int key_lo = KEY_LO + random.nextInt(KEY_HI - KEY_LO);
			int key_hi = KEY_LO + random.nextInt(KEY_HI - KEY_LO);
			if(key_lo > key_hi) { int tmp = key_lo; key_lo = key_hi; key_hi = tmp; } // swap 

			approxExactComparison(tree, key_lo, key_hi, PRECISION_BOUND);
		}		
	}
	
	public static void approxExactComparison(RSTree1D<Integer, Pair<Integer, Double>, Long> tree, int key_lo, int key_hi, double PRECISION_BOUND) {
		// approximate computation
		Quadruple<Double, Double, Integer, ProfilingCursor<Pair<Integer, Double>>> approx = approx1(tree, key_lo, key_hi, PRECISION_BOUND);
		
		// exact computation						
		Triple<Double, Integer, ProfilingCursor<Pair<Integer, Double>>> exact = exact1(tree, key_lo, key_hi);
		
		double estimatedError = approx.getElement2();
		double realError = Math.abs( (approx.getElement1() - exact.getElement1() ) / exact.getElement1() );
				
		System.out.println("approx/exact: aggregate: "+ approx.getElement1() +" / "+ exact.getElement1() +
				" - #entries needed: "+ approx.getElement3() +"/"+ exact.getElement2() + 
				" - estimated error: "+ String.format("%2.4f", estimatedError * 100) +"%"+
				" - real error: "+      String.format("%2.4f", realError      * 100) +"%");
		
		Pair<Map<Integer,Integer>, Map<Integer, Integer>> approxCursorProfilingInfo = approx.getElement4().getProfilingInformation();
		int approxInspected = approxCursorProfilingInfo.getElement1().values().stream().reduce(0, (x,y) -> x+y);
		int approxPruned = approxCursorProfilingInfo.getElement2().values().stream().reduce(0, (x,y) -> x+y);
		
		System.out.println("\t approx: nodes touched: "+ approxCursorProfilingInfo.getElement1() +" - nodes pruned: "+ approxCursorProfilingInfo.getElement2());
		Pair<Map<Integer,Integer>, Map<Integer, Integer>> exactCursorProfilingInfo = exact.getElement3().getProfilingInformation();
		int exactTotal = exactCursorProfilingInfo.getElement1().values().stream().reduce(0, (x,y) -> x+y);
		System.out.println("\t exact : nodes touched: "+ exactCursorProfilingInfo.getElement1() +" - nodes pruned: "+ exactCursorProfilingInfo.getElement2());
		System.out.println("\t approx/exact touched: "+ approxInspected +" / "+ exactTotal);
		System.out.println("\t query: "+ new Interval<Integer>(key_lo, key_hi));
	}
	
	/** Exact computation of one query.
	 * @return a pair <tt>(result, count)</tt> where count is the full number of entries satisfying the range query.
	 */
	public static Triple<Double,Integer, ProfilingCursor<Pair<Integer, Double>>> exact1(RSTree1D<Integer, Pair<Integer, Double>, Long> tree, int key_lo, int key_hi) {
//		double resultExact = (double) Cursors.last(new Aggregator(exactVals, new StatefulAverage()));
		// exact computation
		ProfilingCursor<Pair<Integer, Double>> exactQueryCursor = tree.rangeQuery(key_lo, key_hi);
		Cursor<Double> exactVals = new Mapper<Pair<Integer,Double>, Double>(FunJ8.toOld(e -> e.getSecond()), exactQueryCursor);
		
		AggregationFunction<Number, Number> avgAggFun = new StatefulAverage();
		Double agg = null;
		int i=0;
		for(; exactVals.hasNext(); i++) {
			double val = exactVals.next();
			agg = (Double) avgAggFun.invoke(agg, val);
		}
		int valuesUsed = i;
		return new Triple<Double, Integer, ProfilingCursor<Pair<Integer, Double>>>(agg, valuesUsed, exactQueryCursor);
	}
	
	/** Computes an average-estimator with confidence according to the large sample assumption, 
	 * from as much samples as needed to match PRECISION_BOUND. */
	public static Quadruple<Double, Double, Integer, ProfilingCursor<Pair<Integer, Double>>> approx1(RSTree1D<Integer, Pair<Integer, Double>, Long> tree, int key_lo, int key_hi, double PRECISION_BOUND) {
		int REPORT_INTERVAL = 1000;		
		
		ProfilingCursor<Pair<Integer, Double>> samplingCursor = tree.samplingRangeQuery(key_lo, key_hi, BATCHSAMPLING_SIZE);
		Cursor<Double> vals = new Mapper<Pair<Integer,Double>, Double>(
				FunJ8.toOld(Pair::getSecond), 
				samplingCursor);
		
		ConfidenceAggregationFunction coAggFun = ConfidenceAggregationFunction.largeSampleConfidenceAverage(INCONFIDENCE);
		Double agg = null;
		double eps = Double.POSITIVE_INFINITY;
		double relativeError = Double.POSITIVE_INFINITY;		
		int MIN_ITERATIONS = 30; // Rule of thumb when CLT can be applied
		int i = 0;
		for(; relativeError > PRECISION_BOUND || i < MIN_ITERATIONS; i++) {
			double nVal = vals.next();
			agg = (double) coAggFun.invoke(agg, nVal);
			eps = (double) coAggFun.epsilon();
			relativeError = Math.abs(eps / agg); // TODO: this is probably not totally exact
			/*
			if(i % REPORT_INTERVAL == 0) {
				System.out.println(i + ":\tval: " + nVal + "\t agg: " + agg + 
						"\t eps: "+ eps +"\t relError: "+ String.format("%3.3f", (Math.abs(relativeError) / 100)) +"%");
			}
			*/			
		}
		int valuesSampled = i;
		
		return new Quadruple<Double, Double, Integer, ProfilingCursor<Pair<Integer, Double>>>(agg, relativeError, valuesSampled, samplingCursor);
	}
	
	/** Tests the SamplingCursor for correctness regarding not producing false positives. */
	public static Triple<Integer, Integer, Integer> samplingCursorCorrectness(
					RSTree1D<Integer, Pair<Integer, Double>, Long> tree, 
					SortedMap<Integer,Pair<Integer, Double>> compmap, 
					final int SAMPLING_QUERY_TESTS,
					final int SAMPLE_SIZE) {
		
		//-- rangeQuery tests
		System.out.println("-- SamplingQuery Tests (#queries: "+ SAMPLING_QUERY_TESTS +"; #samples per query: "+ SAMPLE_SIZE +"):");
		
		ArrayList<Integer> containedKeys = new ArrayList<Integer>(compmap.keySet());
		containedKeys.sort(null);
		
		int error_false_positive = 0;
		int error_false_negative = 0;
		int error_both = 0;
		
		for(int i=1; i <= SAMPLING_QUERY_TESTS; i++) {
			Integer lo = KEY_LO + random.nextInt(KEY_HI - KEY_LO);
			Integer hi = KEY_LO + random.nextInt(KEY_HI - KEY_LO);
			if(lo > hi) { int tmp = lo; lo = hi; hi = tmp; }
			long possKeys = (long)hi - (long)lo + 1;
			
			System.out.println("Range Query #"+ i +": "+ lo +" - "+ hi +" (#possKeys: "+ possKeys +"): ");
	
			//-- execute the query
			Cursor<Pair<Integer, Double>> sampCur = new Taker<Pair<Integer, Double>>(tree.samplingRangeQuery(lo, hi, BATCHSAMPLING_SIZE), SAMPLE_SIZE);			
			List<Pair<Integer, Double>> tRes = new ArrayList<Pair<Integer, Double>>(Cursors.toList(sampCur));
			
			System.out.println("T-result (#="+ tRes.size() +"): "+ tRes);
			
			//-- Test current query
			int e_negatives = 0;
			int e_positives = 0;
			
			//-- Tests for false positives
			for(Pair<Integer, Double> tVal : tRes)
				if(!compmap.containsKey(tree.getGetKey().apply(tVal))) e_positives++;
	
			//--- Computing the comparison-result
//			int compLoIdx = HUtil.binFindES(containedKeys, lo);
//			int compHiIdx = HUtil.binFindSE(containedKeys, hi);
//			List<Integer> cRes = containedKeys.subList(compLoIdx, compHiIdx);
			SortedMap<Integer, Pair<Integer, Double>> cResMap = compmap.subMap(lo, hi);
			
			System.out.println("C-result (#="+ cResMap.size() +"): "+ cResMap);			
							
			//- classify error case
			if(e_negatives > 0 || e_positives > 0) {
				System.out.println("\tErronous: #rsize: "+ tRes.size() +"; #compsize: "+ cResMap.size() +"; #missing: "+ e_negatives +"; #too much: "+ e_positives);
				if(e_negatives > 0 && e_positives > 0) error_both++;
				else if(e_negatives > 0) error_false_negative++;
				else if(e_positives > 0) error_false_positive++;
			} else {
				System.out.println("ok.");
			}
			
		}		
		
		System.out.println("\tToo big results:   "+ error_false_positive);
		return new Triple<Integer, Integer, Integer>(error_false_positive, error_false_negative, error_both);
	}

	public static BTree createBTree(String container_prefix, int blockSize, xxl.core.functions.Function getDescriptor) {
		BTree tree = new BTree();
		Container blockContainer = new BlockFileContainer(container_prefix, blockSize);
		Container nodeContainer = new ConverterContainer(
				blockContainer,
				tree.nodeConverter(
						new PairConverterFixedSized<Integer, Double>(IntegerConverter.DEFAULT_INSTANCE, DoubleConverter.DEFAULT_INSTANCE),
						IntegerConverter.DEFAULT_INSTANCE,
						new ComparableComparator()
						)
			);
		tree.initialize(getDescriptor, nodeContainer, 10, 20);
		return tree;
	}
	
	public static void bTreeTest() {
				
		Function<Pair<Integer, Double>, Interval1D> getDescriptorNew = (t -> new Interval1D(t.getElement1()));
		xxl.core.functions.Function getDescriptor = FunJ8.toOld(getDescriptorNew);
		
		Function<Pair<Integer, Double>, Integer> getKey = (t -> t.getElement1());

		//- create the tree
		BTree tree = createBTree("bplus_init_test03", 1024, getDescriptor);
		//- fill the tree
		Map<Integer, Pair<Integer, Double>> compmap = fillXXLTree(tree, 100000, getKey, getDescriptor);
		
		//- close the container so that the metadata-file is written
		// tree.container().close(); // only BPlusTree exposes this functionality
//		((Container) tree.getContainer.invoke(tree.rootEntry)).close();
	}
	
	public static Map<Integer, Pair<Integer,Double>> fillXXLTree(
			Tree tree, 
			int AMOUNT, 
			Function<Pair<Integer, Double>, Integer> getKey, 
			xxl.core.functions.Function getDescriptor) {
		//-- comparison structure
		TreeMap<Integer, Pair<Integer,Double>> compmap = new TreeMap<Integer, Pair<Integer,Double>>();
		
		//-- Insertion - generate test data		
		System.out.println("-- Insertion test: Generating "+ AMOUNT +" random test data points");
	
		Cursor<Pair<Integer, Double>> dataCur = DataDistributions.data_squarePairs(random, KEY_LO, KEY_HI, VAL_LO, VAL_HI);
		for (int i = 1; i <= AMOUNT; i++) {						
//			Pair<Integer,Double> entry = new Pair<Integer, Double>(key, value);
			Pair<Integer,Double> entry = dataCur.next();
			tree.insert(entry);
			compmap.put(getKey.apply(entry), entry);  
			if (i % (AMOUNT / 10) == 0) {
				System.out.print((i / (AMOUNT / 100)) + "%, ");
				System.out.println("inserted: "+ entry);
			}
		}
		
		System.out.println("Resulting tree height: " + tree.height());
	
		return compmap;
	}
	
	public static void main(String[] args) throws Exception {
		
		
		//--- run the actual tests
//		random = new Random();
//		RSTree1D<Integer, Pair<Integer, Double>, Long> tree = createRSTree(resolveFilename("RSTree_noUnbuffered_pairs01"));
//		RSTree1D<Integer, Pair<Integer, Double>, Long> tree = createRSTree_withInnerUnbufferedNodes(resolveFilename("RSTree_someUnbufferred_pairs01"));
//		SortedMap<Integer, Pair<Integer,Double>> compmap = fill(tree, NUMBER_OF_ELEMENTS);
//		samplingCursorCorrectness(tree, compmap, 10, 100);
//		approxExactComparisons(tree, PRECISION_BOUND, 100);
		
		// s_pruning(tree);
		
//		s_generation(tree);
		
		
		
		

		// bTreeTest();
		
//		RSTree1D<Integer, Pair<Integer, Double>, Long> tree = createRSTree("insert_test");
//		QuickTime.start("Insertion");
//		
//		fill(tree, 100000);
//		QuickTime.stop();
		
		//- saving tree to metdata file 
//		String treename = "RS_pairs_big03";
//		createAndSave_RS_pair(resolveFilename(treename +"_meta"), resolveFilename(treename), 1000000);
		//-- loading tree from metadata file and performing tests on it
//		RSTree1D<Integer, Pair<Integer, Double>, Long> tree = load_RS_pair(resolveFilename(treename +"_meta"));
//		
//		//+ single manual tests
//		tree.setRNG(new CopyableRandom());
//		System.out.println("--- tree random seed before querying: "+ tree.rng.getSeed());
//		
//		int key_lo = new Random().nextInt(KEY_HI);
//		System.out.println("query: "+ new Interval<Integer>(key_lo, key_lo + 1000000));
//		approxExactComparison(tree, key_lo, key_lo + 1000000, PRECISION_BOUND);
//		
//		//+ test suite
//		// approxExactComparisons(tree, PRECISION_BOUND, 20);
		
		random = new CopyableRandom(464);
		RSTree1D<Integer, Pair<Integer, Double>, Long> tree = 
				TreeCreation.createRSTree(TestUtils.resolveFilename("filler_test"), 2048, 5, 20);
		tree.setRNG(random);
		Cursor<Pair<Integer, Double>> dataCursor = DataDistributions.data_iidUniformPairsIntDouble(random, KEY_LO, KEY_HI, VAL_LO, VAL_HI);
		TreeCreation.fillTestableMap(tree, 10000, dataCursor, ((Pair<Integer, Double> t) -> t.getElement1()) );
		approxExactComparisons(tree, PRECISION_BOUND, 50);
		
	}
}
