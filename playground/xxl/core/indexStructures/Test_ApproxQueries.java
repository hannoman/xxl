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

	public static final int BLOCK_SIZE = 2048;
	public static final int NUMBER_OF_ELEMENTS = 10000;
	// Wir wollen unser Aggregat nur so weit berechnen, dass es sein Wert +/-1% zu 95% Wahrscheinlichkeit im Intervall liegt.
	// D.h. solange samplen bis das epsilon unseres Konfidenzintervalls < 1% des Aggregatwerts ist.
	public static final double INCONFIDENCE = 0.10;
	public static final double PRECISION_BOUND = 0.10;
	static final int KEY_LO = 0;
	static final int KEY_HI = 9000000; // 10000
	static final double VAL_LO = 0;
	static final double VAL_HI = (KEY_HI * KEY_HI + KEY_HI); // 100000000.0
	static final int N_COMPARISONS = 100;
	static int verbosity = 1;
	
	static final int BATCHSAMPLING_SIZE = 20;
	

	/** Shared state of the RNG. Instanciated Once. */  
	public static CopyableRandom random = new CopyableRandom(42);	
		
	private static void outputln(int minVerbosity, String s) { output(minVerbosity, s +"\n"); }

	private static void output(int minVerbosity, String s) {
		if(verbosity >= minVerbosity) System.out.print(s);
	}

	/** Computes the average of a range query once exactly and once approximately with large sample confidence < PRECISION_BOUND,
	 * then compares the amount of tuples needed. 
	 * Note that it might be possible that the SamplingCursor actually needs more tuples, as a high precision might dictate for
	 * more samples than the result set actually has.
	 * This is done N_QUERIES times. 
	 * @return */
	public static Pair<Integer, Integer> approxExactComparisons(
			SamplableArea<Interval<Integer>, Pair<Integer, Double>> tree, double PRECISION_BOUND, double INCONFIDENCE, int N_QUERIES) {
		int totalTouchedApprox = 0; int totalTouchedExact = 0;
		for(int i=0; i < N_QUERIES; i++) {
			// CHECK: is this a uniform distribution of intervals?
			int key_lo = KEY_LO + random.nextInt(KEY_HI - KEY_LO);
			int key_hi = KEY_LO + random.nextInt(KEY_HI - KEY_LO);
			if(key_lo > key_hi) { int tmp = key_lo; key_lo = key_hi; key_hi = tmp; } // swap 

			Pair<Integer, Integer> nodesTouched = approxExactComparison(tree, key_lo, key_hi, PRECISION_BOUND, INCONFIDENCE);
			totalTouchedApprox += nodesTouched.getElement1();
			totalTouchedExact  += nodesTouched.getElement2();
		}
		return new Pair<Integer, Integer>(totalTouchedApprox, totalTouchedExact);
	}
	
	public static Pair<Integer,Integer> approxExactComparison(
			SamplableArea<Interval<Integer>, Pair<Integer, Double>> tree, int key_lo, int key_hi, double PRECISION_BOUND, double INCONFIDENCE) {
		// approximate computation
		Quadruple<Double, Double, Integer, ProfilingCursor<Pair<Integer, Double>>> approx = approx1(tree, key_lo, key_hi, PRECISION_BOUND, INCONFIDENCE);
		
		// exact computation						
		Triple<Double, Integer, ProfilingCursor<Pair<Integer, Double>>> exact = exact1(tree, key_lo, key_hi);
		
		double estimatedError = approx.getElement2();
		double realError = Math.abs( (approx.getElement1() - exact.getElement1() ) / exact.getElement1() );
				
		outputln(1, "approx/exact: aggregate: "+ approx.getElement1() +" / "+ exact.getElement1() +
				" - #entries needed: "+ approx.getElement3() +"/"+ exact.getElement2() + 
				" - estimated error: "+ String.format("%2.4f", estimatedError * 100) +"%"+
				" - real error: "+      String.format("%2.4f", realError      * 100) +"%");
		
		Pair<Map<Integer,Integer>, Map<Integer, Integer>> approxCursorProfilingInfo = approx.getElement4().getProfilingInformation();
		int approxInspected = approxCursorProfilingInfo.getElement1().values().stream().reduce(0, (x,y) -> x+y);
		int approxPruned = approxCursorProfilingInfo.getElement2().values().stream().reduce(0, (x,y) -> x+y);
		
		outputln(4, "\t approx: nodes touched: "+ approxCursorProfilingInfo.getElement1() +" - nodes pruned: "+ approxCursorProfilingInfo.getElement2());
		Pair<Map<Integer,Integer>, Map<Integer, Integer>> exactCursorProfilingInfo = exact.getElement3().getProfilingInformation();
		int exactTotal = exactCursorProfilingInfo.getElement1().values().stream().reduce(0, (x,y) -> x+y);
		outputln(4, "\t exact : nodes touched: "+ exactCursorProfilingInfo.getElement1() +" - nodes pruned: "+ exactCursorProfilingInfo.getElement2());
		outputln(1, "\t approx/exact touched: "+ approxInspected +" / "+ exactTotal);
		outputln(3, "\t query: "+ new Interval<Integer>(key_lo, key_hi));
		
		return new Pair<Integer, Integer>(approxInspected, exactTotal);
	}
	
	/** Exact computation of one query.
	 * @return a pair <tt>(result, count)</tt> where count is the full number of entries satisfying the range query.
	 */
	public static Triple<Double, Integer, ProfilingCursor<Pair<Integer, Double>>> exact1(TestableMapV2<Interval<Integer>, Pair<Integer, Double>> tree, int key_lo, int key_hi) {
//		double resultExact = (double) Cursors.last(new Aggregator(exactVals, new StatefulAverage()));
		// exact computation
		ProfilingCursor<Pair<Integer, Double>> exactQueryCursor = tree.rangeQuery(new Interval<Integer>(key_lo, key_hi));
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
	public static Quadruple<Double, Double, Integer, ProfilingCursor<Pair<Integer, Double>>> approx1(
			SamplableArea<Interval<Integer>, Pair<Integer, Double>> tree, int key_lo, int key_hi, double PRECISION_BOUND, double INCONFIDENCE) {
		int REPORT_INTERVAL = 1000;		
		
		ProfilingCursor<Pair<Integer, Double>> samplingCursor = tree.samplingRangeQuery(new Interval<Integer>(key_lo, key_hi), BATCHSAMPLING_SIZE);
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
				outputln(1, i + ":\tval: " + nVal + "\t agg: " + agg + 
						"\t eps: "+ eps +"\t relError: "+ String.format("%3.3f", (Math.abs(relativeError) / 100)) +"%");
			}
			*/			
		}
		int valuesSampled = i;
		
		return new Quadruple<Double, Double, Integer, ProfilingCursor<Pair<Integer, Double>>>(agg, relativeError, valuesSampled, samplingCursor);
	}
	
	/** Tests the SamplingCursor for correctness regarding not producing false positives. */
	public static Triple<Integer, Integer, Integer> samplingCursorCorrectness(
					SamplableArea<Interval<Integer>, Pair<Integer, Double>> tree, 
					SortedMap<Integer,Pair<Integer, Double>> compmap, 
					final int SAMPLING_QUERY_TESTS,
					final int SAMPLE_SIZE) {
		
		//-- rangeQuery tests
		outputln(1, "-- SamplingQuery Tests (#queries: "+ SAMPLING_QUERY_TESTS +"; #samples per query: "+ SAMPLE_SIZE +"):");
		
		ArrayList<Integer> containedKeys = new ArrayList<Integer>(compmap.keySet());
		containedKeys.sort(null);
		
		int error_false_positive = 0;
		int error_false_negative = 0;
		int error_both = 0;
		
		for(int i=1; i <= SAMPLING_QUERY_TESTS; i++) {
			Integer lo = KEY_LO + random.nextInt(KEY_HI - KEY_LO);
			Integer hi = KEY_LO + random.nextInt(KEY_HI - KEY_LO);
			if(lo > hi) { int tmp = lo; lo = hi; hi = tmp; }
			Interval<Integer> query = new Interval<Integer>(lo, hi);
			long possKeys = (long)hi - (long)lo + 1;
			
			outputln(1, "Range Query #"+ i +": "+ lo +" - "+ hi +" (#possKeys: "+ possKeys +"): ");
	
			//-- execute the query
			Cursor<Pair<Integer, Double>> sampCur = new Taker<Pair<Integer, Double>>(tree.samplingRangeQuery(query, BATCHSAMPLING_SIZE), SAMPLE_SIZE);			
			List<Pair<Integer, Double>> tRes = new ArrayList<Pair<Integer, Double>>(Cursors.toList(sampCur));
			
			outputln(1, "T-result (#="+ tRes.size() +"): "+ tRes);
			
			//-- Test current query
			int e_negatives = 0;
			int e_positives = 0;
			
			//-- Tests for false positives
			for(Pair<Integer, Double> tVal : tRes)
				if(!compmap.containsKey(tree.getGetKey().apply(tVal))) e_positives++;
	
			//--- Computing the comparison-result
			SortedMap<Integer, Pair<Integer, Double>> cResMap = compmap.subMap(lo, hi);
			
			outputln(1, "C-result (#="+ cResMap.size() +"): "+ cResMap);			
							
			//- classify error case
			if(e_negatives > 0 || e_positives > 0) {
				outputln(1, "\tErronous: #rsize: "+ tRes.size() +"; #compsize: "+ cResMap.size() +"; #missing: "+ e_negatives +"; #too much: "+ e_positives);
				if(e_negatives > 0 && e_positives > 0) error_both++;
				else if(e_negatives > 0) error_false_negative++;
				else if(e_positives > 0) error_false_positive++;
			} else {
				outputln(1, "ok.");
			}
			
		}		
		
		outputln(1, "\tToo big results:   "+ error_false_positive);
		return new Triple<Integer, Integer, Integer>(error_false_positive, error_false_negative, error_both);
	}

	// TODO
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
	
//	// TODO
//	public static void bTreeTest() {
//				
//		Function<Pair<Integer, Double>, Interval1D> getDescriptorNew = (t -> new Interval1D(t.getElement1()));
//		xxl.core.functions.Function getDescriptor = FunJ8.toOld(getDescriptorNew);
//		
//		Function<Pair<Integer, Double>, Integer> getKey = (t -> t.getElement1());
//
//		//- create the tree
//		BTree tree = createBTree("bplus_init_test03", 1024, getDescriptor);
//		//- fill the tree
//		Map<Integer, Pair<Integer, Double>> compmap = fillXXLTree(tree, 100000, getKey, getDescriptor);
//		
//		//- close the container so that the metadata-file is written
//		// tree.container().close(); // only BPlusTree exposes this functionality
////		((Container) tree.getContainer.invoke(tree.rootEntry)).close();
//	}
	
	// TODO
	public static Map<Integer, Pair<Integer,Double>> fillXXLTree(
			Tree tree, 
			int AMOUNT, 
			Function<Pair<Integer, Double>, Integer> getKey, 
			xxl.core.functions.Function getDescriptor) {
		//-- comparison structure
		TreeMap<Integer, Pair<Integer,Double>> compmap = new TreeMap<Integer, Pair<Integer,Double>>();
		
		//-- Insertion - generate test data		
		outputln(1, "-- Insertion test: Generating "+ AMOUNT +" random test data points");
	
		Cursor<Pair<Integer, Double>> dataCur = DataDistributions.squarePairs(random, KEY_LO, KEY_HI, VAL_LO, VAL_HI);
		for (int i = 1; i <= AMOUNT; i++) {						
			Pair<Integer,Double> entry = dataCur.next();
			tree.insert(entry);
			compmap.put(getKey.apply(entry), entry);  
			if (i % (AMOUNT / 10) == 0) {
				output(1, (i / (AMOUNT / 100)) + "%, ");
				outputln(1, "inserted: "+ entry);
			}
		}
		
		outputln(1, "Resulting tree height: " + tree.height());
	
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
		
//---------------------------------------------------------
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
//		outputln(1, "--- tree random seed before querying: "+ tree.rng.getSeed());
//		
//		int key_lo = new Random().nextInt(KEY_HI);
//		outputln(1, "query: "+ new Interval<Integer>(key_lo, key_lo + 1000000));
//		approxExactComparison(tree, key_lo, key_lo + 1000000, PRECISION_BOUND);
//		
//		//+ test suite
//		// approxExactComparisons(tree, PRECISION_BOUND, 20);
		
		//--- run the actual tests
//		long seed = new Random().nextLong();
		long seed = 8183272422593055663L;
		System.out.println("seed: "+ seed);
		
		Cursor<Pair<Integer, Double>> dataCursor = null; 
		
		int nDuplicatesAllowed = 20;
		outputln(1, "-- filling RSTree..");
		
		RSTree1D<Integer, Pair<Integer, Double>, Long> rsTree = 
				TreeCreation.createRSTree(TestUtils.resolveFilename("rsTree_approxProf2"), BLOCK_SIZE, 4, 47, new CopyableRandom(seed), nDuplicatesAllowed);
		dataCursor = DataDistributions.iidUniformPairsIntDouble(new CopyableRandom(seed), KEY_LO, KEY_HI, VAL_LO, VAL_HI);
//		dataCursor = DataDistributions.data_squarePairs(new CopyableRandom(random), KEY_LO, KEY_HI, VAL_LO, VAL_HI);
		TreeCreation.fillTestableMap(rsTree, NUMBER_OF_ELEMENTS, dataCursor, Pair::getElement1, nDuplicatesAllowed);
		
//		Samplable1DMap<Integer, Pair<Integer,Double>> tree = rsTree;
//---------------------------------------------------------		
//		block size: 	2048
//		branching: 	4 - 47
//		leafentries: 	43 - 170
//		samples: 	20 - 83
		
		outputln(1, "-- filling WRSTree..");
		WRSTree1D<Integer, Pair<Integer, Double>, Long> wrsTree = 
				TreeCreation.createWRSTree(TestUtils.resolveFilename("wrsTree_approxProf2"), BLOCK_SIZE, 12, null, new CopyableRandom(seed), nDuplicatesAllowed);
//		dataCursor = DataDistributions.data_iidUniformPairsIntDouble(new CopyableRandom(random), KEY_LO, KEY_HI, VAL_LO, VAL_HI);
		dataCursor = DataDistributions.squarePairs(new CopyableRandom(random), KEY_LO, KEY_HI, VAL_LO, VAL_HI);
		TreeCreation.fillTestableMap(wrsTree, NUMBER_OF_ELEMENTS, dataCursor, Pair::getElement1, nDuplicatesAllowed );
//		Samplable1DMap<Integer, Pair<Integer,Double>> tree = wrsTree;
// ---------------------------------------------------------
//		block size: 	2048
//		branching:	 tA: 12 ~ (4 - 47)
//		leafentries:	 tK: 85 ~ (42 - 169)
//		samples:	 20 - 83
		
//		Samplable1DMap<Integer, Pair<Integer,Double>> tree = wrsTree;

		outputln(1, "\n\n===================== sampling RSTree:\n");
		random = new CopyableRandom(seed); // reset seed for next batch of queries
		Pair<Integer, Integer> rsTouched = approxExactComparisons(rsTree, PRECISION_BOUND, INCONFIDENCE, N_COMPARISONS);
		outputln(1, "\n\n===================== sampling WRSTree:\n");
		random = new CopyableRandom(seed); // reset seed for next batch of queries
		Pair<Integer, Integer> wrsTouched = approxExactComparisons(wrsTree, PRECISION_BOUND, INCONFIDENCE, N_COMPARISONS);
		
//		assert rsTouched.getElement2() == wrsTouched.getElement2();
		
		outputln(1, "\n\n\n---------------------------------");
		outputln(1, "exact query touched (control) RS/WRS: "+ rsTouched.getElement2() +" / "+ wrsTouched.getElement2());
		outputln(1, "total nodes RSTree: \t"+ rsTouched.getElement1());
		outputln(1, "total nodes WRSTree: \t"+ wrsTouched.getElement1());
		
	}
}
