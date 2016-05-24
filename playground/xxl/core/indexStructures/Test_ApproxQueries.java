package xxl.core.indexStructures;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Taker;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.functions.FunJ8;
import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.DoubleConverter;
import xxl.core.io.converters.FixedSizeConverter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.math.functions.AggregationFunction;
import xxl.core.math.statistics.parametric.aggregates.ConfidenceAggregationFunction;
import xxl.core.math.statistics.parametric.aggregates.StatefulAverage;
import xxl.core.util.HUtil;
import xxl.core.util.Interval;
import xxl.core.util.Pair;
import xxl.core.util.PairConverterFixedSized;
import xxl.core.util.Triple;


/**
 * Sanity checks for self-implemented maps (= trees). 
 * 
 * TODO: enable handling of duplicates in comparison map (change type from Map<K,V> to Map<K,Set<V>>)
 * DONE: where are convenient Converters for Pairs located? -> Nowhere :(
 */
public class Test_ApproxQueries {

	public static final int BLOCK_SIZE = 1024;
	public static final int NUMBER_OF_ELEMENTS = 10000;
	// Wir wollen unser Aggregat nur so weit berechnen, dass es sein Wert +/-1% zu 95% Wahrscheinlichkeit im Intervall liegt.
	// D.h. solange samplen bis das epsilon unseres Konfidenzintervalls < 1% des Aggregatwerts ist.
	public static final double CONFIDENCE = 0.95;
	public static final double PRECISION_BOUND = 0.001;
	static final int KEY_LO = 0, KEY_HI = 10000;
	static final double VAL_LO = 0, VAL_HI = 100000000.0;
	

	/** Shared state of the RNG. Instanciated Once. */  
	public static Random random = new Random(42);	
		
	public static void s_pruning(RSTree_v3<Integer, Pair<Integer, Double>, Long> tree) {
		random = new Random(55);
		Map<Integer, Pair<Integer, Double>> compMap = fill(tree, NUMBER_OF_ELEMENTS);
		approxTest(tree, 100);
	}

	/** Temporary test for <tt>fill(tree)</tt> to check whether the tree gets generatd correctly. */ 
	public static void s_generation(RSTree_v3<Integer, Pair<Integer, Double>, Long> tree) {
		random = new Random(55);
		Map<Integer, Pair<Integer, Double>> compMap = fill(tree, NUMBER_OF_ELEMENTS);
		for(Integer key : compMap.keySet()) {
			System.out.println(key +": "+ compMap.get(key));
		}
	}
	
	private static RSTree_v3<Integer, Pair<Integer, Double>, Long> createRSTree(String testFile) {
		Container treeRawContainer = new BlockFileContainer(testFile, BLOCK_SIZE);
		
		FixedSizeConverter<Integer> keyConverter = IntegerConverter.DEFAULT_INSTANCE;		
		FixedSizeConverter<Pair<Integer,Double>> valueConverter = 
				new PairConverterFixedSized<Integer, Double>(IntegerConverter.DEFAULT_INSTANCE, DoubleConverter.DEFAULT_INSTANCE);
		FixedSizeConverter<Interval<Integer>> rangeConverter = Interval.getConverter(keyConverter);
		
		//-- estimating parameters for the tree
		//- fill leafes optimal
		int leafHi = (BLOCK_SIZE - BooleanConverter.SIZE - IntegerConverter.SIZE) / valueConverter.getSerializedSize();
		int leafLo = leafHi / 4;
		
		//- set branching param fixed
		int branchingParamHi = 20;
		int branchingParamLo = branchingParamHi / 4;
		
		//- determine how much is left for samples
		int innerSpaceLeft = BLOCK_SIZE;
		innerSpaceLeft -= BooleanConverter.SIZE; // node type indicator
		innerSpaceLeft -= IntegerConverter.SIZE; // # child nodes
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

		RSTree_v3<Integer, Pair<Integer, Double>, Long> tree = 
				new RSTree_v3<Integer, Pair<Integer,Double>, Long>(
						new Interval<Integer>(Integer.MIN_VALUE, Integer.MAX_VALUE), // universe
						samplesPerNodeLo, 
						samplesPerNodeHi, 
						branchingParamLo, 
						branchingParamHi, 
						leafLo, 
						leafHi, 
						((Pair<Integer, Double> x) -> x.getFirst())
					);
				
//		RSTree_v3<Integer, Pair<Integer, Double>, Long> tree = 
//			new RSTree_v3<Integer, Pair<Integer, Double>, Long>(
//				10, 											// samplesPerNodeLo
//				40,												// samplesPerNodeHi
//				10, 											// branchingParamLo
//				40,												// branchingParamHi
//				leafLo, 										// leafLo
//				leafHi, 										// leafHi
//				((Pair<Integer, Double> x) -> x.getFirst())		// getKey
//			);	
			
		//-- Initialization with container creation inside the tree
		tree.initialize_buildContainer(treeRawContainer, keyConverter, valueConverter);		
		
		System.out.println("Initialization of the tree finished.");
		return tree;
	}

	
	public static Map<Integer,Pair<Integer,Double>> fill(TestableMap<Integer, Pair<Integer, Double>, Long> tree, int AMOUNT) {		
		//-- comparison structure
		TreeMap<Integer, Pair<Integer,Double>> compmap = new TreeMap<Integer, Pair<Integer,Double>>();
		
		//-- Insertion - generate test data		
		System.out.println("-- Insertion test: Generating "+ AMOUNT +" random test data points");
	
		for (int i = 1; i <= AMOUNT; i++) {
			// Data1: Payload correlated with the key: payload ~ NormalDistribution(key*key, key)
			int key = KEY_LO + random.nextInt(KEY_HI - KEY_LO);
			double value = key * key + (random.nextDouble() * 2 * key) - key;
						
			Pair<Integer,Double> entry = new Pair<Integer, Double>(key, value);
			tree.insert(entry);
			compmap.put(tree.getGetKey().apply(entry), entry);
			if (i % (AMOUNT / 10) == 0)
				System.out.print((i / (AMOUNT / 100)) + "%, ");
		}
		
		System.out.println("Resulting tree height: " + tree.height());
	
		return compmap;
	}
	
	/** Compares the amount of tuples read for an exact and an approximate average query on N_QUERIES random range queries. */
	public static void approxTest(RSTree_v3<Integer, Pair<Integer, Double>, Long> tree, int N_QUERIES) {
		for(int i=0; i < N_QUERIES; i++) {
			// CHECK: is this a uniform distribution of intervals?
			int key_lo = KEY_LO + random.nextInt(KEY_HI - KEY_LO);
			int key_hi = KEY_LO + random.nextInt(KEY_HI - KEY_LO);
			if(key_lo > key_hi) { int tmp = key_lo; key_lo = key_hi; key_hi = tmp; } // swap 
			
			// approximate computation
			Triple<Double,Double,Integer> approx = approx1(tree, key_lo, key_hi);
			// exact computation						
			Pair<Double, Integer> exact = exact1(tree, key_lo, key_hi);
			
			double estimatedError = Math.abs(approx.getElement2());
			double realError = Math.abs( (approx.getElement1() - exact.getElement1() ) / exact.getElement1() );
					
			System.out.println("approx/exact: aggregate: "+ approx.getElement1() +" / "+ exact.getElement1() +
					" - #entries needed: "+ approx.getElement3() +"/"+ exact.getElement2() + 
					" - estimated error: "+ String.format("%2.4f", estimatedError / 100) +"%"+
					" - real error: "+ String.format("%2.4f", realError / 100) +"%");
		}		
	}
	
	/** Exact computation of one query.
	 * @return a pair <tt>(result, count)</tt> where count is the full number of entries satisfying the range query.
	 */
	public static Pair<Double,Integer> exact1(RSTree_v3<Integer, Pair<Integer, Double>, Long> tree, int key_lo, int key_hi) {
//		double resultExact = (double) Cursors.last(new Aggregator(exactVals, new StatefulAverage()));
		// exact computation
		Cursor<Pair<Integer, Double>> exactQuery = tree.rangeQuery(key_lo, key_hi);
		Cursor<Double> exactVals = new Mapper<Pair<Integer,Double>, Double>(FunJ8.toOld(e -> e.getSecond()), exactQuery);
		
		AggregationFunction<Number, Number> avgAggFun = new StatefulAverage();
		Double agg = null;
		int i=0;
		for(; exactVals.hasNext(); i++) {
			double val = exactVals.next();
			agg = (Double) avgAggFun.invoke(agg, val);
		}
		int valuesUsed = i;
		return new Pair<Double, Integer>(agg, valuesUsed);
	}
	
	public static Triple<Double, Double, Integer> approx1(RSTree_v3<Integer, Pair<Integer, Double>, Long> tree, int key_lo, int key_hi) {
		int REPORT_INTERVAL = 1000;		
		
		Cursor<Double> vals = new Mapper<Pair<Integer,Double>, Double>(
				FunJ8.toOld(Pair::getSecond), 
				tree.samplingRangeQuery(key_lo, key_hi));
		
		int i = 0;
		ConfidenceAggregationFunction coAggFun = ConfidenceAggregationFunction.largeSampleConfidenceAverage(CONFIDENCE);
		Double agg = null;
		double eps = Double.POSITIVE_INFINITY;
		double relativeError = Double.POSITIVE_INFINITY;		
		int MIN_ITERATIONS = 30; // Rule of thumb when CLT can be applied
		for(; relativeError > PRECISION_BOUND || i < MIN_ITERATIONS; i++) {
			double nVal = vals.next();
			agg = (double) coAggFun.invoke(agg, nVal);
			eps = (double) coAggFun.epsilon();
			relativeError = eps / agg;
//			if(i % REPORT_INTERVAL == 0) {
//				System.out.println(i + ":\tval: " + nVal + "\t agg: " + agg + 
//						"\t eps: "+ eps +"\t relError: "+ String.format("%3.3f", (Math.abs(relativeError) / 100)) +"%");
//			}			
		}
		int valuesSampled = i;
		
		return new Triple<Double, Double, Integer>(agg, relativeError, valuesSampled);
	}
	
	/** Tests the SamplingCursor for correctness regarding not producing false positives. */
	public static Triple<Integer, Integer, Integer> samplingCursorCorrectness(
					RSTree_v3<Integer, Pair<Integer, Double>, Long> tree, 
					Map<Integer,Pair<Integer, Double>> compmap, 
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
			Integer lo = random.nextInt();
			Integer hi = random.nextInt();
			if(lo > hi) { int tmp = lo; lo = hi; hi = tmp; }
			long possKeys = (long)hi - (long)lo + 1;
			
			System.out.println("Range Query #"+ i +": "+ lo +" - "+ hi +" (#possKeys: "+ possKeys +"): ");
	
			//-- execute the query
			Cursor<Pair<Integer, Double>> sampCur = new Taker<Pair<Integer, Double>>(tree.samplingRangeQuery(lo, hi), SAMPLE_SIZE);			
			List<Pair<Integer, Double>> tRes = new ArrayList<Pair<Integer, Double>>(Cursors.toList(sampCur));
			
			System.out.println("T-result (#="+ tRes.size() +"): "+ tRes);
			
			//-- Test current query
			int e_negatives = 0;
			int e_positives = 0;
			
			//-- Tests for false positives
			for(Pair<Integer, Double> tVal : tRes)
				if(!compmap.containsKey(tree.getGetKey().apply(tVal))) e_positives++;
	
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
		random = new Random(55);
		RSTree_v3<Integer, Pair<Integer, Double>, Long> tree = createRSTree(fileName);		
		Map<Integer, Pair<Integer,Double>> compmap = fill(tree, NUMBER_OF_ELEMENTS);
		samplingCursorCorrectness(tree, compmap, 100, 1000);
		
//		s_pruning(tree);
//		s_generation(tree);
	}
}
