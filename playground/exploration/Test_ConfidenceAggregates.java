package exploration;

import java.util.Random;

import xxl.core.math.statistics.parametric.aggregates.ConfidenceAggregationFunction;

public class Test_ConfidenceAggregates {

	/** First test with XXL-implemented estimators with confidence intervals. */
	public static void main(String[] args) {
		int N_SAMPLES = 100000;
		int LO = 0;
		int HI = 10000;
		int REPORT_FREQUENCY = 20;
		double CONF = 0.05; // maximum probabilty of error

		int[] data = new int[] { 10, 10, 10, 0, 5, 2, 7, 70, 33, 15, 23, 7 };
		
//		int[] data = new int[N_SAMPLES];
//		Random rng = new Random();
//		for (int i = 0; i < data.length; i++)
//			data[i] = rng.nextInt(HI - LO) + LO;

		ConfidenceAggregationFunction largeSampleEst = ConfidenceAggregationFunction.largeSampleConfidenceAverage(CONF);
		ConfidenceAggregationFunction conservativEst = ConfidenceAggregationFunction.conservativeConfidenceAverage(CONF, LO, HI);
		//		ConfidenceAggregationFunction aggFun = ConfidenceAggregationFunction.deterministicConfidenceAverage(LO, HI, data.length);

		Number lsAgg = null;
		Number coAgg = null;
		System.out.println("-- for confidence value: " + largeSampleEst.confidence() + "/" + conservativEst.confidence());
		for (int i = 0; i < data.length; i++) {
			lsAgg = largeSampleEst.invoke(lsAgg, data[i]);
			coAgg = conservativEst.invoke(coAgg, data[i]);
			if (true || i % (N_SAMPLES / REPORT_FREQUENCY) == 0) {
				System.out.print((i + 1) + ":\tval: " + data[i] + "\t lsAgg: " + lsAgg + "\t\teps: " + largeSampleEst.epsilon());
				System.out.println("\tcoAgg: " + coAgg + "\t\teps: " + conservativEst.epsilon());
			}
		}

	}
}
