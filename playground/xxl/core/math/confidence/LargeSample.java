package xxl.core.math.confidence;

/** Static functions for computing large-sample confidence intervals (from WoR samples?). 
 * Those functions assume that:
 * 		- the sample size is in proportion to the population size small enough to treat the sample 
 * 			as drawn WR reasonably well.
 * 		- the sample size is large enough to apply the CLT. 
 * 
 * (see "Online Aggregation - Hellerstein et al. (1997)" page 7) */
public class LargeSample {
	
	public static double average(double confidence, int nSeen, double lo, double hi) {
		return (hi - lo) * Math.sqrt( Math.log(2 / (1 - confidence)) / (2*nSeen) );
	}
	
	
}
