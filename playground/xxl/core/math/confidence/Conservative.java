package xxl.core.math.confidence;

/** Static functions for computing conservative confidence intervals (those are the ones where boundaries of the 
 * universe must be known. 
 * 
 * (see "Online Aggregation - Hellerstein et al. (1997)" page 7) */
public class Conservative {
	
	public static double average(double confidence, int nSeen, double lo, double hi) {
		return (hi - lo) * Math.sqrt( Math.log(2 / (1 - confidence)) / (2*nSeen) );
	}
	
	
}
