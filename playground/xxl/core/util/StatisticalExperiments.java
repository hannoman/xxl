package xxl.core.util;

import xxl.core.math.Maths;

public class StatisticalExperiments {

	/** Distribution function of the binomial distribution. */
	public static double binomiDF(int n, double p, int k) {
		return Math.pow(p,  k) * Math.pow(1-p, n-k) * Maths.binomialCoeff2(n, k);
	}
	

	/** For a given dataset <tt>U</tt> with <tt>gamma</tt> elements without duplicates,  <br>
	 * 		a WR-sample <tt>B</tt> of size <tt>beta</tt> drawn from <tt>U</tt><br>
	 * 		a WR-sample <tt>A</tt> of size <tt>alpha</tt> drawn from <tt>B</tt> (!)<br>
	 * and a denoted unspecified element <tt>u_i</tt> of <tt>U</tt> <br>
	 * compute the probability that <tt>A</tt> has <tt>k</tt> copies of <tt>u_i</tt>.<br>
	 * <br>
	 * Compare this with the binomial distribution <tt>B(alpha, 1/gamma)</tt> which would describe direct sampling.
	 */
	public static double resampleMultiplicityDF(int gamma, int beta, int alpha, int k) {
		double prob = 0;
		for (int j = 0; j <= beta; j++) {
			prob += binomiDF(beta, (double)1 / (double)gamma, j) * binomiDF(alpha, (double)j / (double)beta, k);
		}
		return prob;
	}
	
	public static void main(String[] args) {
		
		int gamma = 100;
		int beta = 100;
		int alpha = 10;
		
		for (int k = 0; k <= alpha; k++) {
			double directDraw = binomiDF(alpha, (double)1 / (double)gamma, k);
			double resampleDraw = resampleMultiplicityDF(gamma, beta, alpha, k);
			System.out.println("-- "+ k +": ");
			System.out.println("    direct: "+ String.format("%.10f", directDraw));
			System.out.println("  resample: "+ String.format("%.10f", resampleDraw));
		}
		
		
	}
	
	
}
