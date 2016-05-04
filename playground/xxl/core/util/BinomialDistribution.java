package xxl.core.util;

import java.util.LinkedList;

import xxl.core.math.Maths;
import xxl.core.util.random.ContinuousRandomWrapper;
import xxl.core.util.random.DiscreteRandomWrapper;
import xxl.core.util.random.JavaContinuousRandomWrapper;

public class BinomialDistribution implements DiscreteRandomWrapper {
		
	InversionPRNGforFiniteDistribution generator;
	
	public BinomialDistribution(int n, double p, ContinuousRandomWrapper rng) {
		super();
				
		//-- map the distribution function
		LinkedList<Double> dfMapped = new LinkedList<Double>();
		for(int k=0; k <= n; k++) {
			double prob = Maths.binomialCoeff2(n, k) * Math.pow(p, k) * Math.pow(1-p, n-k);
			dfMapped.add(prob);
		}
		
		//-- accumulate
		LinkedList<Double> cdfMapped = new LinkedList<Double>();
		double s = 0;
		for(int k=0; k <= n; k++) {
			s += dfMapped.get(k);
			cdfMapped.add(s);			
		}
		
		//-- validate
		double margin = 0.000000001; 
		assert (s > 1.0 - margin && s < 1.0 + margin);
		
		generator = new InversionPRNGforFiniteDistribution(cdfMapped, rng);
	}
	
	public BinomialDistribution(int n, double p) {
		this(n, p, new JavaContinuousRandomWrapper());
	}

	@Override
	public int nextInt() {
		return generator.nextInt();
	}

	/** small test. */
	public static void main(String[] args) {
		BinomialDistribution b1 = new BinomialDistribution(10, 0.3);
		for (int i = 0; i < 50; i++) {
			System.out.print(b1.nextInt() +", ");			
		}
	}
}
