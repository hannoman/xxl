package xxl.core.util;

import java.util.ArrayList;
import java.util.List;

import xxl.core.util.random.ContinuousRandomWrapper;
import xxl.core.util.random.DiscreteRandomWrapper;
import xxl.core.util.random.JavaContinuousRandomWrapper;
import xxl.core.util.random.JavaDiscreteRandomWrapper;

public class InversionPRNGforFiniteDistribution implements DiscreteRandomWrapper {
	
	ContinuousRandomWrapper rng;
	ArrayList<Double> cdf;
	
	public InversionPRNGforFiniteDistribution(List<Double> cdf, ContinuousRandomWrapper rng) {	
		this.rng = rng;		
		this.cdf = new ArrayList<Double>(cdf);
	}
	
	public InversionPRNGforFiniteDistribution(List<Double> cdf) {
		this(cdf, new JavaContinuousRandomWrapper());
	}

	@Override
	public int nextInt() {
		double quantile = rng.nextDouble();		
		int pos = HUtil.binFindR(cdf, quantile);
		return pos;
	}
	
	
	
}
