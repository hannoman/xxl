package xxl.core.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class Randoms {

	/** Generates a B(n,p) distributed random value through doing an Bernoulli-experiment n times. */
	public static int binomialDist(int n, double p, Random rng) {
		int sucesses = 0;
		for(int i=0; i < n; i++) {
			if(rng.nextDouble() <= p)
				sucesses++;
		}
		return sucesses;
	}
	
	/** For w = w[0] + w[1] + ... + w[l]
	 * draws values ds[0..l] according to the distributions:
	 * 		#ds[i] = B(toDraw - sum(ds[0], ..., ds[i-1]), w[i] / (w - sum(w[0], ..., w[i-1])))
	 * This ensures that <tt>ds</tt> always contains exactly <tt>toDraw</tt> elements.
	 * (Note that it is possible to draw more values from a container than it contains.) 
	 * <p>
	 * Ok, this is indeed a valid sampling scheme for the multinomial distribution.
	 * (See: Art B. Owen: Monte Carlo theory, methods and example - Ch.5 Random vectors and objects,
	 * URL: http://statweb.stanford.edu/~owen/mc/Ch-randvectors.pdf)
	 * </p><p>
	 * Not to get confused with "multivariate hypergeometrical distribution".
	 * </p><p>
	 * Possible algorithmic improvements include divide and conquer and reordering of groups
	 * to faciliate early abandoning.
	 * </p><p><h3>Time-complexity discussion</h3>
	 * Let <tt>F := Frontier</tt>.<br>
	 * The reference paper suggests a batched sampling scheme where we would generate |F| sample positions
	 * in one run, instead of doing a categorical experiment of rougly (as F is changing) |F| categories |F| times.
	 * This naturally leads to the multinomial distribution <tt>Mult(|F|, ws[1] / totalWeight, ..., ws[|F|] / totalWeight)</tt>.
	 * If we use the implementation here (equals Algorithm 5.1 of the book) we would need to generate |F| binomial distributed
	 * random variables, doubtfully being faster than log(|F|)*|F| .. but that remains to be checked.    
	 * </p>
	 * 
	 * @param ws relative weights of the groups
	 * @param toDraw amount of samples to draw in total
	 * @param rng random number generator
	 * @return ds - a list of integers summing to <tt>toDraw</tt> (see above). 
	 */
	public static ArrayList<Integer> multinomialDist(List<Integer> ws, int toDraw, Random rng) {
		int toDrawCopy = toDraw;
		
		int weightTotal = ws.stream().reduce(0, (x,y) -> x+y);		
		ArrayList<Integer> ds = new ArrayList<Integer>(ws.size());
		for (int i = 0; i < ws.size(); i++) {
			double relWeight = (double)ws.get(i) / (double)weightTotal;
			int drawn = binomialDist(toDraw, relWeight, rng); 
			ds.add(drawn);
			weightTotal -= ws.get(i);
			toDraw -= drawn;			
		}
		
		assert ds.stream().reduce(0, (x,y) -> x+y) == toDrawCopy;
		return ds;
	}
	
	/**
	 * Produce the indices of a WR-sample of size <tt>toDraw</tt> drawn from <tt>n</tt> elements.
	 * In ascending order with repitions.
	 */
	public static List<Integer> multinomialDistUnweighted(int n, int toDraw, Random rng) {
		int toDrawCopy = toDraw;
		
		int weightTotal = n;		
		List<Integer> ds = new LinkedList<Integer>();
		for (int i = 0; i < n; i++) {
			double relWeight = 1.0 / (double)(n-i);
			int drawn = binomialDist(toDraw, relWeight, rng); 
			for (int j = 0; j < drawn; j++)
				ds.add(i);						
			toDraw -= drawn;			
		}
		
		assert ds.stream().reduce(0, (x,y) -> x+y) == toDrawCopy;
		return ds;
	}
	
	
	public static void main(String[] args) {
		
//		ArrayList<Integer> ds = new ArrayList<Integer>(10);		
//		System.out.println(ds);
//		System.out.println(ds.size());
		
		List<Integer> ws = Arrays.<Integer>asList(20,5,10,3,3,2,30);
		System.out.println(ws +" - sum: "+ ws.stream().reduce(0, (x,y) -> x+y) );
		
		Random rng = new Random();
		for (int i = 0; i < 30; i++) {
			System.out.println(multinomialDist(ws, 10, rng));
		}
		
		
	}
}
