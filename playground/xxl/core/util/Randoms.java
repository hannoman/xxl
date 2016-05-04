package xxl.core.util;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
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
	 * 		ds[i] = B(
	 * @param ws
	 * @param rng
	 * @return
	 */
	public static ArrayList<Integer> multinomialDist(List<Integer> ws, Random rng) {
		// TODO: this is awkwardly wrong.
		int n_ = ws.stream().reduce(0, (x,y) -> x+y);
		int n = n_;
		ArrayList<Integer> ds = new ArrayList<Integer>(ws.size());
		for (int i = 0; i < ws.size(); i++) {
			double relWeight = (double)ws.get(i) / (double)n;
			int drawn = binomialDist(n, relWeight, rng); 
			ds.add(drawn);
			n -= drawn;
		}
		
		assert ds.stream().reduce(0, (x,y) -> x+y) == n_;
		return ds;
	}
	
	public static void main(String[] args) {
		
//		ArrayList<Integer> ds = new ArrayList<Integer>(10);		
//		System.out.println(ds);
//		System.out.println(ds.size());
		
		List<Integer> ws = Arrays.<Integer>asList(20,5,10,3,3,2,30);
		System.out.println(ws);
		
		Random rng = new Random();
		for (int i = 0; i < 30; i++) {
			System.out.println(multinomialDist(ws, rng));
		}
		
		
	}
}
