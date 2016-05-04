package xxl.core.util;

import java.util.Random;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Randoms {

	
	public static int binomialDist(int n, double p, Random rng) {
		int sucesses = 0;
		for(int i=0; i < n; i++) {
			if(rng.nextDouble() <= p)
				sucesses++;
		}
		return sucesses;
	}
	
	public static ArrayList<Integer> multinomialDist(List<Integer> ws, Random rng) {
		int n_ = ws.stream().reduce(0, (x,y) -> x+y);
		int n = n_;
		ArrayList<Integer> ds = new ArrayList<Integer>();
		for (int i = 0; i < ws.size(); i++) {
			double relWeight = (double)ws.get(i) / (double)n;
			int drawn = binomialDist(n, relWeight, rng); 
			ds.set(i, drawn);
			n -= drawn;
		}
		
		assert ds.stream().reduce(0, (x,y) -> x+y) == n_;
		return ds;
	}
}
