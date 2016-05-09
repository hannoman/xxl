package xxl.core.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import xxl.core.cursors.filters.Taker;
import xxl.core.cursors.sources.Permutator;

public class Sample {

	/**
	 * Draws _amount_ values at random from the list given, deleting it from the list.  
	 * @param <V>
	 * @param universe
	 * @param amount
	 * @return
	 */
	public static <V> List<V> worRemove(List<V> universe, int amount, Random rng) {
		List<V> yield = new ArrayList<V>(amount);
		
		for(int idx : worIdx(universe.size(), amount, rng)) {
			yield.add(universe.remove(idx + yield.size()));
		}
		return yield;
	}
	
	/**
	 * Draws _amount_ values at random from the list given. Values stay in the list.
	 */
	public static <V> List<V> worKeep(List<V> universe, int amount, Random rng) {
		List<V> yield = new ArrayList<V>(amount);
		
		for(int idx : worIdx(universe.size(), amount, rng)) {
			yield.add(universe.get(idx));
		}
		return yield;
	}
	
	/**
	 * Draws _amount_ values at random (with replacement) from the list given. Values stay in the list.
	 * FIXME: This is incredibly overcomplicated. :( 
	 * (Perhaps could be useful if we don't have random access to the sampling universe.)
	 * -> Also, correctness has still to be shown.
	 */
	public static <V> List<V> wrKeep(List<V> universe, int amount, Random rng) {
		List<V> yield = new ArrayList<V>(amount);
		
		for(int idx : Randoms.multinomialDistUnweighted(universe.size(), amount, rng)) {
			yield.add(universe.get(idx));
		}
		return yield;
	}
	
	/**
	 * Draws <tt>sampleSize</tt> values at random (with replacement) from the list given. Values stay in the list.
	 * Sample is in random order (so no additional permutation has to be done.)
	 * Simple version which just draws one value after the other.
	 */
	public static <V> List<V> wrKeepSimple(List<V> universe, int sampleSize, Random rng) {
		List<V> yield = new ArrayList<V>(sampleSize);
		
		for (int i = 0; i < sampleSize; i++) {
			int idx = rng.nextInt(universe.size());
			yield.add(universe.get(idx));
		}
		
		return yield;
	}
	
	/**
	 * Draws <tt>k</tt> values at random without replacement from the range {0, ..., n-1}.
	 * This implementation doesn't use Fisher-Yates shuffle, but uses a {@link Set} to identify already generated
	 * values and reject them. It's therefore probabilistic. Should be more efficient if k << n. 
	 * 
	 * Note: This could also be achieved using a {@link Taker} cursor after a {@link Permutator} cursor on an array
	 * containing the indices (using Fisher-Yates shuffle in that case).
	 *   
	 * @param n specifying the range {0, ... , n-1}
	 * @param k amount of samples to draw
	 */
	public static List<Integer> worIdx(int n, int k, Random rng) {
		TreeSet<Integer> set = new TreeSet<Integer>();		
		
		while(set.size() < k) {
			int next = rng.nextInt(n - set.size());
			if(!set.contains(next))
				set.add(next);
		}
		
		// give result in sorted order
		List<Integer> generated = new ArrayList<Integer>(set.size());
		for(Integer x : set)
			generated.add(x);
		return generated;		
	}
	
}
