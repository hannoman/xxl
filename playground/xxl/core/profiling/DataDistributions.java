package xxl.core.profiling;

import java.util.Random;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.util.Pair;

public class DataDistributions {

	/** Randomly created data set 1: Payload correlated with the key: payload ~ NormalDistribution(key*key, key) */ 
	public static Cursor<Pair<Integer, Double>> data_squarePairs(Random rng, int KEY_LO, int KEY_HI, double VAL_LO, double VAL_HI) {
		return new AbstractCursor<Pair<Integer,Double>>() {
			@Override
			protected boolean hasNextObject() { return true; }
			
			@Override
			protected Pair<Integer, Double> nextObject() {
				int key = KEY_LO + rng.nextInt(KEY_HI - KEY_LO);
				// double value = key * key + (rng.nextDouble() * 2 * key) - key; // overflowing variant, which leads to very high variance
				double value = (long)key * (long)key + (rng.nextDouble() * 2 * key) - key;
				return new Pair<Integer, Double>(key, value);
			}
		};		
	}

	/** Randomly created data set 2: key and data uncorrelated and uniformly distributed */ 
	public static Cursor<Pair<Integer, Double>> data_iidUniformPairsIntDouble(Random rng, int KEY_LO, int KEY_HI, double VAL_LO, double VAL_HI) {
		return new AbstractCursor<Pair<Integer,Double>>() {
			@Override
			protected boolean hasNextObject() { return true; }
			
			@Override
			protected Pair<Integer, Double> nextObject() {
				int key = KEY_LO + rng.nextInt(KEY_HI - KEY_LO);
				double value = VAL_LO + rng.nextDouble() * (VAL_HI - VAL_LO);
				return new Pair<Integer, Double>(key, value);
			}
		};		
	}

//	/** Randomly created data set 2: key and data uncorrelated and uniformly distributed */ 
//	public static Cursor<Integer> data_iidUniformInts(Random rng, int KEY_LO, int KEY_HI) {
//		return new AbstractCursor<Integer>() {
//			@Override
//			protected boolean hasNextObject() { return true; }
//			
//			@Override
//			protected Integer nextObject() {
//				int key = KEY_LO + rng.nextInt(KEY_HI - KEY_LO);
//				return new Integer(key);
//			}
//		};		
//	}
	
	/** Randomly created data set 3: pathological two peak distribution. -> high variance. */ 
	public static Cursor<Pair<Integer, Double>> data_pathologicalTwoPeaks(Random rng, int KEY_LO, int KEY_HI, double VAL_LO, double VAL_HI) {
		return new AbstractCursor<Pair<Integer,Double>>() {
			@Override
			protected boolean hasNextObject() { return true; }
			
			@Override
			protected Pair<Integer, Double> nextObject() {
				int key = KEY_LO + rng.nextInt(KEY_HI - KEY_LO);
				double value = rng.nextDouble() < 0.5d ? -VAL_HI : VAL_HI;
				return new Pair<Integer, Double>(key, value);
			}
		};		
	}

}
