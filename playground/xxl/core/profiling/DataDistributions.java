package xxl.core.profiling;

import java.util.Random;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.spatial.rectangles.FixedPointRectangle;
import xxl.core.util.Pair;

public class DataDistributions {

	/** Randomly created data set 1: Payload correlated with the key: payload ~ NormalDistribution(key*key, key) */ 
	public static Cursor<Pair<Integer, Double>> squarePairs(Random rng, int KEY_LO, int KEY_HI, double VAL_LO, double VAL_HI) {
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
	public static Cursor<Pair<Integer, Double>> iidUniformPairsIntDouble(Random rng, int KEY_LO, int KEY_HI, double VAL_LO, double VAL_HI) {
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
	public static Cursor<Pair<Integer, Double>> pathologicalTwoPeaks(Random rng, int KEY_LO, int KEY_HI, double VAL_LO, double VAL_HI) {
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
	
	/** Constructs random rectangles specified by the <tt>dimension</tt> and the <tt>bitsPerDimension</tt>.
	 * 
	 *  e.g. dimension = 2; bitsPerDimension = 15 --> hilbert value <= 2**60
	 * 		 dimension = 3; bitsPerDimension = 10 --> hilbert value <= 2**60
	 */
	public static Cursor<FixedPointRectangle> rectanglesRandom(Random rng, int dimension, int bitsPerDimension) {
		int[] bitsPerDimensions = new int[dimension];
		for(int i=0; i < dimension; i++)
			bitsPerDimensions[i] = bitsPerDimension;
		return rectanglesRandom(rng, bitsPerDimensions);
	}
	
	/** Constructs random rectangles with bitsPerDimensions[i] bits in dimension i. 
	 * (Can be different between dimensions.) */
	public static Cursor<FixedPointRectangle> rectanglesRandom(Random rng, int[] bitsPerDimensions) {
		return new AbstractCursor<FixedPointRectangle>() {
			@Override
			protected boolean hasNextObject() { return true; }
			
			@Override
			protected FixedPointRectangle nextObject() {
				long[] leftCorner = new long[bitsPerDimensions.length], rightCorner = new long[bitsPerDimensions.length];
				long tmp;
				for(int i=0; i < bitsPerDimensions.length; i++) {
					// TODO: replace with "nextLong(bound)" nextInt is not sufficient
					leftCorner[i] = rng.nextInt(1 << bitsPerDimensions[i]); 
					rightCorner[i] = rng.nextInt(1 << bitsPerDimensions[i]);
					if(leftCorner[i] > rightCorner[i]) {
						tmp = rightCorner[i]; rightCorner[i] = leftCorner[i]; leftCorner[i] = tmp;
					}
				}
				FixedPointRectangle rect = new FixedPointRectangle(leftCorner, rightCorner);
				return rect;
			}
		};
	}
	
	/** Describes the domain of generated rectangles for rectanglesRandom(...) as FixedPointRectangle. */
	public static FixedPointRectangle universeForBitsPerDimensions(int[] bitsPerDimensions) {
		long[] left = new long[bitsPerDimensions.length];
		for (int i = 0; i < left.length; i++) {
			left[i] = 0;
		}
		
		long[] right = new long[bitsPerDimensions.length];
		for (int i = 0; i < right.length; i++) {
			right[i] = 1 << bitsPerDimensions[i];
		}
		
		return new FixedPointRectangle(left, right);
	}
}
