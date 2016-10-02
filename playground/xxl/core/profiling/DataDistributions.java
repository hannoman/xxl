package xxl.core.profiling;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.spatial.rectangles.FixedPointRectangle;
import xxl.core.util.CopyableRandom;
import xxl.core.util.Interval;
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
	 * In expectation they cover a third of the space in every dimension!
	 * 
	 *  e.g. dimension = 2; bitsPerDimension = 15 --> hilbert value <= 2**60
	 * 		 dimension = 3; bitsPerDimension = 10 --> hilbert value <= 2**60
	 */
	public static Cursor<FixedPointRectangle> rectanglesRandomThird(Random rng, int dimension, int bitsPerDimension) {
		int[] bitsPerDimensions = new int[dimension];
		for(int i=0; i < dimension; i++)
			bitsPerDimensions[i] = bitsPerDimension;
		return rectanglesRandomThird(rng, bitsPerDimensions);
	}
	
	/** Constructs random rectangles with bitsPerDimensions[i] bits in dimension i.
	 * In expectation they cover a third of the space in every dimension! 
	 * (Can be different between dimensions.) */
	public static Cursor<FixedPointRectangle> rectanglesRandomThird(Random rng, int[] bitsPerDimensions) {
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
	
	public static Cursor<FixedPointRectangle> rectanglesRandomVolumed(Random rng, int[] bitsPerDimensions, double relVolumeExpected) {
		double volumeExpected = universeForBitsPerDimensions(bitsPerDimensions).area() * relVolumeExpected;
		
		return new AbstractCursor<FixedPointRectangle>() {
			@Override
			protected boolean hasNextObject() { return true; }
			
			@Override
			protected FixedPointRectangle nextObject() {
				//- Zentrum gleichverteilt wählen und Rohdimensionen unabhängig voneinander gleichverteilt wählen
				long[] center = new long[bitsPerDimensions.length];
				long[] rawDim = new long[bitsPerDimensions.length];
				for(int i=0; i < bitsPerDimensions.length; i++) {
					center[i] = rng.nextInt(1 << bitsPerDimensions[i]);
					rawDim[i] = 1 + rng.nextInt((1 << bitsPerDimensions[i])-1);
				}
				
				//- Skalierungsfaktor bestimmen
				double gotVolume = 1.0;
				for(int i=0; i < bitsPerDimensions.length; i++)
					gotVolume *= rawDim[i];
				double scalingFactor = Math.pow(volumeExpected / gotVolume, 1 / (double) bitsPerDimensions.length);
				
				//- Skalieren und Würfel am Rande des Universums beschneiden
				double[] scaledDims = new double[bitsPerDimensions.length];
				long[] leftCorner = new long[bitsPerDimensions.length], rightCorner = new long[bitsPerDimensions.length];
				for(int i=0; i < bitsPerDimensions.length; i++){
					scaledDims[i] = rawDim[i] * scalingFactor;
					leftCorner[i] = Math.max(0, center[i] - Math.round(scaledDims[i]/2) );
					rightCorner[i] = Math.min((1 << bitsPerDimensions[i])-1, center[i] + Math.round(scaledDims[i]/2) );					
				}				
				
				FixedPointRectangle rect = new FixedPointRectangle(leftCorner, rightCorner);
				return rect;
			}
		};
	}
	
	public static Cursor<FixedPointRectangle> rectanglesRandomParetoVolumed(Random rng, int[] bitsPerDimensions, double relVolumeExpected, double shape) {
		double volumeExpected = universeForBitsPerDimensions(bitsPerDimensions).area() * relVolumeExpected;
		
		return new AbstractCursor<FixedPointRectangle>() {
			@Override
			protected boolean hasNextObject() { return true; }
			
			@Override
			protected FixedPointRectangle nextObject() {
				//- Zentrum gleichverteilt wählen und Rohdimensionen unabhängig voneinander gleichverteilt wählen
				
				long[] center = new long[bitsPerDimensions.length];
				long[] rawDim = new long[bitsPerDimensions.length];
				for(int i=0; i < bitsPerDimensions.length; i++) {
					center[i] = Math.round(paretoBounded(rng, shape, 1, (1 << bitsPerDimensions[i]) - 1));
					rawDim[i] = 1 + rng.nextInt((1 << bitsPerDimensions[i])-1);
				}
				
				//- Skalierungsfaktor bestimmen
				double gotVolume = 1.0;
				for(int i=0; i < bitsPerDimensions.length; i++)
					gotVolume *= rawDim[i];
				double scalingFactor = Math.pow(volumeExpected / gotVolume, 1 / (double) bitsPerDimensions.length);
				
				//- Skalieren und Würfel am Rande des Universums beschneiden
				double[] scaledDims = new double[bitsPerDimensions.length];
				long[] leftCorner = new long[bitsPerDimensions.length], rightCorner = new long[bitsPerDimensions.length];
				for(int i=0; i < bitsPerDimensions.length; i++){
					scaledDims[i] = rawDim[i] * scalingFactor;
					leftCorner[i] = Math.max(0, center[i] - Math.round(scaledDims[i]/2) );
					rightCorner[i] = Math.min((1 << bitsPerDimensions[i])-1, center[i] + Math.round(scaledDims[i]/2) );					
				}				
				
				FixedPointRectangle rect = new FixedPointRectangle(leftCorner, rightCorner);
				return rect;
			}
		};
	}
	
	/** So wie ich die Erklärung von Herrn Seeger verstanden habe..., aber das haut nicht hin. */
	public static long pareto1D(Random rng, int bits, double skewness) {
		long x = 0L;
		for(int i=0; i < bits; i++){
			if(rng.nextDouble() >= skewness)
				x += 1;
			x <<= 1;
		}
		x >>= 1;
		return x;
	}
	
	/** Generation of random variables from the Bounded-Pareto distribution.
	 * 
	 * @param rng
	 * @param shape shape parameter > 0
	 * @param l minimum value >= 1
	 * @param h maximum value
	 * @return
	 * 
	 * See https://en.wikipedia.org/wiki/Pareto_distribution#Bounded_Pareto_distribution
	 */
	public static double paretoBounded(Random rng, double shape, double l, double h) {
		double u = rng.nextDouble();
		double a = shape;
	    double x = Math.pow((-u*(Math.pow(h,a) - Math.pow(l,a)) + Math.pow(h,a)) / Math.pow(h*l,a), (-1.0/a));
	    assert(x <= h);
	    return x;	
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

	public static Cursor<Interval<Integer>> intervalsInteger(CopyableRandom rng, final int lo, final int hi) {
		return new AbstractCursor<Interval<Integer>>() {
			@Override
			protected boolean hasNextObject() { return true; }
			@Override
			protected Interval<Integer> nextObject() {
				int a = lo + rng.nextInt(hi - lo + 1);
				int b = lo + rng.nextInt(hi - lo + 1);
				if(a > b) { int tmp = a; a = b; b = tmp; }
				return new Interval<Integer>(a,b);
			}
		};
	}
	
	
	public static void main(String[] args) {
		Random rng = new Random();
		int nVals = 100;
		ArrayList<Long> vals = new ArrayList<Long>(nVals);
		for(int i=0; i < 100; i++){
//			long x = pareto1D(rng, 5, 0.8); 
			long x = Math.round(paretoBounded(rng, 0.8, 1, 100));
			vals.add(x);
		}
		
		System.out.println(vals);
		Collections.sort(vals);
		System.out.println(vals);
	}
}
