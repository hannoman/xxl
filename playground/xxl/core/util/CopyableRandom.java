package xxl.core.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.math3.fraction.BigFraction;

import xxl.core.io.Convertable;

/** Improved version of {@link java.util.Random} with easy support for reading the state.
 * Based on: http://stackoverflow.com/a/18531276/2114486
 * 
 * The workhorse of this implementation is setSeed which also does the initialisation. 
 * This is required as we want to override this method and Random::new() and Random::new(long) 
 * both call setSeed(long), and we can't avoid the superclass' constructor calls.
 * 		
 * This implementation saves its own seed locally though, to make it visible. * 
 */
public class CopyableRandom extends Random implements Copyable<CopyableRandom>, Convertable {
	private static final long serialVersionUID = 1L;

//	private final AtomicLong seed = new AtomicLong(0L);
	private AtomicLong seed;

	private final static long multiplier = 0x5DEECE66DL;
	private final static long addend = 0xBL;
	private final static long mask = (1L << 48) - 1;
	private static volatile long seedUniquifier = 8682522807148012L;

	public CopyableRandom() {
		super();
//		this(++seedUniquifier + System.nanoTime());
	}

	public CopyableRandom(long seed) {
		super(seed);
//		this.seed.set((seed ^ multiplier) & mask);
	}
	
	/** Copy constructor. */
	public CopyableRandom(CopyableRandom other) {
		this(other.getSeed());
	}

	/* copy of superclasses code, as you can seed the seed changes */
	@Override
	public int next(int bits) {
		long oldseed, nextseed;
		AtomicLong seed_ = this.seed;
		do {
			oldseed = seed_.get();
			nextseed = (oldseed * multiplier + addend) & mask;
		} while (!seed_.compareAndSet(oldseed, nextseed));
		return (int) (nextseed >>> (48 - bits));
	}
	
	/** Self written methods that returns a long in the range [0, bound[ */
	public long nextLong(long bound) {
		assert bound > 0;
		
		int bitsNeeded = 64 - Long.numberOfLeadingZeros(bound - 1);
		long lo, hi; /* lo and hi half of the long */
		int loBitsNeeded = Math.min(bitsNeeded, 32);
		int hiBitsNeeded = Math.max(bitsNeeded - 32, 0);
		long v;
		do {
			lo = Integer.toUnsignedLong(next(loBitsNeeded));
			hi = Integer.toUnsignedLong(next(hiBitsNeeded));
			v = hi << 32 | lo;
		} while(v >= bound);
		return v;	
	}
	

	/* necessary to prevent changes to seed that are made in constructor */
	@Override
	public CopyableRandom copy() {
		return new CopyableRandom((seed.get() ^ multiplier) & mask);
	}

	@Override
	/** We definitely need to overwrite this method as otherwise the state of super().seed would change, 
	 * which doesnt have any impact on our implementation.
	 */
	public synchronized void setSeed(long seed) {		
		if(this.seed == null) {
			// System.out.println("Our nice super constructor forces us to do the initialisation here...");
			this.seed = new AtomicLong();
		}
		this.seed.set(initialScramble(seed));	
		// we nonetheless call super, to reset haveNextNextGaussian without needing access to it.
		super.setSeed(seed); // --> haveNextNextGaussian = false; 
	}

	/** As Random does initialScramble on the passed seed we need to "unscramble" it before. */
	public long getSeed() {
		return initialUnscramble(seed.get());
	}

	/** Transferred copy. */	
	private static long initialScramble(long seed) {
        return (seed ^ multiplier) & mask;
    }
	
	/** Inverse for {@link java.util.Random.initialScramble(long)} */
	private static long initialUnscramble(long seed) {
		return seed ^ multiplier;
	}

	/** Returns the raw seed for this PRNG. As Random does not have any method to set it in raw mode use {@link getSeed()} instead. */
	protected long getRawState() {
		return seed.get();
	}

	
	//----- Converter methods
	@Override
	public void read(DataInput dataInput) throws IOException {
		// this.setSeed(dataInput.readLong());
		this.seed.set(dataInput.readLong());
	}
	
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(this.getSeed());
	}
	
	
	
	public static void main(String[] args) {
//		tests_messy();
		test_longBoundedGenerator();
	}
	
	/** Messy tests. */
	public static void tests_messy() {
	    CopyableRandom cr = new CopyableRandom(22);
	    Random rng = new Random(22);
	    
	    /* changes intern state of cr */
	    for (int i = 0; i < 10; i++) {
	      System.out.println("CR: "+ cr.nextInt(50) +", JR: "+ rng.nextInt(50));
	    }
	    CopyableRandom copy = cr.copy();
	
	    System.out.println("\nTEST: INTEGER\n");
	    for (int i = 0; i < 10; i++)
	      System.out.println("CR\t= " + cr.nextInt(50) + "\nCOPY\t= " + copy.nextInt(50) +"\nDEF\t= " + rng.nextInt(50) +"\n");
	
	    System.out.println("-- Join from read state:");
	    Random rngNewFork = new Random(); rngNewFork.setSeed(cr.getSeed());
	    Random rngNewCons = new Random(cr.getSeed());
	    
	    for (int i = 0; i < 10; i++) {
		      System.out.println("CR\t= " + cr.nextInt(50));
		      System.out.println("newFork\t= " + rngNewFork.nextInt(50));
		      System.out.println("newCons\t= " + rngNewCons.nextInt(50));
	    }
	    
	    long s1 = cr.getSeed();
	    cr.setSeed(cr.getSeed());
	    long s2 = cr.getSeed();
	    System.out.println("involutive: "+ s1 +", "+ s2 +", "+ (s1==s2));
	    
	}
	
	// CHECK: hmm, is this really correct? Results seem kinda fishy.
	public static void test_longBoundedGenerator() {		
		CopyableRandom crng = new CopyableRandom();
		int reps = 10000;
		long bound = Long.MAX_VALUE;
		long max = Long.MIN_VALUE, min = Long.MAX_VALUE;
		for (int i = 0; i < reps; i++) {
			long v = crng.nextLong(bound);
			System.out.println(v);
			if(v > max)
				max = v;
			if(v < min)
				min = v;
		}
		System.out.println();
		System.out.println("bound: \t"+ bound);
		System.out.println("max: \t"+ max);
		System.out.println("min: \t"+ min);
		BigInteger domainSize = BigInteger.valueOf(max - min + 1);
		BigInteger totalDomainSize = BigInteger.valueOf(bound);
		BigFraction pSingle = new BigFraction(domainSize, totalDomainSize);
		BigFraction pTotal = pSingle.pow(reps);
		System.out.println("probability to get this effective domain size or a smaller one: "+ pTotal.percentageValue() +"%");
//		7160696852034835611
//		9223372036854775807 
	}
	
}