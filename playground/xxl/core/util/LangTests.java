package xxl.core.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.google.uzaygezen.core.BitVector;
import com.google.uzaygezen.core.BitVectorFactories;
import com.google.uzaygezen.core.CompactHilbertCurve;

public class LangTests {

	public static void main(String[] args) {
//		main2();
//		hvToCoords();
//		bitManipulation();
//		randBits();
		
		toStringTests();
	}
	
	public static void toStringTests() {
		LinkedList<Long> list = new LinkedList<Long>();
		list.add(2L); list.add(17L); list.add(39L);
		Pair<Integer, List<Long>> p = new Pair<>(1335, list);
		System.out.println(p);
		
	}
	
	public static void randBits() {
		CopyableRandom rng = new CopyableRandom();
		for(int i=0; i < 1000; i++) {
			int x = rng.next(32);
			System.out.println(x);
		}
	}
	
	public static void bitManipulation() {
		int x = 1 << 31;
		int y = (1 << 32) - 1;
		int allOneI = (int) ((1L << 32) - 1);
		
		System.out.println("allOneI: "+ allOneI + " / \t"+ Integer.toBinaryString(allOneI));
		long allOneC = (long) allOneI;
		System.out.println("allOneC: "+ allOneC + " / \t"+ Long.toBinaryString(allOneC));
		long one32L = (1L << 32) - 1; 
		System.out.println("one32L: "+ one32L + " / \t"+ Long.toBinaryString(one32L));
		long allOneD = Integer.toUnsignedLong(allOneI);
		System.out.println("allOneD: "+ allOneD + " / \t"+ Long.toBinaryString(allOneD));
		
		System.out.println(x);
		System.out.println(y);
//		System.out.println( ((int)(17 << 30)) );
//		System.out.println( ((int)((1 << 30) + ((1 << 3) << 30))) );
//		System.out.println( ((int)((1 << 30) + (1 << 33))) );
//		System.out.println( ((int)((16 << 30))) );
//		System.out.println( ((int)(((1 << 3) << 30))) );
//		System.out.println( ((int)(((1 << 1) << 32))) );
//		System.out.println( ((int)((1 << 33))) );
//		System.out.println( ((int)((1L << 33))) );
		
		int hi = 17;
		long comp1 = ((long)hi << 32) + allOneI;
		long comp2pre = (((long)hi) << 32);
		long comp2 = (((long)hi) << 32) | ((long) allOneI);
		
		System.out.println("allOne: "+ allOneI + " / \t"+ Integer.toBinaryString(allOneI));
		System.out.println("comp1: "+ comp1 +" / \t"+ Long.toBinaryString(comp1));
		System.out.println("comp2p: "+ comp2pre +" / \t"+ Long.toBinaryString(comp2pre));
		System.out.println("comp2: "+ comp2 +" / \t"+ Long.toBinaryString(comp2));
		
		CopyableRandom rng = new CopyableRandom();
		long v = Integer.toUnsignedLong(rng.next(5));
		System.out.println(v);
		System.out.println(Long.toBinaryString(v));
	}
	
	public static void coordsToHVs()
    {
        System.out.println( "=== coordinates to hilbert values ===" );
        
        int[] aBitsPerDimension = {2,2};
        CompactHilbertCurve chc = new CompactHilbertCurve(aBitsPerDimension);
        List<Integer> bitsPerDimension = chc.getSpec().getBitsPerDimension();
        BitVector[] p = new BitVector[bitsPerDimension.size()];
        
        for (int i = p.length; --i >= 0;) {
            p[i] = BitVectorFactories.OPTIMAL.apply(bitsPerDimension.get(i));
        }
        BitVector chi = BitVectorFactories.OPTIMAL.apply(chc.getSpec().sumBitsPerDimension());
        
        int x, y;
        for(y=0; y < 1 << aBitsPerDimension[1]; y++) {
        	p[1].copyFrom(y);
        	for(x=0; x < 1 << aBitsPerDimension[0]; x++) {
        		p[0].copyFrom(x);
        		chc.index(p, 0, chi);
                System.out.println("("+ p[0].toLong() +","+ p[1].toLong() +") --> "+ chi.toLong());
        	}
        }
    }
    
    public static void hvToCoords()
    {
        System.out.println( "=== hilbert values to coordinates ===" );
        
        int[] aBitsPerDimension = {2,0,2};
        CompactHilbertCurve chc = new CompactHilbertCurve(aBitsPerDimension);
        List<Integer> bitsPerDimension = chc.getSpec().getBitsPerDimension();
        BitVector[] p = new BitVector[bitsPerDimension.size()];
        
        //- initialize bit vectors
        for(int i = 0; i < bitsPerDimension.size(); i++)
        	p[i] = BitVectorFactories.OPTIMAL.apply(bitsPerDimension.get(i));
        BitVector chi = BitVectorFactories.OPTIMAL.apply(chc.getSpec().sumBitsPerDimension());

        for(int hv=0; hv < 1 << chc.getSpec().sumBitsPerDimension(); hv++) {
        	chi.copyFrom(hv);
        	chc.indexInverse(chi, p);
        	System.out.print(chi.toLong() +" --> ");
        	System.out.print("(");
        	for(int j=0; j < p.length-1; j++)
        		System.out.print(p[j].toLong() +",");
        	System.out.println(p[p.length-1].toLong() +")");
        }
        
        
    }

	public static void main1(String[] args) {
		
		Integer[] a1 = {5,10,15};
//		List l1 = Arrays.asList(a1);
		List<Integer> ll = new ArrayList<Integer>(Arrays.asList(a1));
		
		int x;
//		x = 16;
//		System.out.println("Position for x = "+ x +" in "+ ll.toString() + ": "+ Collections.binarySearch(ll, x));
//		
//		int pos = Math.abs(Collections.binarySearch(ll, x));
//		ll.add(pos, x);
//		
//		System.out.println("Position for x = "+ x +" in "+ ll.toString() + ": "+ Collections.binarySearch(ll, x));
		
		x = 1;
		int mypos = HUtil.binFindES(ll, x);
		
		System.out.println("Position (old) for x = "+ x +" in "+ ll.toString() + ": "+ Collections.binarySearch(ll, x));
		System.out.println("Position (new) for x = "+ x +" in "+ ll.toString() + ": "+ HUtil.binFindES(ll, x));
		
		// KEEP IN MIND
		Integer a = new Integer(42); Integer b = new Integer(42);
		System.out.println("a == b: "+ (a == b) +"; a.equals(b): "+ (a.equals(b)));
		
		System.out.println("Java Version: "+ XXLSystem.getJavaVersion());
		
		System.out.print("XXLSystem.getRootPath(): ");
		try { System.out.println(XXLSystem.getRootPath()); } 
		catch (RuntimeException e) { System.out.println(e.toString()); }
		
		System.out.print("XXLSystem.getOutPath(): ");
		try { System.out.println(XXLSystem.getOutPath()); } 
		catch (RuntimeException e) { System.out.println(e.toString()); }
		
		System.out.print("XXLSystem.getDataPath(): ");
		try { System.out.println(XXLSystem.getDataPath(new String[0])); } 
		catch (RuntimeException e) { System.out.println(e.toString()); }
	}
	
	public static void main2() {
		System.out.print("Huhuhuhuhua");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.print("\b \b ramen");
	}
}
