
package xxl.core.util;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.OptionalInt;
import java.util.RandomAccess;
import java.util.Arrays;

public class HUtil {
	/** Shortcut for int-based exponentiation. 
	 * No checking of overflows is performed. */
	public static int intPow(int a, int b) {
		return (int)Math.pow(a, b);		
	}
	
	/** Splits off the right part of a list and appends it to another given list.
	 * 
	 * @param inList the list to split
	 * @param remLeft number of elements to keep in the list to split. targetList gets the elements from <b><tt>remLeft</tt></b> up to <b><tt>inList.size()</tt></b>
	 * @param targetList the (already instanciated list) where the split off elements should be added to
	 * @return
	 */
	public static <E, O extends List<E>> O splitOffRight(List<E> inList, int remLeft, O targetList) {
		List<E> transferPart = inList.subList(remLeft, inList.size());
		targetList.addAll(transferPart);
		transferPart.clear();
		return targetList;
	}
	
	/** Finds the position <tt>i</tt> so that <tt>A[i-1] <= key < A[i]</tt>.
	 * 
	 * @param list sorted list A
	 * @param key key to insert, respectively lookup.
	 * @return position <tt>i</tt>
	 */
	public static <T> int binFindES(List<? extends Comparable<? super T>> list, T key) {
		int rawpos = Collections.binarySearch(list, key);		
		if(rawpos < 0)
			return -(rawpos + 1);
		else
			return rawpos;
	}
	
	/** Finds the position <tt>i</tt> so that <tt>A[i-1] < key <= A[i]</tt>.
	 * 
	 * @param list sorted list
	 * @param key key to insert, respectively lookup.
	 * @return position <tt>i</tt>
	 */
	public static <T> int binFindSE(List<? extends Comparable<? super T>> list, T key) {
		int pos = binFindES(list, key);
		while(pos >= 1 && list.get(pos-1).equals(key))
			pos--;
		return pos;
	}
	
	public static <T, C extends Comparable<? super T>> int binFindL(C[] arr, T key) {
		return binFindES(Arrays.asList(arr), key);
	}
	
	public static <T, C extends Comparable<? super T>> int binFindR(C[] arr, T key) {
		return binFindSE(Arrays.asList(arr), key);
	}
	
	public static int[] distributeByAbsoluteWeights(int toDistribute, List<Integer> weights) {
		int remaining = toDistribute;
		
		//-- compute total weight
		long totalWeight = 0;
		for(int w : weights) totalWeight += w;
		
		//-- distribute
		int[] assigned = new int[weights.size()];
		for(int i=0; i < weights.size(); i++) {
			int curAssign = (int) Math.round(remaining * (double) weights.get(i) / (double) totalWeight);
			assigned[i] = curAssign;
			remaining -= curAssign;
			totalWeight -= weights.get(i);
		}
		
		//-- verify
		int distributed = Arrays.stream(assigned).reduce(0, (x, y) -> x+y); 
		assert distributed == toDistribute;
		
		return assigned;		
	}
	
	public static int[] distributeByRelativeWeights(int toDistribute, List<Double> weights) {
		int remaining = toDistribute;
		double remWeight = 1.0;
		
		int[] assigned = new int[weights.size()];
		for(int i=0; i < weights.size(); i++) {
			int curAssign = (int) Math.round(remaining * weights.get(i) / remWeight);
			assigned[i] = curAssign;
			remaining -= curAssign;
			remWeight -= weights.get(i);			
		}
		
		//-- verify
		int distributed = Arrays.stream(assigned).reduce(0, (x, y) -> x+y); 
		assert distributed == toDistribute;
		
		return assigned;
	}
	
	/**
	 * Takes all values specified by the given <tt>idxs</tt> (which must be in ascending order) 
	 * from <tt>values</tt> through the use of a ListIterator.
	 */
	public static <V> LinkedList<V> getAll_SequentialAccess(List<Integer> idxs, List<V> values) {
		LinkedList<V> taken = new LinkedList<V>();
		ListIterator<V> valueIter = values.listIterator();
		for(int idx : idxs) {
			for (int i = 0; i < idx; i++) 
				valueIter.next();
			taken.add(valueIter.next());
		}
		return taken;		
	}

	
	/**
	 * Takes all values specified by the given <tt>idxs</tt> 
	 * from <tt>values</tt> through random access.
	 */
	public static <V> LinkedList<V> getAll_RandomAccess(List<Integer> idxs, List<V> values) {
		LinkedList<V> taken = new LinkedList<V>();		
		for(int idx : idxs)
			taken.add(values.get(idx));
		return taken;		
	}
	
	/**
	 * Takes all values specified by the given <tt>idxs</tt> from <tt>values</tt>.
	 */
	public static <V> LinkedList<V> getAll(List<Integer> idxs, List<V> values) {
		if(values instanceof RandomAccess)
			return getAll_RandomAccess(idxs, values);
		else
			return getAll_SequentialAccess(idxs, values);
	}
	
	

	
}
