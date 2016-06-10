
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
	 * @param remLeft number of elements to keep in the list to split. targetList gets the elements from <b><tt>remLeft</tt></b> up to <b><tt>inList.size() - 1</tt></b>
	 * @param targetList the (already instanciated list) where the split off elements should be added to
	 * @return the modified <tt>targetList</tt>
	 */
	public static <E, O extends List<E>> O splitOffRight(List<E> inList, int remLeft, O targetList) {
		List<E> transferPart = inList.subList(remLeft, inList.size());
		targetList.addAll(transferPart);
		transferPart.clear();
		return targetList;
	}
	
	/** Splits off the left part of a list and appends it to another given list.
	 * 
	 * @param inList the list to split
	 * @param takeLeft number of elements to take into targetList. inList keeps the elements a[takeLeft], ..., a[#inList-1].
	 * @param targetList the (already instanciated list) where the split off elements should be added to
	 * @return the modified <tt>targetList</tt>
	 */
	public static <E, O extends List<E>> O splitOffLeft(List<E> inList, int takeLeft, O targetList) {
		List<E> transferPart = inList.subList(0, takeLeft);
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
		int pos = binFindSomeInsertionPoint(list, key);
		while(pos < list.size() && list.get(pos).equals(key))
			pos++;
		return pos;
	}
	
	/** Finds the position <tt>i</tt> so that <tt>A[i-1] < key <= A[i]</tt>.
	 * 
	 * @param list sorted list
	 * @param key key to insert, respectively lookup.
	 * @return position <tt>i</tt>
	 */
	public static <T> int binFindSE(List<? extends Comparable<? super T>> list, T key) {
		int pos = binFindSomeInsertionPoint(list, key);
		while(pos >= 1 && list.get(pos-1).equals(key))
			pos--;
		return pos;
	}

	/** Wrapper for {@link Collections#binarySearch}.
	 * Note that Collections.binarySearch stops as soon as it finds an equal element. 
	 * The javadoc-comment for its return type is kinda misleading.
	 */
	private static <T> int binFindSomeInsertionPoint(List<? extends Comparable<? super T>> list, T key) {
		int rawpos = Collections.binarySearch(list, key);		
		if(rawpos < 0)
			return -(rawpos + 1);
		else
			return rawpos;
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
		int valuesIdx = -1;
		V value = null;
		
		int lastIdx = -1;
		for(int idx : idxs) {
			assert idx >= lastIdx; lastIdx = idx; // ensure that the index list is sorted
			while(valuesIdx < idx) {
				value = valueIter.next();
				valuesIdx++;
			}
			taken.add(value);
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
	
	/** Removes all specified positions (which msut be given in ascending order) from the list. 
	 * @param <V>*/
	public static <V> void removeAll(List<Integer> idxs, List<V> values) {
		int removed = 0;
		for(int idx : idxs) {
			values.remove(idx-removed);
			removed++;
		}
	}
	

	
}
