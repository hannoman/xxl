
package xxl.core.util;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.OptionalInt;
import java.util.RandomAccess;
import java.util.ArrayList;
import java.util.Arrays;

public class HUtil {
	/** Shortcut for int-based exponentiation. 
	 * No checking of overflows is performed. */
	public static int intPow(int a, int b) {
		return (int)Math.pow(a, b);		
	}
	
	
	/** Partitions a number into n approximately equal parts. */
	public static List<Integer> partitionInNParts(int amount, int n) {
		int remaining = amount;
		List<Integer> parts = new ArrayList<Integer>(n);
		for(int i = n; i >= 1; i--) {
			int curPart = remaining / i;
			parts.add(curPart);
			remaining -= curPart;
		}
		assert parts.stream().reduce(0, (x,y) -> x+y) == amount; 
		return parts;
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
	
	

	/** Funktionierende Variante. Allerdings ineffizient da keine dynamische Programmierung verwendet wird. 
	 *  
	 * @param nBins Anzahl der zu füllenden Buckets.
	 * @param binLo Minimales Gewicht eines Buckets (inklusiv). 
	 * @param binHi Maximales Gewicht eines Buckets (exklusiv).
	 * @param weights Gewichte der einzelnen Elemente
	 * @param idxOffset nur intern genutzt. Immer mit idxOffset = 0 aufrufen.
	 * @return
	 */
	public static List<LinkedList<Integer>> packBinsRecursive(int nBins, int binLo, int binHi, List<Integer> weights, int idxOffset) {
		
		LinkedList<LinkedList<Integer>> results = new LinkedList<LinkedList<Integer>>();
		
		if(nBins == 1) {
			int restSum = weights.stream().reduce(0, ((x,y) -> x+y));
			if(restSum >= binLo && restSum <= binHi) {
				LinkedList<Integer> idxsHi = new LinkedList<Integer>();
				idxsHi.add(weights.size()-1 + idxOffset);
				results.add((LinkedList<Integer>) idxsHi.clone());
			}
		} else {
			int i = 0, curBinWeight = 0;
			try {
				while(curBinWeight < binLo) {
					curBinWeight += weights.get(i); // chance for Exception here
					i++;
				}
				
				while(curBinWeight <= binHi) {
					// compute recursive results
					List<LinkedList<Integer>> recursiveResults = packBinsRecursive(nBins-1, binLo, binHi, weights.subList(i, weights.size()), idxOffset+i);
					for(LinkedList<Integer> recursiveResult : recursiveResults) {
						LinkedList<Integer> idxsHi = new LinkedList<Integer>();
						idxsHi.add(i-1 + idxOffset);
						idxsHi.addAll(recursiveResult);
						results.add(idxsHi);					
					}
					
					// expand current bin
					curBinWeight += weights.get(i); // chance for Exception here
					i++;
				}
			} catch(IndexOutOfBoundsException e) {
				// just catch the exception, we don't need to do anything. The Exception just signals the end of valid results.  
			}
		}
		
		return results;
	}

	
	/** Mit dynamischer Programmierung. 
	 *  
	 * @param nBins Anzahl der zu füllenden Buckets.
	 * @param binLo Minimales Gewicht eines Buckets (inklusiv). 
	 * @param binHi Maximales Gewicht eines Buckets (exklusiv).
	 * @param weights Gewichte der einzelnen Elemente
	 * @return
	 */
	public static Pair<LinkedList<Integer>, Integer> packBinsDP(int nBins, int binLo, int binHi, List<Integer> weights) {
		
		Integer[][] d = new Integer[weights.size()][nBins+1];
		Integer[][] lastPartSize = new Integer[weights.size()][nBins+1];
		
		//- Initialisierung der Abbruchfälle
		int ws = 0;
		for(int n=0; n < weights.size(); n++) {
			ws += weights.get(n);
			d[n][0] = Integer.MAX_VALUE;
			lastPartSize[n][0] = null;
			d[n][1] = ws;
			lastPartSize[n][1] = n;
		}
		
		for(int k=1; k <= nBins; k++) {
			d[0][k] = 0;
			lastPartSize[0][k] = 0;
		}
		
		//- Eager Berechnung der Tabelle
		for(int k=2; k <= nBins; k++) {
			for(int n=1; n < weights.size(); n++) {
				Integer best = Integer.MAX_VALUE;
				Integer bestT = null;
			
				try {
					int lastClusterWeight = 0;
					int t = 0;
					while(lastClusterWeight < binLo) {
						t++;
						lastClusterWeight += weights.get(n - t + 1);
					}
					
					do {
						int curD = Math.max( d[n-t][k-1], lastClusterWeight );
						if(curD < best) {
							best = curD;
							bestT = t;
						}
						// grow last cluster by 1 element
						t++;
						lastClusterWeight += weights.get(n - t + 1);
					} while(t <= n && lastClusterWeight <= binHi);
				} catch (IndexOutOfBoundsException e) {
					// TODO: handle exception
				}
			
				d[n][k] = best;
				lastPartSize[n][k] = bestT;
			}
		}
		
		
		//- Rekonstruktion des Ergebnisses
		LinkedList<Integer> result = new LinkedList<Integer>();
		
		int n = weights.size() - 1;
		int k = nBins;
		while(n > 0) {
			result.addFirst(n);
			int t = lastPartSize[n][k];
			n = n - t;
			k = k - 1;
		}
		
//		return result;
		return new Pair(result, d[weights.size()-1][nBins]);
	}

	public static List<Integer> toSizes(LinkedList<Integer> idxsHi) {
		int idxLo = 0;
		LinkedList<Integer> sizes = new LinkedList<Integer>();
		for(int idxHi : idxsHi) {
			sizes.add(idxHi - idxLo + 1);
			idxLo = idxHi + 1;
		}
		return sizes;
	}
	
	public static void packBinsPrettyPrintResult(LinkedList<Integer> result, List<Integer> weights) {
		int maxBinWeight = 0;
		LinkedList<Integer> maxBins = new LinkedList<Integer>();
		int idxLo = 0;
		int binNr = 1;
		for(int idxHi : result) {
			List<Integer> curBin = weights.subList(idxLo, idxHi+1);
			idxLo = idxHi+1;
			int curBinWeight = curBin.stream().reduce(0, ((x,y) -> x+y));
			
			System.out.println("\t bin #"+ binNr +": weight: "+ curBinWeight +"; contents: "+ curBin );
			
			if(curBinWeight > maxBinWeight) {
				maxBinWeight = curBinWeight;
				maxBins.clear();
				maxBins.add(binNr);
			} else if(curBinWeight == maxBinWeight) {
				maxBins.add(binNr);
			}
			binNr++;
		}
		
		System.out.println("Maximal bin weight: "+ maxBinWeight +" on bins "+ maxBins);
		
	}
	
	
	/** Quick tests. */
		public static void main(String[] args) {
	//		System.out.println(partitionInNParts(10, 3));
	//		System.out.println(partitionInNParts(20, 5));
	//		System.out.println(partitionInNParts(100, 18));
			
			List<Integer> weights = Arrays.asList(12, 17, 9, 5, 7, 8, 19, 14, 6);
			int nBins = 4, binLo = 10, binHi = 40;
			Pair<LinkedList<Integer>, Integer> maxResTuple = packBinsDP(nBins, binLo, binHi, weights);
			LinkedList<Integer> maxRes = maxResTuple.getElement1(); int maxResMaxSize = maxResTuple.getElement2();
			System.out.println("-- Maximum-computation: "+ maxRes +" - "+ toSizes(maxRes));
			packBinsPrettyPrintResult(maxRes, weights);
			System.out.println("maxResMaxSize: "+ maxResMaxSize);
			
			System.out.println("-- All Results: ");
			List<LinkedList<Integer>> allRes = packBinsRecursive(nBins, binLo, binHi, weights, 0);
			int i=1;
			for(LinkedList<Integer> res : allRes) {
				System.out.println("---> Result "+ i +": "+ res +" - " + toSizes(res));
				packBinsPrettyPrintResult(res, weights);
				i++;
			}
			
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
