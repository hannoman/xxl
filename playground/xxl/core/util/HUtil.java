
package xxl.core.util;

import java.util.Collections;
import java.util.List;

public class HUtil {
	/** Shortcut for int-based exponentiation. 
	 * No checking of overflows is performed. */
	public static int intPow(int a, int b) {
		return (int)Math.pow(a, b);		
	}
	
	public static <E, O extends List<E>> O splitOffRight(List<E> inList, int remLeft, O targetList) {
		List<E> transferPart = inList.subList(remLeft, inList.size());
		targetList.addAll(transferPart);
		transferPart.clear();
		return targetList;
	}
	
	/** Shortcut to convert the result of Collections.binarySearch to a more usable format.
	 *  
	 * @return the position where key should be inserted to keep list sorted. 
	 * Lowest <tt>i</tt> with key <= list[i].   
	 */
	public static <T> int findPos(List<? extends Comparable<? super T>> list, T key) {
		int rawpos = Collections.binarySearch(list, key);
		
		if(rawpos < 0) {
			return -(rawpos + 1);
		} else {
			return rawpos;
		}		
	}
}
