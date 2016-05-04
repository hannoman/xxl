package xxl.core.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LangTests {

	public static void main(String[] args) {
		
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
		int mypos = HUtil.binFindL(ll, x);
		
		System.out.println("Position (old) for x = "+ x +" in "+ ll.toString() + ": "+ Collections.binarySearch(ll, x));
		System.out.println("Position (new) for x = "+ x +" in "+ ll.toString() + ": "+ HUtil.binFindL(ll, x));
		
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
}
