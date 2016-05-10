package xxl.core.util;

public class QuickTime {
	static long start;
	static String label;
	
	public static void start(String name) {
		start = System.nanoTime();
		label = name;
	}
	
	public static void stop() {
		long elapsed = System.nanoTime() - start;
		System.out.println(label +" time: "+ String.format("%8.2fms", ((double) elapsed / 1000000)));
	}
	
}
