package xxl.core.util;

import java.util.List;

public class Hypercube<K extends Comparable<K>> {

	final int dimension;
	final Interval<K>[] intervals;
	
	public Hypercube(List<Interval<K>> intervals) {
		this.dimension = intervals.size();
		this.intervals = (Interval<K>[]) intervals.toArray();
	}
	
	
	
	
}
