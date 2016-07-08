package xxl.core.profiling.interfaces;

import xxl.core.profiling.ProfilingCursor;
import xxl.core.util.Interval;

/** Interface for doing sanity checks on self implemented maps (= trees). */
public interface TestableMapV2<Q, V> {

//	Function<V, K> getGetKey();
	
	/** Insertion. */
	public boolean insert(V value);

	int height();

	/** Lookup. */
	ProfilingCursor<V> rangeQuery(Q query); 
	
	
}