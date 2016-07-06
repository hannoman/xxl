package xxl.core.indexStructures;

import java.util.List;
import java.util.function.Function;

import xxl.core.profiling.ProfilingCursor;
import xxl.core.util.Interval;

/** Interface for doing sanity checks on self implemented maps (= trees). */
public interface Testable1DMap<K extends Comparable<K>, V> {

	Function<V, K> getGetKey();
	
	/** Insertion. */
	public boolean insert(V value);

	int height();

	/** Lookup. */
	public List<V> get(K key);

	ProfilingCursor<V> rangeQuery(Interval<K> query); 
	
	
}