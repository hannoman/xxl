package xxl.core.indexStructures;

import java.util.List;
import java.util.function.Function;

import xxl.core.collections.containers.Container;
import xxl.core.cursors.Cursor;
import xxl.core.indexStructures.RSTree1D.QueryCursor;
import xxl.core.io.converters.Converter;
import xxl.core.profiling.ProfilingCursor;
import xxl.core.util.Interval;

/** Interface for doing sanity checks on self implemented maps (= trees). */
public interface TestableMap<K extends Comparable<K>, V> {

	/**
	 * Insertion. 
	 */
	public void insert(V value);

	/**
	 * Lookup.
	 */
	public List<V> get(K key);

	int height();

	Function<V, K> getGetKey();

	Cursor<V> rangeQuery(K lo, K hi);

	ProfilingCursor<V> rangeQuery(Interval<K> query);
	
	
}