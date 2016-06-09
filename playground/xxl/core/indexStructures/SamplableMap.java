package xxl.core.indexStructures;

import xxl.core.profiling.ProfilingCursor;

public interface SamplableMap<K extends Comparable<K>, V> extends TestableMap<K, V>{
	
	ProfilingCursor<V> samplingRangeQuery(K lo, K hi, int samplingBatchSize);

}
