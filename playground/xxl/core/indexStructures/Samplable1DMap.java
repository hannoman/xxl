package xxl.core.indexStructures;

import xxl.core.profiling.ProfilingCursor;
import xxl.core.util.Interval;

public interface Samplable1DMap<K extends Comparable<K>, V> extends Testable1DMap<K, V>{
	
	ProfilingCursor<V> samplingRangeQuery(Interval<K> query, int samplingBatchSize);

}