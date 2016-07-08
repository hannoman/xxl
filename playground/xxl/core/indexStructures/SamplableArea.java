package xxl.core.indexStructures;

import java.util.function.Predicate;

import xxl.core.profiling.ProfilingCursor;

public interface SamplableArea<Q, V> {
	
	Predicate<V> getValueInclusionTest(Q query);
	
	ProfilingCursor<V> samplingRangeQuery(Q query, int samplingBatchSize);

}
