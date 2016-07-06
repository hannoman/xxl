package xxl.core.indexStructures;

import java.util.function.Function;

import xxl.core.profiling.ProfilingCursor;

public interface SamplableMapV2<Q, V> {
	
	Function<V, InclusionTestable<Q>> getGetQueryKey();
	
	ProfilingCursor<V> samplingRangeQuery(Q query, int samplingBatchSize);

}
