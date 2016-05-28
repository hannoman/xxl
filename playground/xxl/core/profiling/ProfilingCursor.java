package xxl.core.profiling;

import java.util.Map;

import xxl.core.cursors.Cursor;
import xxl.core.util.Pair;

/** A ProfilingCursor keeps track of the number of nodes touched/pruned on each level.
 */
public interface ProfilingCursor<E> extends Cursor<E> {
	
	public Pair<Map<Integer,Integer>, Map<Integer, Integer>> getProfilingInformation();
	
}
