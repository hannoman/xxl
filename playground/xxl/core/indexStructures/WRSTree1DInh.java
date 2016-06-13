package xxl.core.indexStructures;

import java.util.function.Function;

import xxl.core.util.Interval;

public class WRSTree1DInh<K extends Comparable<K>, V, P> extends RSTree1D<K, V, P> {

	public WRSTree1DInh(int branchingLo, int branchingHi, int leafLo, int leafHi, int samplesPerNodeLo, int samplesPerNodeHi, Interval<K> universe, Function<V, K> getKey) {
		super(branchingLo, branchingHi, leafLo, leafHi, samplesPerNodeLo, samplesPerNodeHi, universe, getKey);
		// TODO Auto-generated constructor stub
	}

}
