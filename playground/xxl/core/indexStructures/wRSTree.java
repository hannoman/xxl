package xxl.core.indexStructures;

import java.util.function.Function;

import xxl.core.util.CopyableRandom;
import xxl.core.util.Interval;

public class wRSTree<K extends Comparable<K>,V,P> extends RSTree1D<K,V,P> {

	public wRSTree(Interval<K> universe, int samplesPerNodeLo, int samplesPerNodeHi, int branchingLo, int branchingHi, int leafLo, int leafHi, Function<V, K> getKey) {
		this.universe = universe;
		this.samplesPerNodeLo = samplesPerNodeLo;
		this.samplesPerNodeHi = samplesPerNodeHi;
		this.branchingLo = branchingLo;
		this.branchingHi = branchingHi;
		this.leafLo = leafLo;
		this.leafHi = leafHi;
		this.getKey = getKey;
		
		// defaults
		this.samplesPerNodeReplenishTarget = this.samplesPerNodeHi;
		this.rng = new CopyableRandom();
	}
}
