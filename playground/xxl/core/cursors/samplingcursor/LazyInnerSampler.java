package xxl.core.cursors.samplingcursor;

import java.util.List;
import java.util.Random;
import java.util.function.Predicate;

public class LazyInnerSampler<V,P> implements StatefulSampler<V, P> {

	final Predicate<V> pred;
	P associatedNode;
	Random rng;
	
	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List<V> tryToSample(int n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public P getNode() {		
		return associatedNode;
	}

	boolean exhausted() {
		return false;
	}

	
}
