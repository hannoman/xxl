package xxl.core.cursors.sources;

import java.util.List;
import java.util.Random;

import xxl.core.cursors.AbstractCursor;

/** Draws values with replacement continously from the given list. */
public class InfiniteSampler<V> extends AbstractCursor<V> {

	List<V> baseList;
	Random rng;
	
	public InfiniteSampler(List<V> baseList, Random rng) {
		super();
		this.baseList = baseList;
		this.rng = rng;
	}
		
	public InfiniteSampler(List<V> baseList) {
		this(baseList, new Random());
	}

	@Override
	protected boolean hasNextObject() {
		return true;
	}

	@Override
	protected V nextObject() {
		return baseList.get(rng.nextInt(baseList.size()));
	}

}
