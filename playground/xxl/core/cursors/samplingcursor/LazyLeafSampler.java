package xxl.core.cursors.samplingcursor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.function.Predicate;

import javafx.scene.effect.Lighting;

/** Helps with lazy filtering according to a query of a list of samples usually provided as the values of a subtree.
 * (See algorithm 1, lines 8-11) 
 */
public class LazyLeafSampler<V,P> implements StatefulSampler<V,P> {

	List<V> uncategorized;
	final Predicate<V> pred;
	ArrayList<V> keepers;
	
	P associatedNode;
	
	@Override
	public P getNode() {
		return associatedNode;
	}

	Random rng;
	int nNotDisabled;
	
	public LazyLeafSampler(List<V> baselist, Predicate<V> pred, P nodeCID, Random rng) {
		super();
		this.uncategorized = baselist;
		this.pred = pred;
		this.associatedNode = nodeCID;
		
		this.keepers = new ArrayList<V>(uncategorized.size());
		this.rng = rng;
		nNotDisabled = uncategorized.size();
	}

	/** This only occassionally produces a sample. Might fail as long as we have uncategorized elements left. */
	protected V trySample() {		
		int x = rng.nextInt(nNotDisabled); // determine whether we use an already categorized sample or an uncategorized one
		if(x < uncategorized.size()) { // categorize new one
			V sample = uncategorized.remove(x);
			if(pred.test(sample)) {
				keepers.add(sample);
				return sample;
			} else {
				nNotDisabled -= 1;
				return null;
			}
		} else {
			return keepers.get(x - uncategorized.size());
		}	
	}
	
	@Override
	public List<V> tryToSample(int n) {
		LinkedList<V> samplesObtained = new LinkedList<V>();
		for(int i=0; i < n; i++) {
			V sample = trySample();
			if(sample != null) {
				samplesObtained.add(sample);
			} 
		}
		return samplesObtained;
	}
	
	@Override
	public boolean exhausted() {
		return nNotDisabled == 0;
	}
	
	public boolean isFinished() {
		return nNotDisabled == 0;
	}
	
	@Override
	public int size() {
		return nNotDisabled;		
	}

}
