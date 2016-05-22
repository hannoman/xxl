package xxl.core.cursors.samplingcursor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.function.Predicate;

import xxl.core.collections.containers.TypeSafeContainer;
import xxl.core.cursors.samplingcursor.SamplingCurse.SamplingResult;
import xxl.core.indexStructures.RSTree_v3;
import xxl.core.indexStructures.RSTree_v3.InnerNode;
import xxl.core.indexStructures.RSTree_v3.LeafNode;
import xxl.core.indexStructures.RSTree_v3.Node;
import xxl.core.util.Pair;

public class SamplingCurse<K extends Comparable<K>,V,P> {
	// the query // TODO: generalize from 1-dimensional case
	final K lo;
	final K hi;			
	
	/** Temporary variable to store nodes to inspect between construction and call to open() 
	 * as we only want to reserve ressources after open().
	 */
	List<P> initialCIDs; 
	
	List<P> frontierCIDs;
	List<Integer> weights;
	List<K> separators;
	
	List<Sampler> samplers; 

	static TypeSafeContainer<P, RSTree_v3<K, V, P>.Node> container;
	Queue<V> precomputed;

	// random number generator
	Random rng = new Random();
	private Predicate<V> pred;
	
	
	
	
	public SamplingCurse(Predicate<V> pred, List<P> initialCIDs, List<K> separators, List<Integer> weights, Random rng) {
		super();
		// rng
		this.rng = rng;
		// query
		this.pred = pred;
		// frontier
		this.initialCIDs = initialCIDs;
		this.separators = separators;		
		this.weights = weights;
		this.samplers = new LinkedList<Sampler>();
		for(P nodeCID : initialCIDs) {
			Node node = container.get(nodeCID);
			samplers.add(Sampler.c)
		}
		
	}



	public SamplingCurse(K lo2, K hi2, List pagePointers, , List childWeights, Random rng2) {
		// TODO Auto-generated constructor stub
	}



	public List<V> tryToSample(int n) {
		
		
		return null;
	}



	public abstract class Sampler {
		public abstract SamplingResult tryToSample(int n);
		
		public static Sampler create(P nodeCID, Predicate<V> pred) throws Exception {
			Node node = container.get(nodeCID);
			if(node.isInner()) {
				InnerNode inode = (InnerNode) node;
				if(inode.hasSampleBuffer()) {
					return new InnerSampler(nodeCID, pred);
				} else {
					return new UnbufferedSampler(inode.allValues(), pred);
				}
			} else {
				throw new Exception("LeafNode unexpected.");					
			}					
		}		
		
	}
	
	/** We need to keep track of the state of this sampler so meticulously to adhere to the paper, because it is 
	 * (probably meant to be) evaluated lazily like this, and otherwise the batched probailities would get skewed.
	 * 
	 * Imagine a frontier with nodes a_1, a_2, ... . T(a_1) is a big tree which only contains a really small 
	 * single relevant part T(b). (And the samples buffer of a_1 is as good as exhausted.)  
	 * On a batched sample of Frontier = (a_1, ..., a_n) 
	 * T(b) would get sampled with probability:
	 * p_1 = |P(a_1)| / sum(|P(a_1)|, ..., |P(a_n)|)
	 * This is incorrect as T(b) should only get sampled with probability:
	 * p_2 = |P(b)| / sum(|P(a_1)|, ..., |P(a_n)|)
	 * Which might make a major difference for |P(b)| << |P(a_1)|
	 * 
	 * Also this incorrectness would cascade.
	 */
	public class UnbufferedSampler extends Sampler {

		final Predicate<V> pred; // predicate representing the query
		List<V> uncategorized;
		ArrayList<V> keepers;
		
		public UnbufferedSampler(List<V> baselist, Predicate<V> pred) {
			this.uncategorized = baselist; // no defensive copy
			this.pred = pred;
			this.keepers = new ArrayList<V>(uncategorized.size());
		}

		/** This only occassionally produces a sample. Might fail as long as we have uncategorized elements left. */
		protected V trySample1() {		
			int x = rng.nextInt(uncategorized.size() + keepers.size());
			return trySample1(x);
		}
		
		/** This only occassionally produces a sample. Might fail as long as we have uncategorized elements left. */
		protected V trySample1(int x) {		
			if(x < uncategorized.size()) { // categorize new one
				V sample = uncategorized.remove(x);
				if(pred.test(sample)) {
					keepers.add(sample);
					return sample;
				} else
					return null;
			} else {
				return keepers.get(x - uncategorized.size());
			}	
		}
		
		/** Batched sampling of n elements where the the effective weight of the node is only updated afterwards. */ 
		public SamplingResult tryToSample(int n) {
			LinkedList<V> samplesObtained = new LinkedList<V>();
			
			int oldAvailable = uncategorized.size() + keepers.size();

			try {
					
				for(int i=0; i < n; i++) {
					int x = rng.nextInt(oldAvailable);
					if(x < uncategorized.size() + keepers.size()) { // otherwise we hit a element which got disabled during this run
						V sample = trySample1(x);
						if(sample != null)
							samplesObtained.add(sample);
					}				
				}
				
			} catch (IllegalArgumentException e) { // this means that this sampler is empty						
				assert e.getMessage() == "bound must be positive";
				assert samplesObtained.isEmpty();
				
				// as we can't descent any further from unbuffered nodes, return an empty sampler list.
				return new SamplingResult();
			}

			return new SamplingResult(samplesObtained);
		}
		
		/** "Iterative" sampling which adjusts the effective weight after each draw.
		 * This does updates after each draw, which is not what we want in a batched draw. */ 
		private SamplingResult tryToSample_iterative(int n) {
			LinkedList<V> samplesObtained = new LinkedList<V>();
						
			try {
				
				for (int i = 0; i < n; i++) {
					V sample = trySample1();
					if (sample != null)
						samplesObtained.add(sample);
				} 
				
			} catch (IllegalArgumentException e) { // this means that this sampler is empty						
				assert e.getMessage() == "bound must be positive";
				assert samplesObtained.isEmpty();
				
				// as we can't descent any further from unbuffered nodes, return an empty sampler list.
				return new SamplingResult();
			}
			
			return new SamplingResult(samplesObtained);
		}
		
	}
	
	
	public class InnerSampler extends Sampler {
		P nodeCID;
		final Predicate<V> pred;
		Iterator<V> sampleIter;
		
		public InnerSampler(P nodeCID, Predicate<V> pred) {
			this.nodeCID = nodeCID;
			this.pred = pred;
			InnerNode inode = (InnerNode) container.get(nodeCID);
			assert inode.hasSampleBuffer();
			
			sampleIter = inode.samples.iterator(); 
		}

		@Override
		public SamplingResult tryToSample(int n) {
			List<V> samplesObtained = new LinkedList<V>();
			
			int i = 0;
			for(; i < n && sampleIter.hasNext(); i++) {
				V sample = sampleIter.next();
				if(pred.test(sample)) {
					samplesObtained.add(sample);
				}
			}
			
			if(i < n) { // we didn't find enough samples in the sample buffer
				// recursively create a new SamplingCurse
				int remaining = n - i;
				InnerNode inode = (InnerNode) container.get(nodeCID);
				
				SamplingCurse subCursor = new SamplingCurse(lo, hi, inode.pagePointers, inode.separators, inode.childWeights, rng);
				samplesObtained.addAll(subCursor.tryToSample(remaining));
				return new SamplingResult(samplesObtained, subCursor);
			} else {
				return new SamplingResult(samplesObtained);
			}
		}
		
	}
	
	/** Field class to for results of sampling from a node. */
	public class SamplingResult {
		boolean replacementNeeded;
		Sampler replacee;
		List<V> samplesObtained;
		
		/** innernodes which don't need expansion or eternal unbuffered nodes */
		public SamplingResult(List<V> samplesObtained) { 
			this.replacementNeeded = false;
			this.replacee = null;
			this.samplesObtained = samplesObtained;
		}
		
		/** inner node which produces something and need expansion */
		public SamplingResult(List<V> samplesObtained, Sampler replacee) { 			
			this.replacementNeeded = true;
			this.replacee = replacee;
			this.samplesObtained = samplesObtained;
		}
		
		/** unbuffered nodes which got terminated */
		public SamplingResult() { 
			this.replacementNeeded = true;
			this.replacee = null;
			this.samplesObtained = null;
		}
	}
	
	
	
	public static void main(String[] args) {
		Random rng = new Random(); 
		for (int i = 0; i < 10; i++) {
			int x = rng.nextInt(0);
			System.out.println(x);
		}		
	}
	
}


