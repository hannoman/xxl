package xxl.core.indexStructures;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

import xxl.core.indexStructures.HilbertRTreeSA.InnerNode;
import xxl.core.indexStructures.HilbertRTreeSA.InsertionInfo;
import xxl.core.spatial.rectangles.FixedPointRectangle;
import xxl.core.util.HUtil;
import xxl.core.util.Interval;
import xxl.core.util.Pair;

public class WbRS_HilbertRTreeInh<V,P> extends HilbertRTreeSA<V, P>{

	/** The branching parameter <b>tA</b> == the fanout.			<br> 
	 * Following inequalities hold:									<br>
	 *																<br>
	 * Weight of an inner node N =: w(N) on level l:				<br>
	 * 		1/2 * tK * (tA ** l) < w(N) < 2 * tK * (tA ** l)		<br>
	 *																<br>
	 * Number of entries in an inner node N =: e(N) on level l: 	<br>
	 * 		1/4 * tA < e(N) < 4 * tA 
	 */
	/*final*/ int branchingParam;
	
	/** The leaf parameter <b>tK</b>, determining the amount of entries a leaf can contain.	<br>
	 * Following inequalities hold:									<br>
	 * 																<br>
	 * Number of entries in a leaf L =: e(L): 						<br>
	 * 		tK <= e(L) < 2*tK 										<br>
	 */
	/*final*/ int leafParam;
	
	private WbRS_HilbertRTreeInh(int branchingLo, int branchingHi, int leafLo, int leafHi, int samplesPerNodeLo, int samplesPerNodeHi, 
			int dimension, FixedPointRectangle universe, Function<V, FixedPointRectangle> getBoundingBox, Function<FixedPointRectangle, Long> getSFCKey, 
			int nDuplicatesAllowed, int splitPolicy) {
		super(branchingLo, branchingHi, leafLo, leafHi, samplesPerNodeLo, samplesPerNodeHi, dimension, universe, getBoundingBox, getSFCKey, nDuplicatesAllowed, splitPolicy);
	}
	
	public static <V,P> WbRS_HilbertRTreeInh<V,P> create(
			int branchingParam, int leafParam, int samplesPerNodeLo, int samplesPerNodeHi, int dimension, 
			FixedPointRectangle universe, Function<V, FixedPointRectangle> getBoundingBox, 
			Function<FixedPointRectangle, Long> getSFCKey, int nDuplicatesAllowed, int splitPolicy) {
		//- old fixed values from RSTree: no changes on leaf insertion logic needed with those
		int leafLo = leafParam / 2;
		int leafHi = leafParam * 2 - 1; // RSTree defines upper bound inclusive, WBTree not. "-1" to keep it compatible.
		//- unused
		int branchingLo = branchingParam / 4 + 1;
		int branchingHi = branchingParam * 4 - 1;
		
		WbRS_HilbertRTreeInh<V, P> inst = new WbRS_HilbertRTreeInh<V,P>(branchingLo, branchingHi, leafLo, leafHi, samplesPerNodeLo, samplesPerNodeHi, dimension, universe, getBoundingBox, getSFCKey, nDuplicatesAllowed, splitPolicy);
		inst.leafParam = leafParam;
		inst.branchingParam = branchingParam;
		
		return inst;
	}
	
	
	public boolean weightUnderflow(int weight, int level) {
		return weight < minWeightInnerNode(level);
	}

	public boolean weightOverflow(int weight, int level) {
		return weight > maxWeightInnerNode(level);
	}
	
	public int minWeightInnerNode(int level) {
		return HUtil.intPow(branchingParam, level) / 2 * leafParam + 1;
	}
	
	public int maxWeightInnerNode(int level) {
		 return 2 * HUtil.intPow(branchingParam, level) * leafParam - 1;
	}
	
	public boolean leafUnderflow(int weight) {
		return weight < leafParam;
	}

	public boolean leafOverflow(int weight) {
		return weight >= 2*leafParam;
	}
	
	@Override
	public InnerNode createInnerNode() {
		return new InnerNode();
	}
	
	public class InnerNode extends HilbertRTreeSA<V,P>.InnerNode {
		@Override
		public boolean overflow() {
			return weightOverflow(this.totalWeight(), level);
		}
		
		@Override
		public InsertionInfo split() {
			//- fetch cooperating siblings
			Pair<Interval<Integer>, List<InnerNode>> siblingInfo = getCooperatingSiblingsAndIdxs();
			Interval<Integer> coopSiblingIdxs = siblingInfo.getElement1();
			List<InnerNode> coopSiblings = siblingInfo.getElement2();
			
			//- Collect all child entries (with meta information) and samples
			List<Long> all_lhvRanges = new LinkedList<Long>();
			List<FixedPointRectangle> all_areaRanges = new LinkedList<FixedPointRectangle>();
			List<P> all_pagePointers = new LinkedList<P>();
			List<Integer> all_childWeights = new LinkedList<Integer>();
			
			LinkedList<V> all_samples = new LinkedList<V>(); // need to be redistributed afterwards
			
			for(InnerNode sibling : coopSiblings) {
				all_lhvRanges.addAll(sibling.lhvSeparators);
				all_areaRanges.addAll(sibling.areaRanges);
				all_pagePointers.addAll(sibling.pagePointers);
				all_childWeights.addAll(sibling.childWeights);
				
				all_samples.addAll(sibling.samples);
			}
			
			//- try sharing (that means whether we can redistribute without an additional node)
			InsertionInfo insertionInfo;
			Pair<LinkedList<Integer>, Integer> packBinsSharingTuple = 
					HUtil.packBinsDP(coopSiblings.size(), minWeightInnerNode(level), maxWeightInnerNode(level), all_childWeights);
			if(packBinsSharingTuple.getElement2() > maxWeightInnerNode(level)) { // need to create a new node
				InnerNode newnode = createInnerNode(); // new InnerNode();
				newnode.pagePointer = container.reserve(null); // assign a CID to the newly created node
				coopSiblings.add(newnode); // update all references last
				insertionInfo = SPLIT(coopSiblingIdxs, newnode);
				// calculate new split points
				packBinsSharingTuple = 
						HUtil.packBinsDP(coopSiblings.size(), minWeightInnerNode(level), maxWeightInnerNode(level), all_childWeights);
				assert packBinsSharingTuple.getElement2() <= maxWeightInnerNode(level) : "Could not redistribute child entries, even with split node.";
			} else {
				insertionInfo = SHARING(coopSiblingIdxs);
			}
			
			//- Redistribute child entries again
			List<Integer> sizes = HUtil.toSizes(packBinsSharingTuple.getElement1());
			int i = 0;
			for(InnerNode sibling : coopSiblings) {
				int curSize = sizes.get(i);
				sibling.lhvSeparators = HUtil.splitOffLeft(all_lhvRanges, curSize, new ArrayList<>(branchingHi));
				sibling.areaRanges = HUtil.splitOffLeft(all_areaRanges, curSize, new ArrayList<>(branchingHi));
				sibling.pagePointers = HUtil.splitOffLeft(all_pagePointers, curSize, new ArrayList<>(branchingHi));
				sibling.childWeights = HUtil.splitOffLeft(all_childWeights, curSize, new ArrayList<>(branchingHi));
				sibling.samples = sibling.shouldHaveSampleBuffer() ? new LinkedList<V>() : null;
				i++;
			}
			
			//-- Redistribute samples (slow and kind of hacky (OPT))
			for(V sample : all_samples) {
				for(InnerNode sibling : coopSiblings) {
					if(getSFCKey.apply(getBoundingBox.apply(sample)) <= sibling.getLHV()) { // we can add it here
						if(sibling.shouldHaveSampleBuffer()) {
							if(sibling.hasSampleBuffer() == false)
								sibling.samples = new LinkedList<V>();
							sibling.samples.add(sample);							
						}
						break;
					}
				}
			}
			
			//- check whether distribution stays ok // DEBUG
			for(InnerNode sibling : coopSiblings)
				assert sibling.samples == null || sibling.samples.size() <= samplesPerNodeHi;
			
			//- update container contents of all cooperating siblings
			for(InnerNode sibling : coopSiblings)
				container.update(sibling.pagePointer, sibling);
			
			//- return
			return insertionInfo;
		}
	}

}
