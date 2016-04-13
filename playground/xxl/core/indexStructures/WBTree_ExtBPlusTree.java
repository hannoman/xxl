package xxl.core.indexStructures;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.BPlusTree.IndexEntry;
import xxl.core.indexStructures.BPlusTree.Node;
import xxl.core.indexStructures.BPlusTree.Node.SplitInfo;
import xxl.core.indexStructures.WBTreeSA_v3.InnerNode;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;
import xxl.core.util.HUtil;
import xxl.core.util.Triple;

public class WBTree_ExtBPlusTree extends BPlusTree {
	
	
	/**
     * Initializes the <tt>BPlusTree</tt>.
     * 
     * @param getKey
     *            the <tt>Function</tt> to get the key of a data object
     * @param keyConverter
     *            the <tt>Converter</tt> for the keys used by the tree
     * @param dataConverter
     *            the <tt>Converter</tt> for data objects stored in the tree
     * @param createSeparator
     *            a factory <tt>Function</tt> to create <tt>Separators</tt>
     * @param createKeyRange
     *            a factory <tt>Function</tt> to create <tt>KeyRanges</tt>
     * @param getSplitMinRatio
     *            a <tt>Function</tt> to determine the minimal relative number
     *            of entries which the node may contain after a split
     * @param getSplitMaxRatio
     *            a <tt>Function</tt> to determine the maximal relative number
     *            of entries which the node may contain after a split
     * @return the initialized <tt>BPlusTree</tt> itself
     */
    protected BPlusTree initialize(Function getKey,
            MeasuredConverter keyConverter, MeasuredConverter dataConverter,
            Function createSeparator, Function createKeyRange,
            Function getSplitMinRatio, Function getSplitMaxRatio) {
        this.getKey = getKey;
        this.keyConverter = keyConverter;
        this.dataConverter = dataConverter;
        this.nodeConverter = createNodeConverter();
        this.createSeparator = createSeparator;
        this.createKeyRange = createKeyRange;
        int space = this.BLOCK_SIZE - nodeConverter.headerSize();
        // unused because we only split because of weight violations
//        this.B_IndexNode = space / nodeConverter.indexEntrySize();
//        this.B_LeafNode = space / nodeConverter.leafEntrySize();
//        this.D_IndexNode = (int) (minCapacityRatio * this.B_IndexNode);
//        this.D_LeafNode = (int) (minCapacityRatio * this.B_LeafNode);
        Function getDescriptor = new AbstractFunction() {
            public Object invoke(Object o) {
                if (o instanceof Separator) return o;
                if (o instanceof IndexEntry) return ((IndexEntry) o).separator;
                return createSeparator(key(o));
            }
        };        
        
        Predicate overflows = new AbstractPredicate() {
            public boolean invoke(Node node) {
                if(node.level() == 0) {
                	// tK <= e(L) < 2*tK
                	return node.totalWeight() >= 2*tK;  
                } else {
                	// 1/2 * tK * (tA ** l) < w(N) < 2 * tK * (tA ** l)
                	return node.totalWeight() >= 2 * HUtil.intPow(tA, level) * tK;
                }                
            }
        };
        
        Predicate underflows = new AbstractPredicate() {
            public boolean invoke(Node node) {
                if(node.level() == 0) {
                	// tK <= e(L) < 2*tK
                	return node.totalWeight() < tK;  
                } else {
                	// 1/2 * tK * (tA ** l) < w(N) < 2 * tK * (tA ** l)
                	return node.totalWeight() <= HUtil.intPow(tA, level) * tK / 2;
                }                
            }
        };
        
        return initialize(getDescriptor, underflows, overflows,
                getSplitMinRatio, getSplitMaxRatio);
    }
	
	
    public class IndexEntry extends BPlusTree.IndexEntry {
    	public int weight;
    	
    	public IndexEntry(int parentLevel) {
			super(parentLevel);
			// TODO Auto-generated constructor stub
		}	
    	
    	
    }
    
	public class Node extends BPlusTree.Node {
		
		public Node(int level, Function createEntryList) {
			super(level, createEntryList);
			// TODO Auto-generated constructor stub
			// needs the other constructor to be implemented too?
		}

		public int totalWeight() {
			int summed = 0;
			for(Object eo : entries) {
				IndexEntry e = (IndexEntry) eo;
				summed += e.weight;
			}
			return summed;
		}
		
		/**
	     * Splits the overflowed node. In non duplicate mode both the leaf and index nodes 
	     * are split in the middle. In duplicate mode the leaf nodes are split 
	     * according following strategy:  
	     * with leaf nodes the last element is selected and checked against 
	     * the the element at 75% position, when both are equal search for further duplicates until 25% reached
	     * or no more duplicates exists, then split at found index position, otherwise split in the middle.
	     * 
	     * @param path
	     * @return a <tt>SplitInfo</tt> containing all needed information about
	     *         the split
	     */
	    protected Tree.Node.SplitInfo split(Stack path) {
	    	reorg = true;
			Node node = (Node) node(path);
			List newEntries = null;
	        int number = node.number();
	        int index = (number+1) / 2;
	        
	        if (this.level() == 0 && duplicate){
	        	int dupIndex = node.number()-1;
	        	Comparable pivotEntry = separator(node.entries.get(dupIndex)).sepValue(); 
	        	if ( pivotEntry.compareTo(separator(node.entries.get((number / 4)*3)).sepValue()) == 0 ){ //  75 %
	        		dupIndex = (number / 4)*3;
	        		while( dupIndex > (number/4) 
	            			&&  pivotEntry.compareTo(separator(node.entries.get(dupIndex-1)).sepValue()) == 0  ){
	            		dupIndex--;
	            	}	
	        		index = dupIndex;
	            }
	        }
	        newEntries = node.entries.subList(index, node.number());
	        Separator sepNewNode = (Separator) separator(newEntries.get(newEntries.size()-1)).clone();
	        entries.addAll(newEntries);
	        newEntries.clear();
	        nextNeighbor = node.nextNeighbor;
	        return (new SplitInfo(path)).initialize(sepNewNode);
	        //TODO SplitStrategie
	    }
		
		// from my stand-alone version
	    private SplitInfo split(int targetWeight) {
			Triple<Integer, Integer, Integer> splitPosInfo = determineSplitposition(targetWeight);
			int splitPos = splitPosInfo.getElement1();
			int remLeft = splitPosInfo.getElement2();
			int remRight = splitPosInfo.getElement3();
			
			InnerNode newode = new InnerNode();
			
			//- split separators
			// separators[splitPos] becomes the separator between the offspring 
			newode.separators = HUtil.splitOffRight(separators, splitPos+1, new ArrayList<K>());
			K offspringSeparator = separators.remove(splitPos);
			
			//- split other lists
			newode.pagePointers = HUtil.splitOffRight(pagePointers, splitPos+1, new ArrayList<P>());			
			newode.childWeights = HUtil.splitOffRight(childWeights, splitPos+1, new ArrayList<Integer>());
			
			//- put new node into Container
			P newodeCID = container.insert(newode);
			
			return new SplitInfo(newodeCID, offspringSeparator, remLeft, remRight);			
		}
	    
	    /** Determines a feasible split position through linear search.
		 * 
		 * TODO: as of now only considers the quality of the left offspring node. Is this a problem?
		 * 
		 * @param targetWeight the weight per node which should be approached
		 * @return the position of the node after which the child-list should be split
		 */
		private Triple<Integer, Integer, Integer> determineSplitposition(int targetWeight) {
			int curSum = 0;
			int curSplitWeightMiss = 0;
			
			int bestSplitAfterPos = -1;
			int bestRemLeftWeight = curSum;
			int bestSplitWeightMiss = Math.abs(targetWeight - curSum);			
			
			for(int curSplitAfterPos = 0; curSplitAfterPos < pagePointers.size(); curSplitAfterPos++) {
				curSum += childWeights.get(curSplitAfterPos);
				curSplitWeightMiss = Math.abs(targetWeight - curSum);
				
				if(curSplitWeightMiss < bestSplitWeightMiss) {
					bestSplitWeightMiss = curSplitWeightMiss;
					bestSplitAfterPos = curSplitAfterPos;
					bestRemLeftWeight = curSum;
				}				
			}
			
			int totalWeight = curSum;
			
			return new Triple<Integer, Integer, Integer>(bestSplitAfterPos, bestRemLeftWeight, totalWeight - bestRemLeftWeight);
		}
	    
	    
	}
}
