package xxl.core.indexStructures;


/**
 * Trying to implement a Weight-balanced B+-Tree with the ideas from 
 * "Optimal Dynamic Interval Management in External Memory" by L. Arge, J.S. Vitter
 * 
 * For implementing deletions see also "global rebuilding technique" in:
 * "The Design of Dynamic Data Structures" by M.H. Overmars
 * 
 */
public class WBPlusTree extends BPlusTree {

	/**
     * Creates a new <tt>WBPlusTree</tt>. 
     * The minimal capacity ratio is set to the default value 0.5 (i.e. 50%) (although this doesn't make sense). 
     * Duplicates are disallowed for now. 
     * 
     * @param blockSize
     *            the block size of the underlying storage.
     */
	public WBPlusTree(int blockSize) {		
		super(blockSize, 0.5f, false); // set minCapacityRatio to 0.5 although this doesn't make much sense
	}

	
	
	public class Node extends BPlusTree.Node {
		// Nodes have additional field "weight" which needs to be adjusted on every insert/delete-operation passing through.
		
		int weight;
		
		public Node(int level) {
			super(level);
			this.weight = 0;
		}

		// nah.. use this constructor as the corresponding super-constructor already calls the private {@link BPlusTree.Node.initialize(int, Function)}
//		public Node(int level, Function createEntryList) {
//			super(level, createEntryList);
//			this.weight = 1;
//		}				
		
	}
	
	// for the WPTree.Node Converter:
	// additionally the weight - that is the number of entries in a subtree rooted at the node - has to be written and read. 
	// .. or does this get obsolete if the weight is accounted for in a smaller container of meta-information, IndexEntry perhaps?


}
