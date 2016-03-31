package xxl_dk;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import xxl.core.functions.Function;
import xxl.core.indexStructures.BPlusTree;
import xxl.core.indexStructures.BPlusTree.IndexEntry;
import xxl.core.indexStructures.BPlusTree.Node;
import xxl.core.indexStructures.Tree.Node.SplitInfo;
import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.IntegerConverter;

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
	
	
	/**
	 * A <tt>NodeConverter</tt> is used by the <tt>WBPlusTree</tt> to convert
	 * the <tt>Nodes</tt> for I/O-purposes.
	 */
	public class NodeConverter {
		// code copied from BPlusTree.NodeConverter
	
		// additionally the weight - that is the number of entries in a subtree rooted at the node - has to be written and read. 
		// .. or does this get obsolete if the weight is accounted for in a smaller container of meta-information, IndexEntry perhaps?

        public Object read(DataInput dataInput, Object object) throws IOException {
        	
        	//- read level
        	int level = dataInput.readInt();
        	                                    
            //- read number of entries 
            int number = dataInput.readInt(); 
            
            // create Node shell
            //Node node = (Node) createNode(level); // indirect call of constructor: Node(int)
            Node node = new Node(level);
            
            //-- read info about next neighbor if available
            boolean readNext = dataInput.readBoolean();
            if (readNext) {
                node.nextNeighbor = (IndexEntry) createIndexEntry(level + 1); // indirect call of constructor: IndexEntry(level + 1)
                // QUE: why is "level+1" used?
                
                // node.nextNeighbor.initialize(readID(dataInput)); // replaced by:
                Object nextNeighborId = container().objectIdConverter().read(dataInput);
                node.nextNeighbor.initialize(nextNeighborId);
            } else {
                node.nextNeighbor = null;
            }
            
            //-- read the content of the node
            readEntries(dataInput, node, number);
            
            //-- init: reads the separators for the IndexEntries if we are in an inner node 
            if (node.level != 0) { // if not leaf
            	for (int i = 0; i < node.number(); i++) {
            		Comparable sepValue = (Comparable) keyConverter.read(dataInput);
            		((IndexEntry) node.getEntry(i)).initialize(createSeparator(sepValue));
            	}
            }
            return node;
            
            // Saveformat for Leafs:
            // (0 = level :: int)(number of entries :: int)(hasNextNeighbor :: bool)(nextNeighborId :: ContainerID)?(leafEntry :: DataEntry){number}
            // Saveformat for InnerNodes:
            // (0 != level :: int)(number of entries :: int)(hasNextNeighbor :: bool)(nextNeighborId :: ContainerID)?(a :: ContainerID){number}(b :: Separator){number}
        }
        
        // @Override // yep, that actually doesn't override anything, so shouldn't node.entries be visible?
        protected void readEntries(DataInput input, Node node, int number) throws IOException 
        {
        	if(node.level == 0) { // if leaf
        		for(int i=0; i < number; i++) {
        			Object entry = dataConverter.read(input);
        			node.entries.add(i, entry); // why is this not visible? check java visibility for inner classes..
        		}
        	} else { // index node
        		for(int i=0; i < number; i++) {
        			IndexEntry entry = new IndexEntry(node.level);
        			Object entryId = container().objectIdConverter().read(input);
                    entry.initialize(entryId);
                    node.entries.add(i, entry); // why is this not visible? check java visibility for inner classes..
        		}
        	}
        }

		public void write(DataOutput dataOutput, Object object)
                throws IOException {
            Node node = (Node) object;
            //2x Integer
            dataOutput.writeInt(node.level);
            dataOutput.writeInt(node.number());
            //Boolean
            dataOutput.writeBoolean(node.nextNeighbor != null);
            //ID
            if (node.nextNeighbor != null)
                    writeID(dataOutput, node.nextNeighbor.id());
            //Entries
            writeEntries(dataOutput, node);
            //Separators
           // edit
            if (node.level != 0)
                    for (int i = 0; i < node.number(); i++)
                        keyConverter.write(dataOutput, separator(
                                node.getEntry(i)).sepValue());
        }
        
        protected void writeEntries(DataOutput output, Node node)
                throws IOException {
            Iterator entries = node.entries();
            while (entries.hasNext()) {
                Object entry = entries.next();
                if (node.level == 0)
                    dataConverter.write(output, entry);
                else
                    writeIndexEntry(output, (IndexEntry) entry);
            }
        }
        
        protected int indexEntrySize() {
//            return BPlusTree.this.container().getIdSize()
//                    + keyConverter.getMaxObjectSize();
        	return container().getIdSize() + keyConverter.getMaxObjectSize();
        }

        protected int leafEntrySize() {
            return dataConverter.getMaxObjectSize();
        }
        
        protected int headerSize() {
            return 2 * IntegerConverter.SIZE + BooleanConverter.SIZE
                    + BPlusTree.this.container().getIdSize();
        }
        
        protected IndexEntry readIndexEntry(DataInput input, int parentLevel)
                throws IOException {
            IndexEntry indexEntry = new IndexEntry(parentLevel);
            Object id = container().objectIdConverter().read(input);
            indexEntry.initialize(id);
            return indexEntry;
        }
        
        protected void writeIndexEntry(DataOutput output, IndexEntry entry)
                throws IOException {
            writeID(output, entry.id);
        }
        
        private Object readID(DataInput input) throws IOException {
//            Converter idConverter = BPlusTree.this.container()
//                    .objectIdConverter();
//            return idConverter.read(input, null);
        	return container().objectIdConverter().read(input);
        }
        
        private void writeID(DataOutput output, Object id) throws IOException {
//            Converter idConverter = BPlusTree.this.container()
//                    .objectIdConverter();
//            idConverter.write(output, id);
            container().objectIdConverter().write(output, id);
        }



        // @Override // yep, that actually doesn't override anything, so shouldn't node.entries be visible?
		//        protected void readEntries_old(DataInput input, Node node, int number) throws IOException 
		//        {
		//            for (int i = 0; i < number; i++) {
		//                Object entry;
		//                if (node.level == 0) // if is leaf 
		//                    entry = dataConverter.read(input);
		//                else {
		//                	// entry = readIndexEntry(input, node.level); // function inlined:
		//                   	entry = new IndexEntry(node.level);
		//                    Object id = container().objectIdConverter().read(input);
		//                    ((IndexEntry) entry).initialize(id);
		//                }
		//                    
		//                node.entries.add(i, entry); // why is this not visible?
		//            }
		//        }






}
