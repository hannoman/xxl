package xxl.core.indexStructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

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
	 * An alternative <tt>NodeConverter</tt> for {@link BPlusTree.Node}s.
	 */
	public class NodeConverter extends Converter<Node> {
		// code copied from BPlusTree.NodeConverter
	
		// additionally the weight - that is the number of entries in a subtree rooted at the node - has to be written and read. 
		// .. or does this get obsolete if the weight is accounted for in a smaller container of meta-information, IndexEntry perhaps?
        @Override
        public Node read(DataInput dataInput, Node unused) throws IOException {
        	int level = dataInput.readInt();
        	if(level == 0) {
        		return readRemainingLeafNode(dataInput);
        	} else {
        		return readRemainingInnerNode(dataInput, level);
        	}
        	
        	// Saveformat for Leafs:
            // (0 = level :: int)(number of entries :: int)(hasNextNeighbor :: bool)(nextNeighborId :: ContainerID)?(leafEntry :: DataEntry){number}
            // Saveformat for InnerNodes:
            // (0 != level :: int)(number of entries :: int)(hasNextNeighbor :: bool)(nextNeighborId :: ContainerID)?(a :: ContainerID){number}(b :: Separator){number}
        }
        
        Node readRemainingLeafNode(DataInput dataInput) throws IOException {
            // create Node shell            
            Node node = new Node(0);

        	//- read number of entries 
            int number = dataInput.readInt();            
            
            //-- read info about next neighbor if available
            boolean readNext = dataInput.readBoolean();
            if (readNext) {
                node.nextNeighbor = new IndexEntry(0 + 1); // indirect call of constructor: IndexEntry(level + 1)
                // QUE: why is "level+1" used?
                
                Object nextNeighborId = container().objectIdConverter().read(dataInput);
                node.nextNeighbor.initialize(nextNeighborId);
            } else {
                node.nextNeighbor = null;
            }
            
            //-- read the content of the node
            for(int i=0; i < number; i++) {
    			Object entry = dataConverter.read(dataInput);
    			node.entries.add(entry);
    		}
            
            return node;             
        }
        
        Node readRemainingInnerNode(DataInput dataInput, int level) throws IOException {
        	// create Node shell
            Node node = new Node(level);
                                                
            //- read number of entries 
            int number = dataInput.readInt();
            
            //-- read info about next neighbor if available
            boolean readNext = dataInput.readBoolean();
            if (readNext) {
                node.nextNeighbor = new IndexEntry(level + 1); // indirect call of constructor: IndexEntry(level + 1)
                // QUE: why is "level+1" used?
                
                Object nextNeighborId = container().objectIdConverter().read(dataInput);
                node.nextNeighbor.initialize(nextNeighborId);
            } else {
                node.nextNeighbor = null;
            }
            
            //-- read the ContainerIDs of the IndexEntries
            Object[] containerIDs = new Object[number];            
            for(int i=0; i < number; i++) {            	
                containerIDs[i] = container().objectIdConverter().read(dataInput);
    		}
            
            //-- reads the Separators for the IndexEntries 
            Separator[] separators = new Separator[number];
        	for (int i = 0; i < node.number(); i++) {
        		separators[i] = (Separator) createSeparator.invoke(keyConverter.read(dataInput));
        	}
        	
        	//-- constructs the IndexEntries and adds them to the Node
        	for(int i=0; i < number; i++) {
        		IndexEntry ie = (new IndexEntry(node.level)).initialize(containerIDs[i], separators[i]);
        		node.entries.add(ie);
        	}

            return node;
        }
        
        public void write(DataOutput dataOutput, Node node) throws IOException {
        	dataOutput.writeInt(node.level);
        	if(node.level == 0) {
        		writeRemainingLeafNode(dataOutput, node);
        	} else {
        		writeRemainingInnerNode(dataOutput, node);
        	}
        }
        
        void writeRemainingLeafNode(DataOutput dataOutput, Node node) throws IOException {
        	
            dataOutput.writeInt(node.number());
            
            if(node.nextNeighbor != null) {
            	dataOutput.writeBoolean(true);
            	container().objectIdConverter().write(dataOutput, node.nextNeighbor.id());
            } else {
            	dataOutput.writeBoolean(false);
            }
            
            //-- write content
            for(Object entry : node.entries) {
            	dataConverter.write(dataOutput, entry);
            }
            
        }
        
        void writeRemainingInnerNode(DataOutput dataOutput, Node node) throws IOException {
        	
            dataOutput.writeInt(node.number());
            
            if(node.nextNeighbor != null) {
            	dataOutput.writeBoolean(true);
            	container().objectIdConverter().write(dataOutput, node.nextNeighbor.id());
            } else {
            	dataOutput.writeBoolean(false);
            }
            
            //-- write ContainerIDs
            for(IndexEntry ie : (List<IndexEntry>) node.entries) {
            	container().objectIdConverter().write(dataOutput, ie.id());
            }
            
            //-- write Separators
            for(IndexEntry ie : (List<IndexEntry>) node.entries) {
            	keyConverter.write(dataOutput, ie.separator.sepValue());
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
            return 2 * IntegerConverter.SIZE + BooleanConverter.SIZE + container().getIdSize();
        }
	}

}
