package xxl.core.indexStructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;

import xxl.core.indexStructures.WBTreeSA_v3.InnerNode;
import xxl.core.indexStructures.WBTreeSA_v3.LeafNode;
import xxl.core.indexStructures.WBTreeSA_v3.Node;
import xxl.core.io.converters.Converter;

public class WBTree_NodeConverter<K extends Comparable<K>, V, P> extends Converter<WBTreeSA_v3<K, V, P>.Node> {		
		
	/** To have access to the inner class. */
	private final WBTreeSA_v3<K, V, P> treeInstance;
	
	//-- Converters
	public Converter<K> keyConverter;
	public Converter<V> valueConverter;
	public Converter<P> cidConverter;
	
	public WBTree_NodeConverter(
			WBTreeSA_v3<K, V, P> treeInstance, 
			Converter<K> keyConverter, 
			Converter<V> valueConverter,
			Converter<P> cidConverter) {
		super();
		this.treeInstance = treeInstance;
		this.keyConverter = keyConverter;
		this.valueConverter = valueConverter;
		this.cidConverter = cidConverter;
	}

	@Override
	public WBTreeSA_v3<K, V, P>.Node read(DataInput dataInput, WBTreeSA_v3<K, V, P>.Node unused) throws IOException {
		boolean isLeaf = dataInput.readBoolean();
		if (isLeaf) {
			return readLeafNode(dataInput);
		} else {
			return readInnerNode(dataInput);
		}
	}

	WBTreeSA_v3<K, V, P>.LeafNode readLeafNode(DataInput dataInput) throws IOException {
		// create Node shell
		WBTreeSA_v3<K, V, P>.LeafNode node = treeInstance.new LeafNode();

		// - read weight == number of entries
		int nChildren = dataInput.readInt();

		// -- read the content of the node
		node.values = new LinkedList<V>();
		for (int i = 0; i < nChildren; i++) {
			node.values.add(valueConverter.read(dataInput));
		}

		return node;
	}

	WBTreeSA_v3<K, V, P>.InnerNode readInnerNode(DataInput dataInput) throws IOException {
		WBTreeSA_v3<K, V, P>.InnerNode node = treeInstance.new InnerNode();
		
		// - read number of childs
		int nChildren = dataInput.readInt();

		// -- read separators
		node.separators = new LinkedList<K>();
		for(int i=0; i < nChildren-1; i++) { // only #childs-1 separators!
			node.separators.add(keyConverter.read(dataInput));
		}
		
		// -- read the ContainerIDs of the IndexEntries
		node.pagePointers = new LinkedList<P>();			
		for (int i = 0; i < nChildren; i++) {
			node.pagePointers.add(cidConverter.read(dataInput));
		}
		
		// -- read weights
		node.childWeights = new LinkedList<Integer>();
		for (int i = 0; i < nChildren; i++) {
			node.childWeights.add(dataInput.readInt());
		}

		return node;
	}

	@Override
	public void write(DataOutput dataOutput, WBTreeSA_v3<K, V, P>.Node node) throws IOException {			
		if (node.isLeaf()) {
			dataOutput.writeBoolean(true);
			writeRemainingLeafNode(dataOutput, (WBTreeSA_v3<K, V, P>.LeafNode) node);
		} else {
			dataOutput.writeBoolean(false);
			writeRemainingInnerNode(dataOutput, (WBTreeSA_v3<K, V, P>.InnerNode) node);
		}
	}

	void writeRemainingLeafNode(DataOutput dataOutput, WBTreeSA_v3<K, V, P>.LeafNode node) throws IOException {
		// - write number of children
		dataOutput.writeInt(node.values.size());
		
		// - write values
		for(V value : node.values) {
			valueConverter.write(dataOutput, value);
		}
	}

	void writeRemainingInnerNode(DataOutput dataOutput, WBTreeSA_v3<K, V, P>.InnerNode node) throws IOException {
		// - write number of children
		dataOutput.writeInt(node.pagePointers.size());

		// -- write separators
		for (K key : node.separators) {
			keyConverter.write(dataOutput, key);
		}

		// -- write ContainerIDs
		for (P childCID : node.pagePointers) {
			cidConverter.write(dataOutput, childCID);
		}
		
		// -- write weights
		for(int w : node.childWeights) {
			dataOutput.writeInt(w);
		}

	}

//		protected int indexEntrySize() {
//			// return BPlusTree.this.container().getIdSize()
//			// + keyConverter.getMaxObjectSize();
//			return this.bPlusTree.container().getIdSize() + this.bPlusTree.keyConverter.getMaxObjectSize();
//		}
//
//		protected int leafEntrySize() {
//			return this.bPlusTree.dataConverter.getMaxObjectSize();
//		}
//
//		protected int headerSize() {
//			return 2 * IntegerConverter.SIZE + BooleanConverter.SIZE + this.bPlusTree.container().getIdSize();
//		}
}