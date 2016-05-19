package xxl.core.indexStructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import xxl.core.indexStructures.BPlusTree.IndexEntry;
import xxl.core.indexStructures.BPlusTree.Node;
import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.IntegerConverter;

/**
 * An alternative <tt>NodeConverter</tt> for {@link BPlusTree.Node}s.
 */
public class BPlusNodeConverter extends Converter<Node> {

	private final BPlusTree bPlusTree;

	public BPlusNodeConverter(BPlusTree tree) {
		this.bPlusTree = tree;
	}

	@Override
	public Node read(DataInput dataInput, Node unused) throws IOException {
		int level = dataInput.readInt();
		if (level == 0) {
			return readRemainingLeafNode(dataInput);
		} else {
			return readRemainingInnerNode(dataInput, level);
		}

		// Saveformat for Leafs:
		// (0 = level :: int)(number of entries :: int)(hasNextNeighbor ::
		// bool)(nextNeighborId :: ContainerID)?(leafEntry :: DataEntry){number}
		// Saveformat for InnerNodes:
		// (0 != level :: int)(number of entries :: int)(hasNextNeighbor ::
		// bool)(nextNeighborId :: ContainerID)?(a :: ContainerID){number}(b ::
		// Separator){number}
	}

	Node readRemainingLeafNode(DataInput dataInput) throws IOException {
		// create Node shell
		Node node = this.bPlusTree.new Node(0);

		// - read number of entries
		int number = dataInput.readInt();

		// -- read info about next neighbor if available
		boolean readNext = dataInput.readBoolean();
		if (readNext) {
			node.nextNeighbor = this.bPlusTree.new IndexEntry(0 + 1);
			// QUE: why is "level+1" used?

			Object nextNeighborId = this.bPlusTree.container().objectIdConverter().read(dataInput);
			node.nextNeighbor.initialize(nextNeighborId);
		} else {
			node.nextNeighbor = null;
		}

		// -- read the content of the node
		for (int i = 0; i < number; i++) {
			Object entry = this.bPlusTree.dataConverter.read(dataInput);
			node.entries.add(entry);
		}

		return node;
	}

	Node readRemainingInnerNode(DataInput dataInput, int level) throws IOException {
		// create Node shell
		Node node = this.bPlusTree.new Node(level);

		// - read number of entries
		int number = dataInput.readInt();

		// -- read info about next neighbor if available
		boolean readNext = dataInput.readBoolean();
		if (readNext) {
			node.nextNeighbor = this.bPlusTree.new IndexEntry(level + 1); // indirect call of constructor: IndexEntry(level + 1)
			// QUE: why is "level+1" used?

			Object nextNeighborId = this.bPlusTree.container().objectIdConverter().read(dataInput);
			node.nextNeighbor.initialize(nextNeighborId);
		} else {
			node.nextNeighbor = null;
		}

		// -- read the ContainerIDs of the IndexEntries
		Object[] containerIDs = new Object[number];
		for (int i = 0; i < number; i++) {
			containerIDs[i] = this.bPlusTree.container().objectIdConverter().read(dataInput);
		}

		// -- reads the Separators for the IndexEntries
		Separator[] separators = new Separator[number];
		for (int i = 0; i < number; i++) {
			separators[i] = (Separator) this.bPlusTree.createSeparator.invoke(this.bPlusTree.keyConverter.read(dataInput));
		}

		// -- constructs the IndexEntries and adds them to the Node
		for (int i = 0; i < number; i++) {
			IndexEntry ie = (this.bPlusTree.new IndexEntry(node.level)).initialize(containerIDs[i], separators[i]);
			node.entries.add(ie);
		}

		return node;
	}

	@Override
	public void write(DataOutput dataOutput, Node node) throws IOException {
		dataOutput.writeInt(node.level);
		if (node.level == 0) {
			writeRemainingLeafNode(dataOutput, node);
		} else {
			writeRemainingInnerNode(dataOutput, node);
		}
	}

	void writeRemainingLeafNode(DataOutput dataOutput, Node node) throws IOException {

		dataOutput.writeInt(node.number());

		if (node.nextNeighbor != null) {
			dataOutput.writeBoolean(true);
			this.bPlusTree.container().objectIdConverter().write(dataOutput, node.nextNeighbor.id());
		} else {
			dataOutput.writeBoolean(false);
		}

		// -- write content
		for (Object entry : node.entries) {
			this.bPlusTree.dataConverter.write(dataOutput, entry);
		}

	}

	void writeRemainingInnerNode(DataOutput dataOutput, Node node) throws IOException {

		dataOutput.writeInt(node.number());

		if (node.nextNeighbor != null) {
			dataOutput.writeBoolean(true);
			this.bPlusTree.container().objectIdConverter().write(dataOutput, node.nextNeighbor.id());
		} else {
			dataOutput.writeBoolean(false);
		}

		// -- write ContainerIDs
		for (IndexEntry ie : (List<IndexEntry>) node.entries) {
			this.bPlusTree.container().objectIdConverter().write(dataOutput, ie.id());
		}

		// -- write Separators
		for (IndexEntry ie : (List<IndexEntry>) node.entries) {
			this.bPlusTree.keyConverter.write(dataOutput, ie.separator.sepValue());
		}
	}

	protected int indexEntrySize() {
		// return BPlusTree.this.container().getIdSize()
		// + keyConverter.getMaxObjectSize();
		return this.bPlusTree.container().getIdSize() + this.bPlusTree.keyConverter.getMaxObjectSize();
	}

	protected int leafEntrySize() {
		return this.bPlusTree.dataConverter.getMaxObjectSize();
	}

	protected int headerSize() {
		return 2 * IntegerConverter.SIZE + BooleanConverter.SIZE + this.bPlusTree.container().getIdSize();
	}
}