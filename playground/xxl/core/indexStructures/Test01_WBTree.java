

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import xxl.core.collections.containers.CastingContainer;
import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.DebugContainer;
import xxl.core.collections.containers.TypeSafeContainer;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.indexStructures.WBTreeSA_v3;
import xxl.core.indexStructures.WBTreeSA_v3.InnerNode;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.IntegerConverter;


/**
 * Should test the NodeConverter of a WB-tree separately and is therefore abandoned, as we need
 * an enclosing instance.
 *  
 * --> DELETE
 */
public class Test01_WBTree {
	
	static final int BLOCK_SIZE = 1024;
	
//	public static void testConverters(WBTree_NodeConverter nodeConverter) {
//		ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
//		
//		// create test node
//		WBTreeSA_v3<Integer, String, Short>.InnerNode node = new InnerNode();
//		// TODO: doesn't work without enclosing tree instance
//		
//	}
	
	private static WBTreeSA_v3<Integer, Integer, Long> createTree(String testFile) {
		
		// obsolete as all converter-issues are now handled by the initialisator
//		WBTreeSA_v3<Integer, Integer, Long> tree = new WBTreeSA_v3<Integer, Integer, Long>(
//				60, 										// leafParam
//				10, 										// branchingParam
//				(x -> x), 									// getKey
//				IntegerConverter.DEFAULT_INSTANCE, 			// keyConverter 
//				IntegerConverter.DEFAULT_INSTANCE);			// valueConverter
		
		WBTreeSA_v3<Integer, Integer, Long> tree = new WBTreeSA_v3<Integer, Integer, Long>(60,10,(x -> x));
		
		Container treeRawContainer = new BlockFileContainer(testFile, BLOCK_SIZE);			
		
//		TypeSafeContainer<Long, WBTreeSA_v3<Integer, Integer, Long>.Node> treeContainer = 
//				new CastingContainer<Long, WBTreeSA_v3<Integer, Integer, Long>.Node>(
//						new DebugContainer(
//								new ConverterContainer(treeRawContainer, tree.getNodeConverter())
//								)
//						); 
		
		WBTreeSA_v3<Integer, Integer, Long>.NodeConverter nodeConverter = 
				tree.new NodeConverter(IntegerConverter.DEFAULT_INSTANCE, 
						IntegerConverter.DEFAULT_INSTANCE, 
						treeRawContainer.objectIdConverter());
		
		TypeSafeContainer<Long, WBTreeSA_v3<Integer, Integer, Long>.Node> treeContainer = 
				new CastingContainer<Long, WBTreeSA_v3<Integer, Integer, Long>.Node>(
						new DebugContainer(
								new ConverterContainer(treeRawContainer, nodeConverter)
								)
						);
		
		tree.initialize_withReadyContainer(treeContainer);

		System.out.println("Initialization of the tree finished.");

		return tree;
	}
	
	public static void main(String[] args) throws Exception {

		String fileName;
		if (args.length > 0) { // custom container file
			fileName = args[0];
		} else { // std container file
			String CONTAINER_FILE_PREFIX = "wb_tree_test01";
			String test_data_dirname = "temp_data";
			System.out.println("No filename as program parameter found. Using standard: \"" + "<project dir>\\" + test_data_dirname + "\\"
					+ CONTAINER_FILE_PREFIX + "\"");

			// and the whole thing in short
			Path curpath = Paths.get("").toAbsolutePath();
			if (!curpath.resolve(test_data_dirname).toFile().exists()) {
				System.out.println("Error: Couldn't find \"" + test_data_dirname + "\" directory.");
				return;
			}
			fileName = curpath.resolve("temp_data").resolve(CONTAINER_FILE_PREFIX).toString();
			System.out.println("resolved to: \"" + fileName + "\".");

		}

		WBTreeSA_v3<Integer, Integer, Long> wbTree = createTree(fileName);		
		//testConverters(wbTree.getNodeConverter());
	}

}
