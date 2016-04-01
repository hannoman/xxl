package xxl.core.indexStructures.btrees;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.indexStructures.BPlusTree;

/** Trying to instantiate a BPlus-Tree. A small step for mankind, but a ... */
public class BPlusInstantiateTest {

	/**
	 * Block size of the used HDD.
	 * 
	 * There seems to be no way to actually determine the block-size out of pure
	 * Java. See also:
	 * http://stackoverflow.com/questions/10484115/get-system-block-size-in-java
	 */
	static int BLOCK_SIZE = 1024;

//	public static void createTree(String[] args) {
//		
//		String container_prefix = null;
//		if(args.length > 0) { // custom container file
//			container_prefix = args[0];			
//		}
//		else { // std container file
//			String container_file_prefix = "simple_bplus_tree_test";
//			System.out.println("No filename as program parameter found. Using standard: \""+ "<project dir>\\temp_data\\"+ container_file_prefix +"\"");
//			
//			// and the whole thing in short
//			Path curpath = Paths.get("").toAbsolutePath();
//			if(!curpath.resolve("temp_data").toFile().exists()) {
//				System.out.println("Error: Couldn't find \"test_data\" directory.");
//				return;
//			}
//			container_prefix = curpath.resolve("temp_data").resolve(container_file_prefix).toString();
//			System.out.println("resolved to: \""+ container_prefix +"\".");			
//		}
//		
//		BPlusTree tree = new BPlusTree(BLOCK_SIZE, 0.5, true);
//		
//		Container container = new BlockFileContainer(container_prefix, BLOCK_SIZE);
//		
//		tree.initialize(getDescriptor, container, 10, 20);
//		
//
//	}
	
	public static void main(String[] args) {
//		testLambdas();
	}
	
	private static void endlessLoopToLetJavaRun(){
		int x = 10;
		int i = 0;
		while (true) {
			x = (27 * x + 17) % (19 * 31);
			// if (x > Math.pow(10, 6)) {
			// x /= 2;
			// }
			i++;
			if (i % 10000 == 0) {
				System.out.println("iteration #" + i + " : " + x);
			}
		}
	}
	
//	public static void testLambdas() {
//		Function<Integer, Integer> f = (x -> x + 5);
//		System.out.println(f.apply(7));
//		System.out.println((x -> x*x) 17);
//	}

}
