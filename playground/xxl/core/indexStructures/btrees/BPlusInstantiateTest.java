package xxl.core.indexStructures.btrees;

import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.functions.FunJ8;
import xxl.core.indexStructures.BPlusTree;
import xxl.core.util.Pair;

/** Trying to instantiate a BPlus-Tree. A small step for mankind, but a ... */
public class BPlusInstantiateTest {

	public static void createBTree(String container_prefix, int blockSize) {
		//- construction of tree
		BPlusTree tree = new BPlusTree(blockSize, 0.5, true);
		//- prepare for initialisation (try initialisator with least parameters)
		Container container = new BlockFileContainer(container_prefix, blockSize);
		
		Function<Pair<Integer, Double>, Integer> getDescriptorNew = (t -> t.getElement1());
		xxl.core.functions.Function getDescriptor = FunJ8.toOld(getDescriptorNew);
		
		//- initialize
		tree.initialize(getDescriptor, container, 10, 20);
		

	}
	
	private static String resolveFilename(String fileName) throws FileNotFoundException {
		String result;
		
		String testdata_dirname = "temp_data";
//		 System.out.println("Trying to resolve to: \""+"<project dir>\\"+ testdata_dirname +"\\"+ fileName + "\"");

		// and the whole thing in short
		Path curpath = Paths.get("").toAbsolutePath();
		if (!curpath.resolve(testdata_dirname).toFile().exists()) {
			throw new FileNotFoundException("Error: Couldn't find \"" + testdata_dirname + "\" directory.");
		}
		result = curpath.resolve(testdata_dirname).resolve(fileName).toString();
		System.out.println("resolved to: \"" + result + "\".");
		return result;
	}

	
	public static void main(String[] args) throws FileNotFoundException {
		createBTree(resolveFilename("btree_init_test01"));
	}
		
}
