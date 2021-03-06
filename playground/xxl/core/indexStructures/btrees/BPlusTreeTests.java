package xxl.core.indexStructures.btrees;

/* XXL: The eXtensible and fleXible Library for data processing

 Copyright (C) 2000-2011 Prof. Dr. Bernhard Seeger
 Head of the Database Research Group
 Department of Mathematics and Computer Science
 University of Marburg
 Germany

 This library is free software; you can redistribute it and/or
 modify it under the terms of the GNU Lesser General Public
 License as published by the Free Software Foundation; either
 version 3 of the License, or (at your option) any later version.

 This library is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 Lesser General Public License for more details.

 You should have received a copy of the GNU Lesser General Public
 License along with this library;  If not, see <http://www.gnu.org/licenses/>. 

 http://code.google.com/p/xxl/

 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Random;

import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.cursors.Cursors;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.functions.Identity;
import xxl.core.indexStructures.BPlusNodeConverter;
import xxl.core.indexStructures.BPlusTree;
import xxl.core.indexStructures.descriptors.BigIntegerKeyRange;
import xxl.core.indexStructures.descriptors.BigIntegerSeparator;
import xxl.core.io.LRUBuffer;
import xxl.core.io.converters.BigIntegerConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.Converters;
import xxl.core.io.converters.MeasuredConverter;

public class BPlusTreeTests {

	public static final int BLOCK_SIZE = 1024;
	public static final float MIN_RATIO = 0.5f;
	public static final int BUFFER_SIZE = 10;
	public static final int NUMBER_OF_BITS = 256;
	public static final int MAX_OBJECT_SIZE = 78;
	public static final int NUMBER_OF_ELEMENTS = 10000;

	private static BPlusTree createTree(String testFile) {
		System.out.println("Creating new BPlusTree on container files: \"" + testFile + "\" ...");
		System.out.println("Initialization of the B+ tree.");
		
		BPlusTree tree = new BPlusTree(BLOCK_SIZE, MIN_RATIO, true);
		
		Function<Object, Object> getKey = new AbstractFunction<Object, Object>() {
			@Override
			public Object invoke(Object argument) {
				return argument;
			}
		};

		BufferedContainer treeContainer = new BufferedContainer(new ConverterContainer(new BlockFileContainer(testFile, BLOCK_SIZE),
				tree.nodeConverter()), new LRUBuffer<Object, Object, Object>(BUFFER_SIZE), true);

		// TODO: BufferedContainer macht Probleme hier!!
		// Container treeContainer = new ConverterContainer(new
		// BlockFileContainer(name, BLOCK_SIZE), tree.nodeConverter());

//		MeasuredConverter<BigInteger> measuredBigIntegerConverter = new MeasuredConverter<BigInteger>() {
//			@Override
//			public int getMaxObjectSize() {
//				return MAX_OBJECT_SIZE;
//			}
//
//			@Override
//			public BigInteger read(DataInput dataInput, BigInteger object) throws IOException {
//				return BigIntegerConverter.DEFAULT_INSTANCE.read(dataInput, object);
//			}
//
//			@Override
//			public void write(DataOutput dataOutput, BigInteger object) throws IOException {
//				BigIntegerConverter.DEFAULT_INSTANCE.write(dataOutput, object);
//			}
//		};

		MeasuredConverter<BigInteger> measuredBigIntegerConverter = 
				Converters.createMeasuredConverter(MAX_OBJECT_SIZE, BigIntegerConverter.DEFAULT_INSTANCE); 

		tree.initialize(getKey, treeContainer, measuredBigIntegerConverter, measuredBigIntegerConverter,
				BigIntegerSeparator.FACTORY_FUNCTION, BigIntegerKeyRange.FACTORY_FUNCTION);

		System.out.println("Initialization of the B+ tree finished.");

		return tree;
	}

	private static BPlusTree createTree_alternateConverter(String testFile) {
		System.out.println("Creating new BPlusTree on container files: \"" + testFile + "\" ...");
		System.out.println("Initialization of the B+ tree.");

		BPlusTree tree = new BPlusTree(BLOCK_SIZE, MIN_RATIO, true);

		//-- create the alternate NodeConverter
		Converter<BPlusTree.Node> nodeConverter = new BPlusNodeConverter(tree);

		BufferedContainer treeContainer = new BufferedContainer(new ConverterContainer(new BlockFileContainer(testFile, BLOCK_SIZE),
				nodeConverter), new LRUBuffer<Object, Object, Object>(BUFFER_SIZE), true);

		// TODO: BufferedContainer macht Probleme hier!!
		// Container treeContainer = new ConverterContainer(new
		// BlockFileContainer(name, BLOCK_SIZE), tree.nodeConverter());

		Function<Object, Object> getKey = Identity.DEFAULT_INSTANCE;
		
		MeasuredConverter<BigInteger> measuredBigIntegerConverter = 
			Converters.createMeasuredConverter(MAX_OBJECT_SIZE, BigIntegerConverter.DEFAULT_INSTANCE); 

		tree.initialize(getKey, treeContainer, measuredBigIntegerConverter, measuredBigIntegerConverter,
				BigIntegerSeparator.FACTORY_FUNCTION, BigIntegerKeyRange.FACTORY_FUNCTION);

		System.out.println("Initialization of the B+ tree finished.");

		return tree;
	}
	
	private static BPlusTree createTree_explicitSplitRatioFunctions(String testFile) {
		System.out.println("Creating new BPlusTree on container files: \"" + testFile + "\" ...");
		System.out.println("Initialization of the B+ tree.");

		BPlusTree tree = new BPlusTree(BLOCK_SIZE, MIN_RATIO, true);

		//-- create the alternate NodeConverter
		Converter<BPlusTree.Node> nodeConverter = new BPlusNodeConverter(tree);

		BufferedContainer treeContainer = new BufferedContainer(new ConverterContainer(new BlockFileContainer(testFile, BLOCK_SIZE),
				nodeConverter), new LRUBuffer<Object, Object, Object>(BUFFER_SIZE), true);

		// TODO: BufferedContainer macht Probleme hier!!
		// Container treeContainer = new ConverterContainer(new
		// BlockFileContainer(name, BLOCK_SIZE), tree.nodeConverter());

		Function<Object, Object> getKey = Identity.DEFAULT_INSTANCE;
		
		MeasuredConverter<BigInteger> measuredBigIntegerConverter = 
			Converters.createMeasuredConverter(MAX_OBJECT_SIZE, BigIntegerConverter.DEFAULT_INSTANCE); 
		
		// explicit definition of getSplit(Min/Max)Ratio functions to be able to set breakpoints
		Function<BPlusTree.Node, Float> getSplitMinRatio = new AbstractFunction<BPlusTree.Node, Float>() {
			public Float invoke(BPlusTree.Node node) {
				System.out.println("HURZ splitMinRatio");
				return MIN_RATIO;
			}
		};
		
		Function<BPlusTree.Node, Float> getSplitMaxRatio = new AbstractFunction<BPlusTree.Node, Float>() {
			public Float invoke(BPlusTree.Node node) {
				System.out.println("HURZ splitMaxRatio");
				return 1.0f;
			}
		};
		
		//---> ok, the splitRatio functions never get called from BPlusTree
		
		tree.initialize(getKey, treeContainer, measuredBigIntegerConverter, measuredBigIntegerConverter,
				BigIntegerSeparator.FACTORY_FUNCTION, BigIntegerKeyRange.FACTORY_FUNCTION,
				getSplitMinRatio, getSplitMaxRatio);		
		
		System.out.println("Initialization of the B+ tree finished.");

		return tree;
	}


	public static void testBPlusTree(BPlusTree bpTree) throws IOException {

		// Generate test data
		System.out.println();
		System.out.println("-- Generating random test data");
		Random random = new Random(42);
		for (int i = 0; i < 10; i++)
			new BigInteger(NUMBER_OF_BITS, random);
		for (int i = 0; i < NUMBER_OF_ELEMENTS; i++) {
			bpTree.insert(new BigInteger(NUMBER_OF_BITS, random));
			if (i % (NUMBER_OF_ELEMENTS / 10) == 0)
				System.out.print((i / (NUMBER_OF_ELEMENTS / 100)) + "%, ");
		}
		System.out.println("100%");
		bpTree.wasReorg();
		System.out.println("Resulting tree height: " + bpTree.height());

		// delete test
		random = new Random(42);
		System.out.println("-- Remove Test:");
		int error = 0;
		for (int i = 0; i < 10; i++) {
			BigInteger rem = new BigInteger(NUMBER_OF_BITS, random).negate();
			BigInteger fou = (BigInteger) bpTree.remove(rem);
			if (fou != null)
				error++;
		}
		if (error > 0)
			System.err.println("false positive: " + error);
		error = 0;
		for (int i = 0; i < NUMBER_OF_ELEMENTS * .1 - 10; i++) {
			BigInteger rem = new BigInteger(NUMBER_OF_BITS, random);
			BigInteger fou = (BigInteger) bpTree.remove(rem);
			if (!rem.equals(fou))
				error++;
		}
		if (error > 0)
			System.err.println("false negative: " + error);

		// query test
		BPlusTree.KeyRange region = (BPlusTree.KeyRange) bpTree.rootDescriptor();
		BigInteger min = (BigInteger) region.minBound();
		BigInteger max = (BigInteger) region.maxBound();
		BigInteger temp = max.subtract(min).divide(new BigInteger("10"));
		System.out.println("-- Query Test:");
		BigInteger minQR = min.add(temp);
		BigInteger maxQR = minQR.add(temp);
		System.out.println("Query: [" + minQR + ", " + maxQR + "]");
		Iterator<?> results = bpTree.rangeQuery(minQR, maxQR);
		System.out.println("Responses number: " + Cursors.count(results));
		System.out.println("End.");
	}

	public static void main(String[] args) throws Exception {

		String fileName;
		if (args.length > 0) { // custom container file
			fileName = args[0];
		} else { // std container file
			String CONTAINER_FILE_PREFIX = "simple_bplus_tree_test";
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

//		BPlusTree bpTree = createTree(fileName);
//		BPlusTree bpTree = createTree_alternateConverter(fileName);
		BPlusTree bpTree = createTree_explicitSplitRatioFunctions(fileName);
		testBPlusTree(bpTree);
	}
}
