package xxl_dk;

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

	public static void main(String[] args) {
		BPlusTree tree = new BPlusTree(BLOCK_SIZE, 0.5, true);

		// tree.

		// tree.

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

}
