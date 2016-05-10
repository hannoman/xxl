package xxl.core.indexStructures;

import java.util.List;
import java.util.function.Function;

import xxl.core.collections.containers.Container;
import xxl.core.io.converters.Converter;

/** Interface for doing sanity checks on self implemented maps (= trees). */
public interface TestableMap<K extends Comparable<K>, V, P> {

	/** Initialize the tree with a raw container (e.g. <tt>BlockFileContainer</tt>) and the needed converters.
	 * We construct the usable node container from them ourselfes.
	 * 
	 * @param rawContainer container to store the data in
	 * @param keyConverter converter for the key-type K 
	 * @param valueConverter converter for the value type V
	 */
	public void initialize_buildContainer(Container rawContainer, Converter<K> keyConverter, Converter<V> valueConverter);

	/**
	 * Insertion. 
	 */
	public void insert(V value);

	/**
	 * Lookup.
	 */
	public List<V> get(K key);

	int height();

	Function<V, K> getGetKey();
	
	
}