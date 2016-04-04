package xxl.core.collections.containers;

import java.util.Iterator;
import java.util.NoSuchElementException;

import xxl.core.cursors.Cursor;
import xxl.core.functions.Function;
import xxl.core.io.converters.FixedSizeConverter;
import xxl.core.util.Decorator;

public class CastingContainer<K, V> implements TypeSafeContainer<K, V>, Decorator<Container> {
	
	/**
	 * A reference to the container to be decorated. This reference is
	 * used to perform method calls on the <i>original</i> container.
	 */
	protected Container container;

	/**
	 * Constructs a new DecoratorContainer that decorates the specified
	 * container.
	 *
	 * @param container the container to be decorated.
	 */
	public CastingContainer(Container container) {
		this.container = container;
	}
	
	@Override
	public Container getDecoree() {
		return container;
	}

	@Override
	public void clear() {
		container.clear();		
	}

	@Override
	public void close() {
		container.close();		
	}

	@Override
	public boolean contains(K id) {
		return container.contains(id);
	}

	@Override
	public void flush() {
		container.flush();		
	}

	@Override
	public void flush(K id) {
		container.flush(id);
	}

	@Override
	public V get(K id, boolean unfix) throws NoSuchElementException {
		return (V) container.get(id, unfix);
	}

	@Override
	public V get(K id) throws NoSuchElementException {
		return (V)container.get(id);
	}

	@Override
	public Iterator<V> getAll(Iterator<K> ids, boolean unfix) throws NoSuchElementException {
		return (Iterator<V>)container.getAll(ids, unfix);
	}

	@Override
	public Iterator<V> getAll(Iterator<K> ids) throws NoSuchElementException {
		return (Iterator<V>)container.getAll(ids);
	}

	@Override
	public Iterator<K> ids() {
		return (Iterator<K>)container.ids();
	}

	@Override
	public K insert(V object, boolean unfix) {
		return (K)container.insert(object, unfix);
	}

	@Override
	public K insert(V object) {
		return (K)container.insert(object);
	}

	@Override
	public Iterator<K> insertAll(Iterator<V> objects, boolean unfix) {
		return (Iterator<K>)container.insertAll(objects, unfix);
	}

	@Override
	public Iterator<K> insertAll(Iterator<V> objects) {
		return (Iterator<K>)container.insertAll(objects);
	}

	@Override
	public boolean isUsed(K id) {
		return container.isUsed(id);
	}

	@Override
	public Cursor<V> objects() {
		return (Cursor<V>)container.objects();
	}

	@Override
	public FixedSizeConverter<K> objectIdConverter() {
		return (FixedSizeConverter<K>)container.objectIdConverter();
	}

	@Override
	public int getIdSize() {
		return container.getIdSize();
	}

	@Override
	public void remove(K id) throws NoSuchElementException {
		container.remove(id);		
	}

	@Override
	public void removeAll(Iterator<K> ids) throws NoSuchElementException {
		container.removeAll(ids);		
	}

	@Override
	public K reserve(Function getObject) {
		return (K)container.reserve(getObject);
	}

	@Override
	public int size() {
		return container.size();
	}

	@Override
	public void unfix(K id) throws NoSuchElementException {
		container.unfix(id);		
	}

	@Override
	public void unfixAll(Iterator<K> ids) throws NoSuchElementException {
		container.unfixAll(ids);		
	}

	@Override
	public void update(K id, V object, boolean unfix) throws NoSuchElementException {
		container.update(id, object, unfix);		
	}

	@Override
	public void update(K id, V object) throws NoSuchElementException {
		container.update(id, object);		
	}

	@Override
	public void updateAll(Iterator<K> ids, Function<K, V> function, boolean unfix) throws NoSuchElementException {
		container.updateAll(ids, function, unfix);		
	}

	@Override
	public void updateAll(Iterator<K> ids, Function<K, V> function) throws NoSuchElementException {
		container.updateAll(ids, function);		
	}

	@Override
	public void updateAll(Iterator<K> ids, Iterator<V> objects, boolean unfix) throws NoSuchElementException {
		container.updateAll(ids, objects, unfix);		
	}

	@Override
	public void updateAll(Iterator<K> ids, Iterator<V> objects) throws NoSuchElementException {
		container.updateAll(ids, objects);		
	}

	@Override
	public K[] batchInsert(V[] blocks) {
		return (K[])container.batchInsert(blocks);
	}
		

}
