package xxl.core.util;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;
import java.util.Arrays;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;

public class CursorChain<V> extends AbstractCursor<V> {
	
	LinkedList<Supplier<Cursor<V>>> continuations;
	Cursor<V> current;
	
	public CursorChain(Supplier<Cursor<V>> ... conts) {
		this.continuations = new LinkedList<Supplier<Cursor<V>>>(Arrays.asList(conts));
	}
	
	@Override
	protected boolean hasNextObject() {
		while(current == null || !current.hasNext()) {
			if(continuations.isEmpty())
				return false;
			else
				current = continuations.pop().get();
		}
		return true;
	}
	
	@Override
	protected V nextObject() {
		return current.next();
	}
	
}
