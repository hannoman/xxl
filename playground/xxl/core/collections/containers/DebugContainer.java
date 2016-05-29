package xxl.core.collections.containers;

import java.util.NoSuchElementException;

/** Decorates a container for simple printf-debugging.
 */
public class DebugContainer extends ConstrainedDecoratorContainer {

	public DebugContainer(Container container) {
		super(container);		
	}

	@Override
	public Object get(Object id, boolean unfix) throws NoSuchElementException {
		System.out.println("container: GET: "+ id.toString() +" "+ (unfix ? "(U)" : "(F)"));
		return super.get(id, unfix);
	}

	@Override
	public Object insert(Object object, boolean unfix) {
		Object id = super.insert(object, unfix);
		System.out.println("container: PUT: "+ id.toString() +" "+ (unfix ? "(U)" : "(F)"));
		return id;
	}

	@Override
	public void update(Object id, Object object, boolean unfix) throws NoSuchElementException {
		System.out.println("container: UPD: "+ id.toString() +" "+ (unfix ? "(U)" : "(F)"));
		super.update(id, object, unfix);
	}

	
	
}
