package xxl.core.cursors.samplingcursor;

import java.util.List;

public interface StatefulSampler<V,P> {

	int size();

	List<V> tryToSample(int n);

	P getNode();

	boolean exhausted();
	
}
