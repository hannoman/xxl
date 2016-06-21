package xxl.core.util;

import xxl.core.spatial.rectangles.FixedPointRectangle;

public class SpatialHelpers {

	
	public static long[] centralPoint(FixedPointRectangle rect) {

		long[] left = (long[])rect.getCorner(false).getPoint(); // who needs types anyway?
		long[] right = (long[])rect.getCorner(true).getPoint(); 

		long[] center = new long[rect.dimensions()];
		for(int i=0; i < center.length; i++) {
			center[i] = left[i] + (right[i] - left[i]) / 2;
		}
		
		return center;
	}
	
	
}
