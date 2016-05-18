package xxl.core.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import xxl.core.io.Convertable;

/** Might be useful some time, but misses the point for my application, as object types of base types (Interger, Double, etc.) are
 * of no course not instances of Convertable. */
@SuppressWarnings("serial")
public class PairConvertable<A extends Convertable, B extends Convertable> extends Pair<A,B> implements Convertable {

	@Override
	public void read(DataInput dataInput) throws IOException {
		getFirst().read(dataInput);
		getSecond().read(dataInput);		
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		getFirst().write(dataOutput);
		getSecond().write(dataOutput);
	}
	
}