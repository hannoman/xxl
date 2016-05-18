package xxl.core.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import xxl.core.io.converters.Converter;

@SuppressWarnings("serial")
public class PairConverter<A,B> extends Converter<Pair<A,B>> {
	Converter<A> aConv;
	Converter<B> bConv;

	public PairConverter(Converter<A> aConv, Converter<B> bConv) {
		super();
		this.aConv = aConv;
		this.bConv = bConv;
	}

	@Override
	public Pair<A, B> read(DataInput dataInput, Pair<A, B> object) throws IOException {
		if(object == null) 
			object = new Pair<A,B>();
		object.setFirst(aConv.read(dataInput));
		object.setSecond(bConv.read(dataInput));
		return object;
	}

	@Override
	public void write(DataOutput dataOutput, Pair<A, B> object) throws IOException {
		aConv.write(dataOutput, object.getFirst());
		bConv.write(dataOutput, object.getSecond());		
	}
	
}
