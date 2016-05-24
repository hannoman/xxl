package xxl.core.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import xxl.core.io.converters.Converter;

public class Interval<K extends Comparable<K>> {

	public K lo, hi;
	public boolean loIn, hiIn;
//	private boolean unknownEmptyInterval;
	
	public Interval(K min, boolean includesMin, K max, boolean includesMax) {
		super();
		this.lo = min;
		this.hi = max;
		this.loIn = includesMin;
		this.hiIn = includesMax;
	}

	public Interval(K min, K max) {
		this(min, true, max, true);
	}
	
	public Interval(K key) {
		this(key, key);
	}
	
	/** private empty constructor which doesn't initialize anything. */
	private Interval() {}
	
	public boolean isEmpty() {
		return lo.compareTo(hi) > 0 || (lo.compareTo(hi) == 0 && (!loIn || !hiIn));
	}
	
	public boolean contains(Interval<K> other) {
		return borderCompare(lo, loIn, other.lo, other.loIn) <= 0
			&& borderCompare(hi, hiIn, other.hi, other.hiIn) >= 0;
	}

	public boolean contains(K key) {
//		return contains(new Interval<K>(key)); // works too
		return locate(key) == 0;
	}
	
	public int locate(K key) {
		if(borderCompare(key, true, lo, loIn) < 0) return -1;
		else if(borderCompare(key, true, hi, hiIn) > 0) return 1;
		else return 0;
	}
	
	public Interval<K> intersection(Interval<K> other) {
		Interval<K> res = new Interval<K>();
		if(borderCompare(lo, loIn, other.lo, other.loIn) >= 0) {
			res.lo = lo; 
			res.loIn = loIn;
		} else {
			res.lo = other.lo; 
			res.loIn = other.loIn;
		}
		
		if(borderCompare(hi, hiIn, other.hi, other.hiIn) <= 0) {
			res.hi = hi; 
			res.hiIn = hiIn;
		} else {
			res.hi = other.hi; 
			res.hiIn = other.hiIn;
		}
		
		return res;
	}
	
	public boolean intersects(Interval<K> other) {
		return !intersection(other).isEmpty();
	}
	
	public Pair<Interval<K>, Interval<K>> split(K key, boolean includedLeft) {
		Interval<K> left  = new Interval<K>(lo, loIn, key, includedLeft);
		Interval<K> right = new Interval<K>(key, !includedLeft, hi, hiIn);
		return new Pair<Interval<K>, Interval<K>>(left, right);
	}
	
	public Interval<K> union(Interval<K> other) {
		Interval<K> res = new Interval<K>();
		if(borderCompare(lo, loIn, other.lo, other.loIn) <= 0) {
			res.lo = lo; 
			res.loIn = loIn;
		} else {
			res.lo = other.lo; 
			res.loIn = other.loIn;
		}
		
		if(borderCompare(hi, hiIn, other.hi, other.hiIn) >= 0) {
			res.hi = hi; 
			res.hiIn = hiIn;
		} else {
			res.hi = other.hi; 
			res.hiIn = other.hiIn;
		}
		
		return res;
	}
	
	public Interval<K> union(K key) {
		return union(new Interval<K>(key));
	}
	
	public boolean equals(Object otherRaw) {
		try {
			@SuppressWarnings("unchecked")
			Interval<K> other = (Interval<K>) otherRaw;

			return lo.equals(other.lo)
				&& loIn == other.loIn
				&& hi.equals(other.hi)
				&& hiIn == other.hiIn;
		}
		catch (ClassCastException cce) {
			return false;
		}
		catch (NullPointerException npe) {
			return false;
		}
	}
	
	public String toString() {
		String s = "";
		s += loIn ? "[" : "]";
		s += lo == null ? "null" : lo.toString();
		s += ", ";
		s += hi == null ? "null" : hi.toString();
		s += hiIn ? "]" : "[";
		return s;
	}
	
	private int borderCompare(K b1, boolean i1, K b2, boolean i2) {
		int bCompare = b1.compareTo(b2);
		if(bCompare != 0)
			return bCompare;
		else {
			if(i1 && !i2) return -1;
			else if(!i1 && i2) return 1;
			else { assert i1 == i2; return 0; }
		}
	}
	
	public static <T extends Comparable<T>> Converter<Interval<T>> getConverter(Converter<T> kConv) {
		@SuppressWarnings("serial")
		Converter<Interval<T>> compConv = new Converter<Interval<T>>() {

			@Override
			public Interval<T> read(DataInput dataInput, Interval<T> object) throws IOException {
				if(object == null)
					object = new Interval<T>();
				
				kConv.read(dataInput, object.lo);
				object.loIn = dataInput.readBoolean();
				kConv.read(dataInput, object.hi);
				object.hiIn = dataInput.readBoolean();
				return object;
			}

			@Override
			public void write(DataOutput dataOutput, Interval<T> object) throws IOException {
				kConv.write(dataOutput, object.lo);
				dataOutput.writeBoolean(object.loIn);
				kConv.write(dataOutput, object.hi);
				dataOutput.writeBoolean(object.hiIn);				
			}			
		};
		
		return compConv;
	}
	
}
