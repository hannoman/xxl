package xxl.core.util;

import xxl.core.indexStructures.Descriptor;
import xxl.core.indexStructures.Separator;

/**
 * This class represents key ranges (i.e. intervals of keys). 
 * The interface uses generics now, as i like to keep it.
 * 
 * The endpoints are always included.
 */
public class KeyRangeG<K extends Comparable<K>> implements Descriptor {
    
	public K min, max;
	
    public KeyRangeG(K min, K max) {
		super();
		this.min = min;
		this.max = max;
	}
    
    /**
     * Checks whether the given <tt>Descriptor</tt> overlaps the current
     * <tt>KeyRange</tt>.
     * 
     * @param descriptor
     *            the <tt>KeyRange</tt> to check
     * @return <tt>true</tt> if the given <tt>Descriptor</tt> an
     *         instance of of <tt>KeyRange</tt> and overlaps this
     *         <tt>KeyRange</tt> and <tt>false</tt> otherwise.
     * 
     * @see xxl.core.indexStructures.Descriptor#overlaps(xxl.core.indexStructures.Descriptor)
     */
    public boolean overlaps(Descriptor descriptor) {
        if (!(descriptor instanceof KeyRangeG)) return false;
        KeyRangeG<K> qInterval = (KeyRangeG<K>) descriptor;
        return contains(qInterval.min) || qInterval.contains(this.min); 
    }
    /**
     * Checks whether the given <tt>Descriptor</tt> is totally contained
     * in this <tt>KeyRange</tt>.
     * 
     * @param descriptor
     *            the <tt>KeyRange</tt> to check
     * @return <tt>true</tt> if the given <tt>Descriptor</tt> an
     *         instance of <tt>KeyRange</tt> and lies totally in this
     *         <tt>KeyRange</tt> and <tt>false</tt> otherwise
     * 
     * @see xxl.core.indexStructures.Descriptor#contains(xxl.core.indexStructures.Descriptor)
     */
    public boolean contains(Descriptor descriptor) {
        if (!(descriptor instanceof KeyRangeG)) return false;
        KeyRangeG<? extends K> other = (KeyRangeG<? extends K>) descriptor;
        return contains(other.min) && contains(other.max);
    }
    /**
     * Checks whether the current <tt>KeyRangel</tt> contains the given
     * key.
     * 
     * @param key
     *            the key to check
     * @return <tt>true</tt> if key lies in this <tt>KeyRangel</tt> and
     *         <tt>false</tt> otherwise
     */
    public boolean contains(K key) {
    	// TODO: null signal negative/positive infinity when used as a bound. In the case of key we don't now whether it should be positive or negative.
    	// So we just say it's not contained.
    	if(key == null) return false;
        return (min==null || min.compareTo(key) <= 0) && (max==null || max.compareTo(key) >= 0);
    }
    /**
     * Tests whether this <tt>KeyRange</tt> equals the given
     * <tt>KeyRange</tt>.
     * 
     * @param object
     *            the second <tt>KeyRange</tt>
     * @return <tt>true</tt> if the given object an instance of
     *         <tt>KeyRange</tt> and equals the current <tt>KeyRange</tt>
     *         and <tt>false</tt> otherwise
     */
    public boolean equals(Object object) {
        if (!(object instanceof KeyRangeG)) return false;
        KeyRangeG<? extends K> other = (KeyRangeG<? extends K>) object;
        return min.compareTo(other.min) == 0 && max.compareTo(other.max) == 0;
    }
    /**
     * Builds the union of two <tt>KeyRanges</tt>. The current
     * <tt>KeyRange</tt> will be changed and returned.
     * 
     * @param descriptor
     *            the <tt>KeyRange</tt> which is to unite with the current
     *            <tt>KeyRange</tt>
     * 
     * @see xxl.core.indexStructures.Descriptor#union(xxl.core.indexStructures.Descriptor)
     */
    public void union(Descriptor descriptor) {
        if (descriptor instanceof KeyRangeG) {
            KeyRangeG<? extends K> other = (KeyRangeG<? extends K>) descriptor;
            if (other.min == null || min.compareTo(other.min) > 0)
                    this.min = other.min;
            if (other.max == null || max.compareTo(other.max) < 0)
                    this.max = other.max;
        } else {
        	// perhaps allow unions with single points of K ? 
        	throw new UnsupportedOperationException();
        }
    }
    /**
     * Builds the union of this <tt>KeyRanges</tt> and a key. The union is
     * the minimal extension of the current <tt>KeyRange</tt> which
     * contains the given key. The current <tt>KeyRange</tt> will be
     * changed and returned.
     * 
     * @param key
     *            the key which is to unite with the current
     *            <tt>KeyRange</tt>
     */
    public void union(Comparable key) {
    	K kkey;
    	try {
    		kkey = (K) key;
    	} catch (ClassCastException e) {
    		System.out.println("Catched ClassCastException: "+ e.getMessage());
    		return;
    	}
        if (min.compareTo(kkey) > 0) min = kkey;
        else if (max.compareTo(kkey) < 0) max = kkey;
    }
    
    /** Returns the intersection of this with other. */ 
    public KeyRangeG<K> intersect(KeyRangeG<? extends K> other) {
    	K bmin = min, bmax = max;
    	if(min == null || (other.min != null && min.compareTo(other.min) < 0))
    		bmin = other.min;
    	if(max == null || (other.max != null && max.compareTo(other.max) > 0))
    		bmax = other.max;
    	return new KeyRangeG<K>(bmin, bmax);
    }    
    
    /**
     * Checks whether this <tt>KeyRangel</tt> is a point (i.e. minimal and
     * maximal bounds are equal).
     * 
     * @return <tt>true</tt> if this <tt>KeyRangel</tt> is a point and
     *         <tt>false</tt> otherwise
     */
    public boolean isPoint() {
        return min != null && max != null && min.compareTo(max) == 0;
    }
    /**
     * Overwrites the method
     * {@link xxl.core.indexStructures.Separator#isDefinite()}so that is
     * always returns <tt>true</tt>.
     * 
     * @return <tt>true</tt>
     */
    public boolean isDefinite() {
        return true;
    }
    public String toString() {
        StringBuffer sb = new StringBuffer("[");
        sb.append(min == null ? "-inf" : min.toString());
        if (!isPoint()) {
            sb.append(", ");
            sb.append(max == null ? "+inf" : max.toString());
        }
        sb.append("]");
        return sb.toString();
    }

	@Override
	public Object clone() {
		// return new KeyRangeG<K>((K) min.clone(), (K) max.clone());
		return new KeyRangeG<K>(min, max); // FIXME
	}
    
    
    
    
    
}