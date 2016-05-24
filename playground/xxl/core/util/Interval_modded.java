package xxl.core.util;

/** Class to represent intervals of any totally ordered type. */
public class Interval_modded<K extends Comparable<K>> {

	protected K [] border;

	protected boolean [] inclusive;

	/**
	 * Constructs a new interval by providing the left and right borders and a comparator.
	 *
	 * @param leftBorder The left border of the interval.
	 * @param leftInclusive <tt>True</tt> iff the left border belongs to the interval.
	 * @param rightBorder The right border of the interval.
	 * @param rightInclusive <tt>True</tt> iff the right border belongs to the interval.
	 * @param comparator The comparator defining the order of the basic data type.
	 * @throws IllegalArgumentException if the interval does not contain any point.
	 */
	public Interval_modded (K leftBorder, boolean leftInclusive, K rightBorder, boolean rightInclusive) throws IllegalArgumentException {
		this.border = (K[]) new Object[] {leftBorder, rightBorder};
		this.inclusive = new boolean [] {leftInclusive, rightInclusive};
	}

	/**
	 * Constructs a new closed interval by providing the left and right borders and a comparator.
	 *
	 * @param leftBorder The left border of the interval.
	 * @param rightBorder the right border of the interval.
	 * @param comparator The comparator defining the order on the basic data type.
	 * @throws IllegalArgumentException if the interval does not contain any point.
	 */
	public Interval_modded (K leftBorder, K rightBorder) throws IllegalArgumentException {
		this(leftBorder, true, rightBorder, true);
	}

	/**
	 * Constructs a new closed interval by providing a single point and a comparator.
	 * That means the interval is defined as follows: [point, point]
	 *
	 * @param point The only point the interval will contain.
	 * @param comparator The comparator defining the order on the basic data type.
	 */
	public Interval_modded (K point) {
		this(point, true, point, true);
	}

	/**
	 * Copy-constructor.
	 * The created new instance of <tt>Interval1D</tt> will be equal to the given interval.
	 * The clone of the interval is another interval that has exactly the
	 * same border properties and the same comparator as the current interval.
	 *
	 * @param interval The interval to be cloned.
	 */
	public Interval_modded (Interval_modded<K> interval) {
		this(interval.border[0], interval.inclusive[0], interval.border[1], interval.inclusive[1]);
	}
	
	/**
	 * Clones this interval.
	 * The produced new <tt>Interval1D</tt> will be equal to this interval.
	 * The clone of the interval is another interval that has exactly the
	 * same border properties and the same comparator as the current interval.
	 * The copy-constructor is called.
	 * <p>Overrides the <code>clone</code> method of <code>Object</code>.
	 *
	 * @return a clone of this interval.
	 * @see #Interval (Interval_modded interval)
	 */
	public Object clone () {
		return new Interval_modded(this);
	}

	/**
	 * Returns a String representation of this interval.
	 * <p>Overrides the <code>toString</code> method of <code>Object</code>.
	 *
	 * @return the String representation of this interval.
	 */
	public String toString () {
		return (inclusive[0]?"[":"]")+border[0]+","+border[1]+((inclusive[1]?"]":"["));
	}

	/**
	 * Returns <tt>true</tt> iff the given object is an interval having the same
	 * border properties and comparator as this interval.
	 * Otherwise <tt>false</tt> is returned.
	 * <p>Overrides the <code>equals</code> method of <code>Object</code>.
	 *
	 * @param object The object, an interval, to be compared with this interval.
	 * @return Returns <tt>true</tt> if the given object is an interval having the same
	 * 		border properties and comparator as this interval.
	 *		Returns <tt>false</tt> if the given object is not an interval, the borders
	 * 		differ in any kind or the used comparators are not equal.
	 */
	public boolean equals (Object object) {
		try {
			Interval_modded<K> interval = (Interval_modded<K>)object;

			for (int i=0; i<2; i++)
				if (inclusive[i]!=interval.inclusive[i] ||
					border[i]!=interval.border[i] && !border[i].equals(interval.border[i]) && border[i].compareTo(interval.border[i])!=0)
					return false;
			return true;
		}
		catch (ClassCastException cce) {
			return false;
		}
		catch (NullPointerException npe) {
			return false;
		}
	}

	/**
	 * Returns the desired border of this interval.
	 *
	 * @param rightBorder Returns the right border if <tt>true</tt>.
	 * @return Returns the interval's right border is the specified parameter is <tt>true</tt>,
	 * 		otherwise (<tt>false</tt>) the interval's left border.
	 */
	public Object border (boolean rightBorder) {
		return border[rightBorder? 1: 0];
	}

	/**
	 * Checks if the specified border is included in this interval.
	 * Returns <tt>true</tt> if the desired border belongs to this interval
	 * otherwise <tt>false</tt>.
	 *
	 * @param rightBorder Examines the right border if <tt>true</tt>. If this parameter
	 * 		is <tt>false</tt> the left border is examined.
	 * @return Returns <tt>true</tt> if the specified border belongs to this interval,
	 * 		otherwise <tt>false</tt>.
	 */
	public boolean includes (boolean rightBorder) {
		return inclusive[rightBorder? 1: 0];
	}

	/**
	 * Checks whether a point is contained by this interval.
	 *
	 * @param point The point to be tested.
	 * @return Returns 0 if the point is contained by this interval,
	 * 		returns -1 if the point is located to the right of this interval, else 1.
	 * @throws IllegalArgumentException  if the point can not be tested properly.
	 */ 
	public int contains (Object point) throws IllegalArgumentException {
		try {
			int result = 0;

			for(int i=0; i<2; i++) {
				int comparison = comparator.compare(border[i], point);

				result += (comparison!=0 || inclusive[i])? Maths.signum(comparison): 1-2*i;
			}
			return result/2;
		}
		catch (Exception e) {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Checks whether an interval is contained by this interval.
	 * The implementation is as follows:
	 * <br><br>
	 * <code><pre>
	 * 	for(int i=0; i<2; i++) {
	 * 		int comparison = Math.signum(comparator.compare(border[i], interval.border[i]));
	 *
	 * 		if (comparison==0? !inclusive[i] && interval.inclusive[i]: comparison==1-2*i)
	 * 			return false;
	 * 	}
	 * 	return true;
	 * </code></pre>
	 * At first the left borders of the intervals are tested to be equal using this
	 * interval's comparator. If this is the case (<code>comparison == 0</code>)
	 * the inclusion of the left borders of intervals has to be checked.
	 * If <code>comparison == 1</code> <tt>false</tt> is returned, because the left
	 * border of the specified interval is less than the left border of this interval and
	 * so the specified interval is larger.
	 * If the left border properties are exactly equal, then the right border properties are
	 * checked in the same way.
	 *
	 * @param interval The interval to be tested.
	 * @return Returns <tt>true</tt> if this intervals contains the given interval,
	 * 		otherwise <tt>false</tt>.
	 * @throws IllegalArgumentException if the interval can not be tested properly.
	 */
	public boolean contains (Interval_modded interval) throws IllegalArgumentException {
		try {
			for(int i=0; i<2; i++) {
				int comparison = Maths.signum(comparator.compare(border[i], interval.border[i]));

				if (comparison==0? !inclusive[i] && interval.inclusive[i]: comparison==1-2*i)
					return false;
			}
			return true;
		}
		catch (Exception e) {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Checks whether an descriptor is contained by this interval.
	 * <b>Note:</b The descriptor is casted to an Interval1D and
	 * the method {@link #contains(Interval_modded)} is called.
	 *
	 * @param descriptor The descriptor to be tested.
	 * @return Returns <tt>true</tt> if this intervals contains the given descriptor,
	 * 		otherwise <tt>false</tt>.
	 * @throws IllegalArgumentException  if the descriptor	can not be tested properly.
	 * @see #contains(Interval_modded)
	 */
	public boolean contains (Descriptor descriptor) throws IllegalArgumentException {
		return contains((Interval_modded)descriptor);
	}

	/**
	 * Checks whether an interval and this interval do overlap.
	 * The implementation is as follows:
	 * <br><br>
	 * <code><pre>
	 * 	int result = 0;
	 *
	 * 	for(int i=0; i<2; i++) {
	 * 		int comparison = comparator.compare(border[i], interval.border[1-i]);

	 * 		result += (comparison!=0 || inclusive[i] && interval.inclusive[1-i])? Math.signum(comparison): 1-2*i;
	 * 	}
	 * 	return result/2;
	 * </code></pre>
	 *
	 * @param interval The interval to be tested.
	 * @return Returns 0 if the interval and this interval do overlap,
	 * 		returns -1 if the interval is located to the right of this interval, else 1.
	 * @throws IllegalArgumentException if the interval can not be tested properly.
	 */
	public int overlaps (Interval_modded interval) throws IllegalArgumentException {
		try {
			int result = 0;

			for(int i=0; i<2; i++) {
				int comparison = comparator.compare(border[i], interval.border[1-i]);

				result += (comparison!=0 || inclusive[i] && interval.inclusive[1-i])? Maths.signum(comparison): 1-2*i;
			}
			return result/2;
		}
		catch (Exception e) {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Checks whether a descriptor and this interval do overlap.
	 * <b>Note:</b The descriptor is casted to an Interval1D and
	 * the method {@link #overlaps(Interval_modded)} is called.
	 *
	 * @param descriptor The descriptor to be tested.
	 * @return Returns 0 if the descriptor and this interval do overlap,
	 * 		returns -1 if the descriptor is located to the right of this interval, else 1.
	 * @throws IllegalArgumentException if the descriptor can not be tested properly.
	 * @see #overlaps(Interval_modded)
	 */
	public boolean overlaps (Descriptor descriptor) throws IllegalArgumentException {
		return overlaps((Interval_modded)descriptor)==0;
	}

	/**
	 * Extends this interval to contain a given interval, too.
	 * The borders of this interval are changed in following way:
	 * <br><br>
	 * <code><pre>
	 * 	for (int i=0; i<2; i++) {
	 * 		int comparison = Math.signum(comparator.compare(border[i], interval.border[i]));
	 *
	 * 		if (comparison==0)
	 * 			inclusive[i] |= interval.inclusive[i];
	 * 		else if (comparison==1-2*i) {
	 * 			inclusive[i] = interval.inclusive[i];
	 * 			border[i] = interval.border[i];
	 * 		}
	 * 	}
	 * 	return this;
	 * </code></pre>
	 * If the given interval is larger than this one, <code>border[i]</code> is set to
	 * <code>interval.border[i]</code> and <code>inclusive[i]</code> is set to
	 * <code>interval.inclusive[i]</code>. If the intervals are equal concerning their
	 * borders, i.e. <code>comparision == 0</code>, then only the inclusion of this
	 * interval's borders will possibly be set.
	 *
	 * @param interval The interval which defines the extension of this interval.
	 * @return Returns this interval, now containg the specified interval, too.
	 * @throws IllegalArgumentException if the union can not be performed properly.
	 */
	public Interval_modded union (Interval_modded interval) throws IllegalArgumentException {
		try {
			for (int i=0; i<2; i++) {
				int comparison = Maths.signum(comparator.compare(border[i], interval.border[i]));

				if (comparison==0)
					inclusive[i] |= interval.inclusive[i];
				else if (comparison==1-2*i) {
					inclusive[i] = interval.inclusive[i];
					border[i] = interval.border[i];
				}
			}
			return this;
		}
		catch (Exception e) {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Extends this interval to contain a given descriptor, too.
	 * <b>Note:</b The descriptor is casted to an Interval1D and
	 * the method {@link #union(Interval_modded)} is called.
	 *
	 * @param descriptor The descriptor which defines the extension of this interval.
	 * @throws IllegalArgumentException if the union can not be performed properly.
	 * @see #union(Interval_modded)
	 */
	public void union (Descriptor descriptor) throws IllegalArgumentException {
		union((Interval_modded)descriptor);
	}

	/**
	 * Shrinks this interval to reflect the intersection with a given interval.
	 * An intersection can only be computed if the interval overlaps with the given interval,
	 * therefore an <tt>IllegalArgumentException</tt> is thrown, if the intervals
	 * do not overlap.
	 * If the intervals overlap, this interval is shrinked as follows:
	 * <br><br>
	 * <code><pre>
	 * 	for (int i=0; i<2; i++) {
	 * 		int comparison = Math.signum(comparator.compare(border[i], interval.border[i]));
	 *
	 * 		if (comparison==0)
	 * 			inclusive[i] &= interval.inclusive[i];
	 * 		else if (comparison==i*2-1) {
	 * 			inclusive[i] = interval.inclusive[i];
	 * 			border[i] = interval.border[i];
	 * 		}
	 * 	}
	 * 	return this;
	 * </code></pre>
	 * If <code>comparsion == 0</code> the intervals have the same left (right) border and
	 * therefore only the inclusion of this interval's border have to be set.
	 * If this interval's left border (<tt>border[0]</tt>) is less than the given interval's
	 * left border (<tt>interval.border[0]</tt>), i.e. <code>comparsion == -1</code> the given
	 * interval's border properties are assumed by this interval.
	 * In the other case when this interval's right border (<tt>border[1]</tt>) is
	 * taller than the given interval's right border (<tt>interval.border[1]</tt>),
	 * i.e <code>comparsion == -1</code>, the given interval's borders are also
	 * assumed by this interval.
	 *
	 * @param interval The interval to be intersected with.
	 * @return This interval (shrinked).
	 * @throws IllegalArgumentException if the intersection can not be performed properly.
	 */
	public Interval_modded intersect (Interval_modded interval) throws IllegalArgumentException {
		if (overlaps(interval)!=0)
			throw new IllegalArgumentException("Intervals do not overlap");
		try {
			for (int i=0; i<2; i++) {
				int comparison = Maths.signum(comparator.compare(border[i], interval.border[i]));

				if (comparison==0)
					inclusive[i] &= interval.inclusive[i];
				else if (comparison==i*2-1) {
					inclusive[i] = interval.inclusive[i];
					border[i] = interval.border[i];
				}
			}
			return this;
		}
		catch (Exception e) {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Shrinks this interval to reflect the intersection with a given interval.
	 * An intersection can only be computed if the interval overlaps with the given interval,
	 * therefore an <tt>IllegalArgumentException</tt> is thrown, if the intervals
	 * do not overlap. <br>
	 * <b>Note:</b> The descriptor is casted to an Interval1D and
	 * the method {@link #intersect(Interval_modded)} is called.
	 *
	 * @param descriptor The descriptor to be intersected with.
	 * @return This descriptor (shrinked).
	 * @throws IllegalArgumentException if the intersection cannot be performed properly.
	 * @see #intersect(Interval_modded)
	 */
	public Descriptor intersect (Descriptor descriptor) throws IllegalArgumentException {
		return intersect((Interval_modded)descriptor);
	}
}
