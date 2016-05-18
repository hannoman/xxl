package xxl.core.cursors.sources;

import java.util.Random;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.math.statistics.parametric.aggregates.ConfidenceAggregationFunction;

/** Own implementation of a cursor which computes a running average estimation over an (infinite) sample stream
 * with computation of confidence intervals.
 * This exists very probably already inside XXL but I can't make sense of it yet. 
 */
// TODO: we are using Doubles here because Java can't do any arithmetic on Number, can't this be done prettier? 
public class AverageEstimator extends AbstractCursor<Double> {
	
	// determining config
	final Cursor<Double> sampleSrc;
	// config
	final double targetConfidence;
	// state
	private int nSeen;
	private double summed;
	private double squaresSummed;
	
	
	public AverageEstimator(double targetConfidence, Cursor<Double> sampleSrc) {
		super();
		this.sampleSrc = sampleSrc;
		this.targetConfidence = targetConfidence;		
		summed = 0.0;
		squaresSummed = 0.0;
		nSeen = 0;
	}

	@Override
	protected boolean hasNextObject() {
		if(!sampleSrc.hasNext())
			return false;
		
		double v = sampleSrc.next();
		nSeen++;
		summed += v;
		squaresSummed += v*v;
		
		return true;
	}

	@Override
	protected Double nextObject() {		
		return summed / nSeen;
	}
	

}
