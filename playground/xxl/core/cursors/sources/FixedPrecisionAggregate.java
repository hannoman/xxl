package xxl.core.cursors.sources;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.math.statistics.parametric.aggregates.ConfidenceAggregationFunction;
import xxl.core.util.Pair;

public class FixedPrecisionAggregate extends AbstractCursor<Pair<Number, Double>> {
	final ConfidenceAggregationFunction coAggFun;
	final double targetPrecision; // = Unprecision
	final Cursor<? extends Number> srcCursor;
	
	private Number agg;
	private Double error;
	private Pair<Number, Double> nextYield;
	
	public FixedPrecisionAggregate(ConfidenceAggregationFunction coAggFun, double targetPrecision, Cursor<? extends Number> srcCursor) {
		super();
		this.coAggFun = coAggFun;
		this.targetPrecision = targetPrecision;
		this.srcCursor = srcCursor;
		
		agg = null;
		error = 1.0;
		nextYield = null;
	}

	@Override
	protected boolean hasNextObject() {
		if(srcCursor.hasNext() && error > targetPrecision) { 
			Number val = srcCursor.next();
			agg = coAggFun.invoke(agg, val);
			error = (Double) coAggFun.epsilon(); // hopefully coAggFun.epsilon() returns something castable to Double
			nextYield = new Pair<Number, Double>(agg, error);
			return true;
		}
		else
			return false;
	}

	@Override
	protected Pair<Number, Double> nextObject() {
		return nextYield;
	}
	
	
	
	
	
}
