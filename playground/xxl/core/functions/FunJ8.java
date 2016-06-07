package xxl.core.functions;

import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;

/** Conversions between old XXL-style functions and Java 8's new functional capabilities. */
public class FunJ8 {

	/** Convert Java 8 function to XXL-old-style function. */
	public static <P, R> Function<P, R> toOld(final java.util.function.Supplier<R> nf) {
		return new AbstractFunction<P, R>() {
			@Override
			public R invoke() {
				return nf.get();
			}
		};
	}
	
	/** Convert Java 8 function to XXL-old-style function. */
	public static <P, R> Function<P, R> toOld(final java.util.function.Function<P,R> nf) {
		return new AbstractFunction<P, R>() {
			@Override
			public R invoke(P a1) {
				return nf.apply(a1);
			}
		};
	}
	
	/** Convert Java 8 function to XXL-old-style function. */
	public static <P, R> Function<P, R> toOld(final java.util.function.BiFunction<P,P,R> nf) {
		return new AbstractFunction<P, R>() {
			@Override
			public R invoke(P a1, P a2) {
				return nf.apply(a1, a2);
			}
		};
	} 
	
	/** Convert Java 8 BiPredicate<U,U> to XXL-old-style Predicate<U>. */
	public static <P> Predicate<P> toOld(final java.util.function.BiPredicate<P,P> nf) {
		return new AbstractPredicate<P>() {
			@Override
			public boolean invoke(P a1, P a2) {
				return nf.test(a1, a2);
			}
		};
	}
	
}
