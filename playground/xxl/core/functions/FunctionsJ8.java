package xxl.core.functions;

import xxl.core.functions.*;

import java.util.function.*;

public class FunctionsJ8 {

	public static <P, R> Function<P, R> toOldFunction(final java.util.function.Supplier<R> nf) {
		return new AbstractFunction<P, R>() {
			@Override
			public R invoke() {
				return nf.get();
			}
		};
	}
	
	public static <P, R> Function<P, R> toOldFunction(final java.util.function.Function<P,R> nf) {
		return new AbstractFunction<P, R>() {
			@Override
			public R invoke(P a1) {
				return nf.apply(a1);
			}
		};
	}
	
	public static <P, R> Function<P, R> toOldFunction(final java.util.function.BiFunction<P,P,R> nf) {
		return new AbstractFunction<P, R>() {
			@Override
			public R invoke(P a1, P a2) {
				return nf.apply(a1, a2);
			}
		};
	} 
}
