package xxl.core.util;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;

import xxl.core.collections.sweepAreas.ListSAImplementor;
import xxl.core.collections.sweepAreas.SortMergeEquiJoinSA;
import xxl.core.comparators.ComparableComparator;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.groupers.HashGrouper;
import xxl.core.cursors.joins.NestedLoopsJoin;
import xxl.core.cursors.joins.SortMergeEquivalenceJoin;
import xxl.core.cursors.joins.SortMergeJoin.Type;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.functions.FunJ8;
import xxl.core.functions.Tuplify;
import xxl.core.util.ListJoinOuter3way.JoinResult;
import xxl.core.xxql.IterableCursor;

import java.util.Arrays;

public class ListJoinOuter3way {
	
	public static class JoinResult<K> {
		public List<K> joined;
		public List<K> leftAnti;
		public List<K> rightAnti;
		
		public JoinResult(List<K> joined, List<K> leftAnti, List<K> rightAnti) {
			super();
			this.joined = joined;
			this.leftAnti = leftAnti;
			this.rightAnti = rightAnti;
		}
	}
	
	public static <K extends Comparable<K>> JoinResult<K> listJoinOuter3way(List<K> list1, List<K> list2) {
		//- compute the full join		
		Iterator<K> sortedInput0 = list1.iterator(); 
		Iterator<K> sortedInput1 = list2.iterator();
		SortMergeEquiJoinSA<K> sweepArea0 = new SortMergeEquiJoinSA<K>( new ListSAImplementor<K>(), 0, 2 );
		SortMergeEquiJoinSA<K> sweepArea1 = new SortMergeEquiJoinSA<K>( new ListSAImplementor<K>(), 1, 2 );
		ComparableComparator<K> comparator = new ComparableComparator<K>(); 
		xxl.core.functions.Function<Object, Object[]> newResult = Tuplify.DEFAULT_INSTANCE;
		Type type = Type.OUTER_JOIN;
		
		SortMergeEquivalenceJoin<K, Object[]> join = new SortMergeEquivalenceJoin<K, Object[]>(
				sortedInput0, 
				sortedInput1, 
				sweepArea0, 
				sweepArea1, 
				comparator, 
				newResult, 
				type);

		//- partition the tuples depending on where nulls occur
		Function<Object[], Integer> groupFun = new Function<Object[], Integer>() {
			@Override
			public Integer apply(Object[] t) {
				if(t[0] == null)
					return 2;
				else if(t[1] == null)
					return 1;
				else
					return 0;
			}
		};  

		List<Cursor<Object[]>> groups = Cursors.toList(new HashGrouper<Object[]>(join, FunJ8.toOld(groupFun)));

		//- simplify the result
		// for now simplify the joined pairs to single values (we don't have support for custom comparators anyway)
//		List<Pair<K,K>> joined = Cursors.toList(new Mapper<Object[], Pair<K,K>>( FunJ8.toOld( (Object[] t) -> new Pair<K,K>((K) t[0], (K) t[1]) ), groups.get(0) ) );
		List<K> joined = Cursors.toList(new Mapper<Object[], K>( FunJ8.toOld( (Object[] t) -> (K) t[0] ), groups.get(0) ) );
		List<K> leftAnti  = Cursors.toList(new Mapper<Object[], K>( FunJ8.toOld( (Object[] t) -> (K) t[0] ), groups.get(1) ) );
		List<K> rightAnti = Cursors.toList(new Mapper<Object[], K>( FunJ8.toOld( (Object[] t) -> (K) t[1] ), groups.get(2) ) );
		
//		return new Triple<List<Pair<K,K>>, List<K>, List<K>>(joined, leftAnti, rightAnti);
		return new JoinResult<K>(joined, leftAnti, rightAnti);
	}
	
	/** Quick test. */
	public static void main(String[] args) {
		List<Integer> list1 = Arrays.asList(2,5,7,10,12,18);
		List<Integer> list2 = Arrays.asList(1,2,4,5,7,10,18,19);

//		Triple<List<Pair<Integer, Integer>>, List<Integer>, List<Integer>> resTrip = listJoinOuter3way(list1, list2);
//		JoinResult<Integer> resTrip = listJoinOuter3way(list1, list2);
		JoinResult<Integer> resTrip = join3way(list1, list2);
				
		System.out.println("-- list 1:\n\t"+ list1);
		System.out.println("-- list 2:\n\t"+ list2);
		
//		System.out.println("-- joined:\n\t"+ resTrip.getElement1());
//		System.out.println("-- left anti:\n\t"+ resTrip.getElement2());
//		System.out.println("-- right anti:\n\t"+ resTrip.getElement3());
		
		System.out.println("-- joined:\n\t"+ resTrip.joined);
		System.out.println("-- left anti:\n\t"+ resTrip.leftAnti);
		System.out.println("-- right anti:\n\t"+ resTrip.rightAnti);
	}
	
	
	/** No need for K being Comparable. But its .equals is used. */
	public static <K> JoinResult<K> join3way(List<K> list1, List<K> list2) {
		BiPredicate<K,K> predNew = ( (x,y) -> x.equals(y) );
		// NestedLoopsJoin(Iterator<? extends I> input0, Cursor<? extends I> input1, , Function<? super I,? extends E> newResult, NestedLoopsJoin.Type type)
		NestedLoopsJoin<K, Object[]> join = new NestedLoopsJoin<K, Object[]>(
			 list1.iterator(), 						// Iterator<K> input0
			 new IterableCursor<K>(list2),			// Cursor<K> which needs to be resetable 
			 FunJ8.toOld(predNew), 					// Predicate<? super I> predicate
			 new Tuplify(), 						// Function<? super I,? extends E> newResult
			 NestedLoopsJoin.Type.OUTER_JOIN);		// NestedLoopsJoin.Type type
		 
		//- partition the tuples depending on where nulls occur
		return partitionOuterJoin(join);
	}
	
	/** Partitions the result of a full outer join into its 3 components: joined elements, left remainders, right remainders. */
	public static <K> JoinResult<K> partitionOuterJoin(Cursor<Object[]> tuples) {
		List<K> joined = new LinkedList<K>();
		List<K> leftAnti = new LinkedList<K>();
		List<K> rightAnti = new LinkedList<K>();
		
		while(tuples.hasNext()) {
			Object[] t = tuples.next();
			if(t[0] == null)
				rightAnti.add((K) t[1]);
			else if(t[1] == null)
				leftAnti.add((K) t[0]);
			else
				joined.add((K) t[0]);
		}
		
		return new JoinResult<K>(joined, leftAnti, rightAnti); 
	}
	
	
}
