package xxl.core.indexStructures;

import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import xxl.core.cursors.mappers.Mapper;
import xxl.core.functions.AbstractFunction;
import static xxl.core.functions.FunctionsJ8.*;

public class WBTree_ExtTree extends Tree {

	@Override
	public Node createNode(int level) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public class IndexEntry extends Tree.IndexEntry {
		
		public IndexEntry(int parentLevel) {
			super(parentLevel);	
		}

		/** Weight of the referenced child node. */
		int weight;		
		
		/** Separator (some kind of Descriptor) of childEntry. */
		Separator separator;		
		
	}
	
	public class Node extends Tree.Node {

		List<IndexEntry> entries; 
		
		@Override
		protected SplitInfo initialize(Object entry) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public int number() {
			return entries.size();
		}

		@Override
		public Iterator<IndexEntry> entries() {
			return entries.iterator();
		}

		@Override
		public Iterator descriptors(Descriptor nodeDescriptor) {
			// what is the heck is the parameter for? See: {@link Tree.Node.descriptors(Descriptor)}			
			return new Mapper<IndexEntry, Separator>( toOldFunction((IndexEntry entry) -> entry.separator) , entries());
		}

		/** Returns an Iterator pointing to entries whose descriptors overlap the queryDescriptor 
		 * @param queryDescriptor the descriptor describing the query
		 * @return an Iterator pointing to entries whose descriptors overlap the <tt>queryDescriptor</tt>
		*/
		@Override
		public Iterator query(Descriptor queryDescriptor) {
			// naive implementations which only relies on the functionality of Descriptor,
			// namely the test for overlap <tt>overlaps</tt>
			KeyRange query = (KeyRange) queryDescriptor;
			
			Iterator<IndexEntry> baseIter = entries();
			
			Iterator<IndexEntry> iter = new Iterator<IndexEntry>() {
				IndexEntry computedNext = null;
				@Override
				public boolean hasNext() {
					while(baseIter.hasNext()) {
						IndexEntry e = baseIter.next();
						if(queryDescriptor.overlaps(e.separator)) {
							
						}
					}
					// TODO Auto-generated method stub
					return false;
				}
				
				@Override
				public IndexEntry next() {
					// TODO Auto-generated method stub
					return null;
				}
			};
			
			return null;
		}

		@Override
		protected Tree.IndexEntry chooseSubtree(Descriptor descriptor, Stack path) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		protected void grow(Object data, Stack path) {
			// TODO Auto-generated method stub
			
		}

		@Override
		protected SplitInfo split(Stack path) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		protected void post(SplitInfo splitInfo, xxl.core.indexStructures.Tree.IndexEntry newIndexEntry) {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	

}
