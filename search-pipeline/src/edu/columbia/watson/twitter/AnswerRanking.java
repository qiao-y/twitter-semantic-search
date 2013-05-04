package edu.columbia.watson.twitter;
/**
 * @author qiaoyu
 */
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.mahout.math.Vector;

import edu.columbia.watson.twitter.util.GlobalProperty;



public class AnswerRanking {

	private static Logger logger = Logger.getLogger(AnswerRanking.class);

	/**
	 * get the most relevant K tweet IDs by calculating cosine value
	 * between the query and the corpus
	 * @return K most relevant tweets
	 */
	public static List<IDCosinePair> getTopKAnswer(Vector queryVector) {
		PriorityQueue<IDCosinePair> allCosValues = new PriorityQueue<IDCosinePair>();
		Set<Entry<Long, Vector>> corpusVectorEntrySet = CorpusVectorCache.getInstance().getAllVectors().entrySet();

		int count = 0;
		for (Entry<Long, Vector> entry : corpusVectorEntrySet){
			if (count++ % 10000 == 0)
				logger.info(count + "cosine values calculated");
			
			Vector corpusVector = entry.getValue();
			Double cosine = corpusVector.dot(corpusVector) / (Math.sqrt(corpusVector.getLengthSquared()) * Math.sqrt(queryVector.getLengthSquared()));
			
			IDCosinePair newPair = new IDCosinePair(entry.getKey(),cosine);
			if (allCosValues.size() < GlobalProperty.getInstance().getK()){		// use a min-heap to maintain the K largest cosine values
				allCosValues.add(newPair);
			}
			else{
				allCosValues.poll();
				allCosValues.add(newPair);
			}
		}

		List<IDCosinePair> result = new ArrayList<IDCosinePair>();
		while (allCosValues.size() > 0)
			result.add(allCosValues.poll());
		return result;
	}
	

	public static class IDCosinePair implements Comparable<IDCosinePair>
	{
		private long ID;
		private Double cosine;
		public IDCosinePair(long tweetID, Double cos)
		{
			ID = tweetID;
			cosine = cos;
		}
		
		/**
		 * opposite to normal comparison logic to implement max-heap
		 */
		@Override
		public int compareTo(IDCosinePair arg0) {
			return cosine.compareTo(arg0.cosine);
		}

		public long getID() {
			return ID;
		}
		
		public Double getCosine(){
			return cosine;
		}	
	}
	
}
