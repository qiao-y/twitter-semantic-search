package edu.columbia.watson.twitter;
/**
 * @author qiaoyu
 */
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.mahout.math.Vector;

import edu.columbia.watson.twitter.util.GlobalProperty;



public class AnswerRanking {

	private static Logger logger = Logger.getLogger(AnswerRanking.class);
	private HTMLRetrieval htmlScorer = null;
	
	public AnswerRanking() throws IOException
	{
		htmlScorer = new HTMLRetrieval();
	}
	
	
	/**
	 * get the most relevant K tweet IDs by calculating cosine value
	 * between the query and the corpus
	 * @return K most relevant tweets
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ParseException 
	 */
	public List<IDCosinePair> getTopKAnswer(Vector queryVector, List<Long> relevantTweetIDList, String query, Long queryTime) throws SQLException, IOException, ParseException {
		PriorityQueue<IDCosinePair> allCosValues = new PriorityQueue<IDCosinePair>();
		Set<Entry<Long, Vector>> corpusVectorEntrySet = CorpusVectorCache.getInstance().getCorpusVector(relevantTweetIDList).entrySet();
		Float maxScore = 10000000.0f;
		Map<Long,Float> htmlScoreMap = htmlScorer.getHTMLScores(query, maxScore);
		
		int count = 0;
		for (Entry<Long, Vector> entry : corpusVectorEntrySet){
			if (entry.getKey() > queryTime)
				continue;
			if (++count % 10000 == 0)
				logger.info(count + " cosine values calculated");

			Vector corpusVector = entry.getValue();
			
			//tweet score
			Double cosineScore = corpusVector.dot(queryVector) / (Math.sqrt(corpusVector.getLengthSquared()) * Math.sqrt(queryVector.getLengthSquared()));

			//url embedded in tweet score
			float htmlScore = 0.0f;
			if (htmlScoreMap.containsKey(entry.getKey())){
				htmlScore = htmlScoreMap.get(entry.getKey());
				logger.info("contains html, query = " + query + " , tweetid = " + entry.getKey() + ", score = " + htmlScore);
				
			}
			//htmlScorer.getLinkedHtmlScore(query, entry.getKey());
			
			float lambda = GlobalProperty.getInstance().getLambda();
			float delta = GlobalProperty.getInstance().getDelta();
			Double finalScore = lambda * cosineScore + (1 - lambda) * htmlScore * delta;
			
			IDCosinePair newPair = new IDCosinePair(entry.getKey(),finalScore);
			
			if (allCosValues.size() < GlobalProperty.getInstance().getK()){		// use a min-heap to maintain the K largest cosine values
				allCosValues.add(newPair);
			}
			else if (allCosValues.peek().getCosine() < newPair.getCosine()){
				allCosValues.poll();
				allCosValues.add(newPair);
			}
		}
		logger.info("allCosValues size = " + allCosValues.size());
		List<IDCosinePair> result = new ArrayList<IDCosinePair>();
		while (allCosValues.size() > 0)
			result.add(allCosValues.poll());
		Collections.reverse(result);		//sort in descending order
		logger.info(result.toString());
		return result;
	}


	public static class IDCosinePair implements Comparable<IDCosinePair>
	{
		private long ID;
		private Double cosine;
		public IDCosinePair(long tweetID, Double cos){
			ID = tweetID;
			cosine = cos;
		}

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

		@Override
		public String toString() {
			return "IDCosinePair [ID=" + ID + ", cosine=" + cosine + "]";
		}	

	}

}
