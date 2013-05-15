package edu.columbia.watson.twitter;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import edu.columbia.watson.twitter.util.GlobalProperty;

/**
 * Convert a query into vector 
 * TODO: Change to built-in tokenizer
 * @author qiaoyu
 */

public class QueryVectorization {
	private static Logger logger = Logger.getLogger(QueryVectorization.class);
	private Map<Integer,Vector> sigmaIMultUTCache = new HashMap<Integer,Vector>();
	
	public QueryVectorization() {
		loadVectorCache(); 
	}
	
	
	private void loadVectorCache(){
		Configuration conf = new Configuration();
		Path sigmaIMultUTPath = new Path(GlobalProperty.getInstance().getSigmaIMultUTPath());
		
		logger.info("Before loadVectorCache, sigmaIMultUTPath = " + sigmaIMultUTPath.toString());	
	
		int count = 0;
		SequenceFileDirIterable<IntWritable,VectorWritable> seqFileDir = 
				new SequenceFileDirIterable<IntWritable,VectorWritable>(sigmaIMultUTPath, PathType.LIST, PathFilters.logsCRCFilter(), null, true, conf);
		for (Pair<IntWritable,VectorWritable> record : seqFileDir) {
			logger.info("Loading IMultU matrix row = " + count++);
			Integer first = record.getFirst().get();
			Vector vec = record.getSecond().get();
			sigmaIMultUTCache.put(first, vec);
		}
		logger.info("After loadVectorCache, size = " + sigmaIMultUTCache.size());
	}
	
	/** do stochastic svd **/
	public Vector getLSAQueryVector(String query){
		Vector vectorQ = getVectorFromString(query);
		Vector vectorQPrime = getReducedQueryVec(vectorQ);
		return vectorQPrime;
	}
	
	
	// returns path of generated vector on disk
	private Vector getVectorFromString(String query){
		/**
		 * a very ugly way now - split by space
		 */
		String [] splitted = query.split(" ");
		Map<Integer,Integer> termFrequencyCount = new HashMap<Integer,Integer>();	//wordID -> frequency
		for (String term : splitted){
			if (term == null || term.equals(""))
				continue;
			String termPrime = term.toLowerCase();
			logger.info("Processing term: " + termPrime);
			Integer wordID = DictionaryCache.getInstance().getWordID(termPrime);
			if (wordID == null)
				continue;		
			//TODO: if every term returns null, we need fall back to Lucene-based search
			if (termFrequencyCount.containsKey(wordID))
				termFrequencyCount.put(wordID, termFrequencyCount.get(wordID) + 1);
			else
				termFrequencyCount.put(wordID, 1);
		}	

		Vector result = new RandomAccessSparseVector(DictionaryCache.getInstance().getDicSize());

		for (Map.Entry<Integer, Integer> entry : termFrequencyCount.entrySet()) {
			double idf = 
					GlobalProperty.getInstance().getDocNum()/DictionaryCache.getInstance().getWordDocFrequency(entry.getKey());
			result.set(entry.getKey(), entry.getValue()*Math.log(idf));
		}
		
		return result;
	}


	private Vector getReducedQueryVec(Vector vectorQ) {
		
		RandomAccessSparseVector vectorQPrime = new RandomAccessSparseVector(GlobalProperty.getInstance().getRank());
		
		int count = 0;
		for (Map.Entry<Integer, Vector> entry : sigmaIMultUTCache.entrySet()) {
			logger.info("Iterating IMultU matrix row = " + count);
			count++;
			double sum = 0.0;
			Vector vec = entry.getValue();
			Iterator<Vector.Element> itr = vectorQ.iterateNonZero();
			while (itr.hasNext()) {
				Vector.Element ele = itr.next();
				sum += ele.get() * vec.get(ele.index());
			}
			vectorQPrime.set(entry.getKey(), sum);
		}
		return vectorQPrime;
	}
	
	/*
	public static void main(String args[]) throws IOException
	{
		Vector sparseVector = QueryVectorization.getLSAQueryVector("hello world");
		System.out.println(sparseVector.asFormatString());
	}
	*/

}
