package edu.columbia.watson.twitter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.hadoop.stochasticsvd.SSVDSolver;

/**
 * Convert a query into vector 
 * TODO: Change to built-in tokenizer
 * @author qiaoyu
 */

public class QueryVectorization {
	private static Logger logger = Logger.getLogger(DocumentRetrieval.class);
	
	public static Vector getSparseVectorFromString(String query){
		/**
		 * a very ugly way now - split by space
		 */
		String [] splitted = query.split(" ");
		Map<Integer,Integer> termFrequencyCount = new HashMap<Integer,Integer>();	//wordID -> frequency
		for (String term : splitted){
			int wordID = DictionaryCache.getInstance().getWordID(term);
			if (termFrequencyCount.containsKey(wordID))
				termFrequencyCount.put(wordID, termFrequencyCount.get(wordID) + 1);
			else
				termFrequencyCount.put(wordID, 1);
		}	

		Vector result = new RandomAccessSparseVector(DictionaryCache.getInstance().getDicSize());

		for (Map.Entry<Integer, Integer> entry : termFrequencyCount.entrySet())
			result.set(entry.getKey(), entry.getValue());

		return result;
	}

	/** do stochastic svd **/
//	public static Vector doSSVD(Vector originalVector){
//		Configuration conf = new Configuration();
//		if (conf == null) {
//			logger.error("Cannot initialize Hadoop Configuration");
//			throw new IOException("No Hadoop configuration present");
//		}
//
//		SSVDSolver solver = new SSVDSolver(conf, inputPaths, getTempPath(), r, k, p, reduceTasks);
//
//		solver.setMinSplitSize(minSplitSize);
//		solver.setComputeU(computeU);
//		solver.setComputeV(computeV);
//		solver.setcUHalfSigma(cUHalfSigma);
//		solver.setcVHalfSigma(cVHalfSigma);
//		solver.setOuterBlockHeight(h);
//		solver.setAbtBlockHeight(abh);
//		solver.setQ(q);
//		solver.setBroadcast(broadcast);
//		solver.setOverwrite(overwrite);
//		solver.setPcaMeanPath(xiPath);
//	}


	public static void main(String args[]) throws IOException
	{
		Vector sparseVector = QueryVectorization.getSparseVectorFromString("hello world");
		//		QueryVectorization zizi = new QueryVectorization();
		//	zizi.loadDictionaryToMap("/mnt/corpus/dict/dictionary.file-0");
	}

}
