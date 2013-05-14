package edu.columbia.watson.twitter;

import java.io.IOException;
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
	
	/*private static Matrix transoposeVector(Vector vec) {
		SparseMatrix matrix = new SparseMatrix(vec.size(), 1);
		matrix.assignColumn(0, vec);
		return matrix;
	}*/

	// returns path of generated vector on disk
	private static Vector getVectorFromString(String query){
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

	/** do stochastic svd **/
	public static Vector getLSAQueryVector(String query){
		Vector vectorQ = getVectorFromString(query);
		Vector vectorQPrime = getReducedQueryVec(vectorQ);
		return vectorQPrime;
	}
	
	private static Vector getReducedQueryVec(Vector vectorQ) {
		
		Configuration conf = new Configuration();
		
		Path sigmaIMultUTPath = new Path(GlobalProperty.getInstance().getSigmaIMultUTPath());
		
		RandomAccessSparseVector vectorQPrime = new RandomAccessSparseVector(GlobalProperty.getInstance().getK());
		
		int count = 0;
		for (Pair<IntWritable,VectorWritable> record :
				new SequenceFileDirIterable<IntWritable,VectorWritable>(sigmaIMultUTPath,
					PathType.LIST,
					PathFilters.logsCRCFilter(),
					null,
					true,
					conf)) {
			logger.info("Iterating IMultU matrix row = " + count);
			count++;
			double sum = 0.0;
			Vector vec = record.getSecond().get();
			Iterator<Vector.Element> itr = vectorQ.iterateNonZero();
			while (itr.hasNext()) {
				Vector.Element ele = itr.next();
				sum += ele.get() * vec.get(ele.index());
			}
			vectorQPrime.set(record.getFirst().get(), sum);
		}
		
		return vectorQPrime;
	}
	
	public static void main(String args[]) throws IOException
	{
		Vector sparseVector = QueryVectorization.getLSAQueryVector("hello world");
		System.out.println(sparseVector.asFormatString());
	}

}
