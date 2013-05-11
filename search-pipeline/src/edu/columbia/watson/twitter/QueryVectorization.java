package edu.columbia.watson.twitter;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterator;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixUtils;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SparseColumnMatrix;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.stochasticsvd.SSVDSolver;

import edu.columbia.watson.twitter.util.GlobalProperty;

/**
 * Convert a query into vector 
 * TODO: Change to built-in tokenizer
 * @author qiaoyu
 */

public class QueryVectorization {
	private static Logger logger = Logger.getLogger(DocumentRetrieval.class);
	
	private static String writeVecTransposed(Vector vec) {
		String pathStr = GlobalProperty.getInstance().getTempDir() + "twitter-semantic-search/query-ssvd/transposed";
		Path path = new Path(pathStr);
		int termNum = DictionaryCache.getInstance().getDicSize();
		Configuration conf = new Configuration();
		if (conf == null) {
			logger.error("Cannot initialize Hadoop Configuration");
			//throw new IOException("No Hadoop configuration present");
			return null;
		}
		
		try {
			MatrixUtils.write(path, conf, matrix);
		} catch (IOException e) {
			logger.error("Cannot write transposed query matrix to " + pathStr);
			e.printStackTrace();
			return null;
		}
		return pathStr;
	}
	
	private static Matrix transoposeVector(Vector vec) {
		SparseMatrix matrix = new SparseMatrix(vec.size(), 1);
		matrix.assignColumn(0, vec);
		return matrix;
	}

	// returns path of generated vector on disk
	private static Vector getVectorFromString(String query){
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

		for (Map.Entry<Integer, Integer> entry : termFrequencyCount.entrySet()) {
			double idf = 
					GlobalProperty.getInstance().getDocNum()/DictionaryCache.getInstance().getWordDocFrequency(entry.getKey());
			result.set(entry.getKey(), entry.getValue()*Math.log(idf));
		}
		
		return result;
	}

	/** do stochastic svd **/
	public static Vector getLSAQueryVector(String query){
		
		Configuration conf = new Configuration();
		if (conf == null) {
			logger.error("Cannot initialize Hadoop Configuration");
			//throw new IOException("No Hadoop configuration present");
			return null;
		}
		
		Path sigmaIMultUTPath = new Path(GlobalProperty.getInstance().getSigmaIMultUTPath());
		
		Vector vectorQ = getVectorFromString(query);
		Matrix vectorQT = transoposeVector(vectorQ);
		
		Matrix vectorSigIUT = null;
		try {
			vectorSigIUT = MatrixUtils.read(conf, sigmaIMultUTPath);
		} catch (IOException e) {
			logger.error("Cannot read SigmaI_Mult_UT matrix from " + sigmaIMultUTPath);
			e.printStackTrace();
			return null;
		}
		
		Matrix vectorQPrime = vectorSigIUT.times(vectorQT);
		Vector vectorQPrimeT = vectorQPrime.viewColumn(0);

		return vectorQPrimeT;
	}

	private static String writeTmpVector(Vector vec) {
		String pathStr = GlobalProperty.getInstance().getTempDir() + "twitter-semantic-search/query-ssvd/input";
		Configuration conf = new Configuration();
		Path path = new Path(pathStr);
		IntWritable key = new IntWritable(0);
		VectorWritable value = new VectorWritable(vec);
		SequenceFile.Writer writer = null;
		try { 
			FileSystem fs = FileSystem.get(URI.create(pathStr), conf);
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
			writer.append(key, value);
		} catch (IOException e) {
			logger.error("Error writing sequence file: " + pathStr);
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(writer); 
		} 
		return pathStr;
	}
	
	private static Vector readSingleVector(String pathStr) {
		Configuration conf = new Configuration();
		Path path = new Path(pathStr);
		SequenceFileIterator<IntWritable, VectorWritable> iterator;
		Vector vec = null;
		try {
			iterator = new SequenceFileIterator<IntWritable, VectorWritable>(path, true, conf);
			while (iterator.hasNext()) {
				Pair<IntWritable, VectorWritable> record = iterator.next();
				vec = record.getSecond().get();
				break;
			}
			logger.info("Successfully read vector U: " + pathStr);
		} catch (IOException e) {
			logger.error("Error reading vector U: " + pathStr);
			e.printStackTrace();
		}
		return vec;
	}

	public static void main(String args[]) throws IOException
	{
		Vector sparseVector = QueryVectorization.getLSAQueryVector("hello world");
		//		QueryVectorization zizi = new QueryVectorization();
		//	zizi.loadDictionaryToMap("/mnt/corpus/dict/dictionary.file-0");
	}

}
