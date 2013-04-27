package edu.columbia.watson.twitter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterator;



public class QueryVectorization {
	private Map<String,Integer> dictionary = new HashMap<String,Integer>();

	public void loadDictionaryToMap(String fileName) throws IOException{
		Configuration conf = new Configuration();
		Path input = new Path(fileName);

		SequenceFileIterator<Writable, IntWritable> iterator = new SequenceFileIterator<Writable, IntWritable>(input, true, conf);

		while (iterator.hasNext()) {
			Pair<Writable,IntWritable> record = iterator.next();
			String key = record.getFirst().toString();
			Integer id = record.getSecond().get();
			dictionary.put(key,id);
		}
	}
	
	public static void main(String args[]) throws IOException
	{
		QueryVectorization zizi = new QueryVectorization();
		zizi.loadDictionaryToMap("/mnt/corpus/dict/dictionary.file-0");
	}

}
