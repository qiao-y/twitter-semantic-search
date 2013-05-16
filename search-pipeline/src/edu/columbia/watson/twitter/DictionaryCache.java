package edu.columbia.watson.twitter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterator;

import edu.columbia.watson.twitter.util.GlobalProperty;

/**
 * Dictionary cache used when creating the vector of a query
 * @author qiaoyu
 */

public class DictionaryCache {
	private static DictionaryCache instance = null;
	private static Logger logger = Logger.getLogger(DictionaryCache.class);
	private Map<String,Integer> dictionary = new HashMap<String,Integer>();
	private Map<Integer,Long> docFreqMap = new HashMap<Integer,Long>();

	public static DictionaryCache getInstance() 
	{
		if (instance == null)
			instance = new DictionaryCache();
		return instance;
	}

	public Integer getWordID(String word)
	{
		return dictionary.get(word);
	}

	public Long getWordDocFrequency(int id)
	{
		return docFreqMap.get(id);
	}

	public int getDicSize()
	{
		return dictionary.size();
	}

	private DictionaryCache() 
	{
		loadDictionaryToMap(GlobalProperty.getInstance().getDicPath());
		loadDocFrequency(GlobalProperty.getInstance().getDocFreqPath());
	}

	private void loadDictionaryToMap(String fileName)
	{
		Configuration conf = new Configuration();
		Path input = new Path(fileName);

		SequenceFileIterator<Writable, IntWritable> iterator;
		try {
			iterator = new SequenceFileIterator<Writable, IntWritable>(input, true, conf);
			while (iterator.hasNext()) {
				Pair<Writable,IntWritable> record = iterator.next();
				String key = record.getFirst().toString();
				Integer id = record.getSecond().get();
				dictionary.put(key,id);
			}
			logger.info("Successfully loaded dictionary map, size = " + dictionary.size());
		} catch (IOException e) {
			logger.error("Error loading sequence file: " + fileName);
			e.printStackTrace();
		}
	}

	private void loadDocFrequency(String fileName)
	{
		Configuration conf = new Configuration();
		//Path input = new Path(fileName);

		for (Pair<IntWritable,LongWritable> record :
			new SequenceFileDirIterable<IntWritable,LongWritable>(new Path(fileName),
					PathType.LIST,
					PathFilters.logsCRCFilter(),
					null,
					true,
					conf)) {

			Integer termId = record.getFirst().get();
			Long docFreq = record.getSecond().get();
			docFreqMap.put(termId,docFreq);
		}
		logger.info("Successfully loaded document frequency map, size = " + docFreqMap.size());
	}

	public static void main(String args[]) throws IOException
	{
		int id = DictionaryCache.getInstance().getWordID("hello");
		System.out.println(id);
		System.out.println(DictionaryCache.getInstance().getWordDocFrequency(id));

		//	System.out.println(DictionaryCache.getInstance().getWordDocFrequency(DictionaryCache.getInstance().getWordID("hello")));
	}

}
