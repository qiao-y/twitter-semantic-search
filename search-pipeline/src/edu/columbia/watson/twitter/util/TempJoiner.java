package edu.columbia.watson.twitter.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;

public class TempJoiner {
	static void main(String[] args){
		String fileName = "/mnt/corpus/rowid_tweetid/docIndex";
		Configuration conf = new Configuration();

		for (Pair<IntWritable,Text> record :
			new SequenceFileDirIterable<IntWritable,Text>(new Path(fileName),
					PathType.LIST,
					PathFilters.logsCRCFilter(),
					null,
					true,
					conf)) {

			Integer rowID = record.getFirst().get();
			Long tweetID = Long.parseLong(record.getSecond().toString());
			System.out.println(rowID + "->" + tweetID);
		}
	}
}
