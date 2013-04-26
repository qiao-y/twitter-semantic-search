package edu.columbia.watson.twitter;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterator;



public class QueryVectorization {
	private Map<String,Integer> dictionary = new HashMap<String,Integer>();
	
	public void loadDictionaryToMap(String fileName) throws IOException{
	  //  Path[] pathArr= null;
	    Configuration conf = new Configuration();
	    Path input = new Path("/mnt/corpus/dict/dictionary.file-0");
	    FileSystem fs = input.getFileSystem(conf);
	   Writer writer = new OutputStreamWriter(System.out);
	  
            SequenceFileIterator<?,?> iterator = new SequenceFileIterator<Writable, Writable>(input, true, conf);
	   // writer.append("Key class: ").append(iterator.getKeyClass().toString());
            //writer.append(" Value Class: ").append(iterator.getValueClass().toString()).append('\n');
 
            while (iterator.hasNext()) {
            	Pair<?,?> record = iterator.next();
            	String key = record.getFirst().toString();
            //	writer.append("Key: ").append(key);
            	String str = record.getSecond().toString();
            //	writer.append(": Value: ").append(str);
		dictionary.put(key,Integer.valueOf(str));
            //	writer.write('\n');

       	   }

        System.out.println(dictionary.size());
        
//	    if (fs.getFileStatus(input).isDir()) {
//	      pathArr = FileUtil.stat2Paths(fs.listStatus(input, new OutputFilesFilter()));
//	    } else {
//	      pathArr = new Path[1];
//	      pathArr[0] = input;
//	    }
//	    
	    
	    
//	    Writer writer = new OutputStreamWriter(System.out);;



	}
	
	public static void main(String args[]) throws IOException
	{
		QueryVectorization zizi = new QueryVectorization();
		zizi.loadDictionaryToMap("");
	}
	
}
