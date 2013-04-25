/**
 * @author Wei Wang
 * date: Apr 25, 2013
 */

package edu.columbia.watson.twitter.lsa.util;

import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import org.apache.mahout.utils.io.*;

/*
 * Similar to seqdirectory in Mahout, except that it
 * converts a large file into sequence files. 
 * Each line in the large file is a document.
 */
public class SequenceFileMaker { 

	public static void main(String[] args) throws IOException { 

		if (args.length != 3) {
			System.out.println("Usage: SequenceFileMaker <start_row> <input_path> <output_path>");
			return;
		}

		int startRow = Integer.parseInt(args[0]);
		int currentRow = 0;

		FileInputStream fin = new FileInputStream(args[1]);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fin));

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(args[2]), conf);
		Path path = new Path(args[2]);

    ChunkedWriter writer = new ChunkedWriter(conf, 100, path);

		String line = null;
		String tweetId = null;
		String userId = null;
		String time = null;
		String hashtag = null;
		String location = null;
		String url = null;
		String tweet = null;

		try { 
			while ((line = reader.readLine()) != null) {
				if (line.trim().length() == 0) {
					currentRow++;
					continue;
				}

				if (currentRow < startRow) {
					currentRow++;
					continue;
				}

				int index = 0;
				int preIndex = 0;

				index = line.indexOf('\t', preIndex + 1);
				tweetId = line.substring(preIndex, index);
				//System.out.println("tweetId:" + tweetId);
				preIndex = index;
				index = line.indexOf('\t', preIndex + 1);
				userId = line.substring(preIndex, index);
				//System.out.println("userId:" + userId);
				preIndex = index;
				index = line.indexOf('\t', preIndex + 1);
				time = line.substring(preIndex, index);
				//System.out.println("time:" + time);
				preIndex = index;
				index = line.indexOf('\t', preIndex + 1);
				hashtag = line.substring(preIndex, index);
				//System.out.println("hashtag:" + hashtag);
				preIndex = index;
				index = line.indexOf('\t', preIndex + 1);
				location = line.substring(preIndex, index);
				//System.out.println("location:" + location);
				preIndex = index;
				index = line.indexOf('\t', preIndex + 1);
				url = line.substring(preIndex, index);
				//System.out.println("url:" + url);
				preIndex = index;
				tweet = line.substring(preIndex + 1);
				//System.out.println("tweet:" + tweet);
				writer.write(tweetId, tweet); 

				currentRow++;
			} 
		} catch (Exception e) {
			e.printStackTrace();
		} finally { 
			System.err.println("Before exit: Last row is " + currentRow);
			IOUtils.closeStream( writer);
			reader.close();
		} 
	} 
}
