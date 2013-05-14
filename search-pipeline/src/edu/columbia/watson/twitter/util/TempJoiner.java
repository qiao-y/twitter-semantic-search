package edu.columbia.watson.twitter.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;

public class TempJoiner {
	
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException{
		
		String fileName = "/mnt/corpus/rowid_tweetid/docIndex";
		Configuration conf = new Configuration();
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		Connection conn = DriverManager.getConnection(GlobalProperty.getInstance().getMySqlConnectionString(),
				GlobalProperty.getInstance().getMySqlUserName(),GlobalProperty.getInstance().getMySqlPassword());
		int count = 0;
		String query = "INSERT INTO twitterid_rowid_mapping (twitter_id, row_id) VALUES(?,?)";
		PreparedStatement pstmt = conn.prepareStatement(query);
			
		for (Pair<IntWritable,Text> record :
			new SequenceFileDirIterable<IntWritable,Text>(new Path(fileName),
					PathType.LIST,
					PathFilters.logsCRCFilter(),
					null,
					true,
					conf)) {

			Integer rowID = record.getFirst().get();
			Long tweetID = Long.parseLong(record.getSecond().toString());
			pstmt.setLong(1, tweetID);
			pstmt.setInt(2, rowID);
			pstmt.execute();
			
			if (count++ % 10000 == 0){
				System.out.println(count + " done");
			}
			
			
			//System.out.println(rowID + "->" + tweetID);
		}
	}
}
