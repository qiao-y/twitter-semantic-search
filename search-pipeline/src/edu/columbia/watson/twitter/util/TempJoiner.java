package edu.columbia.watson.twitter.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
		String query = "INSERT INTO tweetid_vec_mapping (twitterID, vector) VALUES(?,?)";
		String query2 = "SELECT vector from rowid_vec_mapping where row_id = ";
		PreparedStatement pstmt = conn.prepareStatement(query);
		Statement st = conn.createStatement();
		
		for (Pair<IntWritable,Text> record :
			new SequenceFileDirIterable<IntWritable,Text>(new Path(fileName),
					PathType.LIST,
					PathFilters.logsCRCFilter(),
					null,
					true,
					conf)) {

			Integer rowID = record.getFirst().get();
			Long tweetID = Long.parseLong(record.getSecond().toString());
					
			ResultSet rs = st.executeQuery(query2 + rowID);
			byte[] vec = rs.getBytes("vector");
			
			pstmt.setLong(1, tweetID);
			pstmt.setBytes(2, vec);
			pstmt.execute();
			
			if (count++ % 10000 == 0){
				System.out.println(count + " done");
			}
			
			
			//System.out.println(rowID + "->" + tweetID);
		}
	}
}
