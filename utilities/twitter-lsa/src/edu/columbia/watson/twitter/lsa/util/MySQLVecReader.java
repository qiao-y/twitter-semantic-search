/**
 * @author Wei Wang
 * date: Apr 25, 2013
 */

package edu.columbia.watson.twitter.lsa.util;

import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.Vector;

import java.util.List;
import java.util.Map;

import java.sql.*;

import java.io.*;

/*
 * A demo of reading vectors from database
 */
public class MySQLVecReader {

	public static void main(String[] args) throws Exception {

		if (args.length != 1) {
			System.out.println("Usage: MySQLVecReader <output_dir>");
			return;
		}

		Class.forName("com.mysql.jdbc.Driver").newInstance();
		Connection conn = null;

		conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/twitter-semantic-search", "root", "twitter-semantic-search");

		Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path resultPath = new Path(args[0], "vectors");
    SequenceFile.Writer resultWriter = SequenceFile.createWriter(fs,
                                                                conf,
                                                                resultPath,
																																IntWritable.class,
                                                                VectorWritable.class);

		try {

			String query = "SELECT * FROM rowid_vec_mapping";
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);

			while (rs.next()) {
				IntWritable rowId = new IntWritable(rs.getInt("row_id"));
				byte[] vecBytes = rs.getBytes("vector");
				ByteArrayInputStream byteIn = new ByteArrayInputStream(vecBytes);
				DataInputStream dataIn = new DataInputStream(byteIn);
				Vector v = VectorWritable.readVector(dataIn);
				VectorWritable vec = new VectorWritable(v);
				resultWriter.append(rowId, vec);
			}

		} finally {
      Closeables.closeQuietly(resultWriter);
			conn.close();
		}
	}

}
