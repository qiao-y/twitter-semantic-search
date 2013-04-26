/**
 * @author Wei Wang
 * date: Apr 25, 2013
 */

package edu.columbia.watson.twitter.lsa.util;

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

import java.util.List;
import java.util.Map;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.io.*;

/*
 * Persist vectors in given file into database.
 */
public class MySQLVecPersister {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.println("Usage: MySQLVecPersister <row_start> <input_file>");
			return;
		}

		int rowStart = Integer.parseInt(args[0]);

		Class.forName("com.mysql.jdbc.Driver").newInstance();
		Connection conn = null;

		conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/twitter-semantic-search", "root", "twitter-semantic-search");

		int rowId = 0;
		VectorWritable vec = null;

		try {
			String query = "INSERT INTO rowid_vec_mapping (row_id, vector) VALUES(?,?)";
			PreparedStatement pstmt = conn.prepareStatement(query);

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);

			for (Pair<IntWritable,VectorWritable> record :
					new SequenceFileDirIterable<IntWritable,VectorWritable>(new Path(args[1]),
						PathType.LIST,
						PathFilters.logsCRCFilter(),
						null,
						true,
						conf)) {
				rowId = record.getFirst().get();
				if (rowId < rowStart) {
					continue;
				}
				vec = record.getSecond();
				ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
				DataOutputStream dataOut = new DataOutputStream(byteOut);
				vec.write(dataOut);
				pstmt.setInt(1, rowId);
				pstmt.setBytes(2, byteOut.toByteArray());
				pstmt.execute();
					}
		} finally {
			System.err.println("Before exit: Last row id is " + rowId);
			conn.close();
		}
	}
}
