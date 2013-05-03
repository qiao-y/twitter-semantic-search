package edu.columbia.watson.twitter;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import edu.columbia.watson.twitter.util.GlobalProperty;

public class CorpusVectorCache {
	private static CorpusVectorCache instance = null;
	private static Logger logger = Logger.getLogger(CorpusVectorCache.class);

	private Map<Long,Vector> corpusVector = new HashMap<Long,Vector>();
	private final static String query = "SELECT * FROM tweetid_vec_mapping";

	public static CorpusVectorCache getInstance() 
	{
		if (instance == null)
			instance = new CorpusVectorCache();
		return instance;
	}

	public Vector getVectorByTweetID(long tweetID)
	{
		return corpusVector.get(tweetID);
	}

	public Map<Long,Vector> getAllVectors()
	{
		return corpusVector;
	}

	private CorpusVectorCache()
	{	
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn = DriverManager.getConnection(GlobalProperty.getInstance().getMySqlConnectionString(),
					GlobalProperty.getInstance().getMySqlUserName(),GlobalProperty.getInstance().getMySqlPassword());
			logger.info("Connection established.");
		}  catch (InstantiationException e) {
			logger.error(e);
		} catch (IllegalAccessException e) {
			logger.error(e);
		} catch (ClassNotFoundException e) {
			logger.error(e);
		} catch (SQLException e) {
			logger.error(e);	
		}
		logger.info("conn = null ? " + (conn == null));

		int count = 0;
		try {
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
			logger.info("After executing query");
			while (rs.next()) {
				if (count++ % 10000 == 0)
					logger.info(count + "records received.");
				long tweetID = rs.getLong("tweetID");
				byte[] vecBytes = rs.getBytes("vector");
				ByteArrayInputStream byteIn = new ByteArrayInputStream(vecBytes);
				DataInputStream dataIn = new DataInputStream(byteIn);
				Vector v = VectorWritable.readVector(dataIn);
				corpusVector.put(tweetID,v);
			} 
		} catch (SQLException e) {
			logger.error(e);
			logger.error("Error getting result, count = " + count);
		} catch (IOException e) {
			logger.error(e);
			logger.error("Error converting to vector, count = " + count);
		} finally{
			try {
				conn.close();
			} catch (SQLException e) {
				logger.error(e);
			}
		}

		logger.info("Done loading vector, size = " + corpusVector.size());
	}
}




