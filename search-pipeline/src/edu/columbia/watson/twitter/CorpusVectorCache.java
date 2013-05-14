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
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import edu.columbia.watson.twitter.util.GlobalProperty;

public class CorpusVectorCache {
	private static CorpusVectorCache instance = null;
	private static Logger logger = Logger.getLogger(CorpusVectorCache.class);
	private Connection conn = null;
		
	public static CorpusVectorCache getInstance() 
	{
		if (instance == null)
			instance = new CorpusVectorCache();
		return instance;
	}

	/*public Vector getVectorByTweetID(long tweetID)
	{
		return corpusVector.get(tweetID);
	}

	public Map<Long,Vector> getAllVectors()
	{
		return corpusVector;
	}*/

	public Map<Long,Vector> getCorpusVector(List<Long> tweetIDList) throws SQLException, IOException{
		
		Statement st = conn.createStatement();
		StringBuilder queryBuilder = new StringBuilder("SELECT * FROM tweetid_vec_mapping where tweetID in (");
		for (int i = 0 ; i < tweetIDList.size()-1 ; ++i){
			queryBuilder.append(tweetIDList.get(i));
			queryBuilder.append(",");
		}
		if (tweetIDList.size() >= 1)
			queryBuilder.append(tweetIDList.get(tweetIDList.size()-1));
		queryBuilder.append(")");
		String query = queryBuilder.toString();
		logger.info("Before executing query:" + query);
		ResultSet rs = st.executeQuery(query);
		Map<Long,Vector> result = new HashMap<Long,Vector>();
		while (rs.next()) {
			long tweetID = rs.getLong("tweetID");
			byte[] vecBytes = rs.getBytes("vector");
			ByteArrayInputStream byteIn = new ByteArrayInputStream(vecBytes);
			DataInputStream dataIn = new DataInputStream(byteIn);
			Vector v = VectorWritable.readVector(dataIn);
			result.put(tweetID,v);
		}
		logger.info("Before return from getCorpusVector, size = " + result.size());
		return result;
	}
	
	
	private CorpusVectorCache()
	{	
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


	}
}




