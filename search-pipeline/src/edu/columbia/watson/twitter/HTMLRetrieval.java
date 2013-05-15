package edu.columbia.watson.twitter;

/**
 * With reference to: Apache Lucene Demo program
 */

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;


import edu.columbia.watson.twitter.util.GlobalProperty;

public class HTMLRetrieval {

	private static Logger logger = Logger.getLogger(HTMLRetrieval.class);
	private String indexDir = GlobalProperty.getInstance().getIndexDir();
	private IndexReader reader;
	private IndexSearcher searcher;
	private Analyzer analyzer;
	
	public HTMLRetrieval() throws IOException{
		try {
			reader = DirectoryReader.open(FSDirectory.open(new File(indexDir)));
		} catch (IOException e) {
			logger.error("Error creating index reader, index dir = " + indexDir);
			logger.error(e);
			throw e;
		} catch (InstantiationException e) {
			logger.error(e);
		} catch (IllegalAccessException e) {
			logger.error(e);
		} catch (ClassNotFoundException e) {
			logger.error(e);
		}
		searcher = new IndexSearcher(reader);
		analyzer = new StandardAnalyzer(Version.LUCENE_42);
	}


	public String retrieveLinkedTweetByID(long tweetID) throws SQLException{
		Statement st = conn.createStatement();
		String query = "select tweet from tweet_corpus where id  =" + String.valueOf(tweetID);
		ResultSet rs = st.executeQuery(query);
		logger.info("After executing " + query);
		while (rs.next()) {
			return rs.getString("tweet");
		} 
		return "";
	}


	/**
	 * First pass of search 
	 * @param tweetID
	 * @return Corresponding tweet
	 * @throws ParseException 
	 * @throws IOException 
	 */

	/*
	public String retrieveLinkedTweetByID(long tweetID) throws ParseException, IOException {
		//QueryParser parser = new QueryParser(Version.LUCENE_42, "tweetID", analyzer);
		//String queryString = String.valueOf(tweetID);
		//Query query = parser.parse(queryString);
		Query query = NumericRangeQuery.newLongRange("tweetID",1,tweetID, tweetID, true, true);	

		//XXX: test this value. should be 1
		TopDocs results = searcher.search(query, 1);
		ScoreDoc[] hits = results.scoreDocs;
		logger.info("hits = " + results.totalHits);

		for (ScoreDoc doc : hits){
			//System.out.println(searcher.doc(doc.doc).get("tweetID"));
			String tweet = searcher.doc(doc.doc).get("content");
			return tweet;
		}
		return "";
	}*/

	public List<Long> retrieveAllRelevantTweetID(String queryText) throws ParseException, IOException{
		QueryParser parser = new QueryParser(Version.LUCENE_42, "content", analyzer);
		Query query = parser.parse(queryText);

		TopDocs results = searcher.search(query, 3 * GlobalProperty.getInstance().getK());
		ScoreDoc[] hits = results.scoreDocs;

		//only return the tweet ID fields
		Set<Long> tweetIDSet = new HashSet<Long>(); //to remove duplicate tweet id
		for (ScoreDoc doc : hits){
			Long tweetID = Long.parseLong((searcher.doc(doc.doc).get("tweetID")));
			tweetIDSet.add(tweetID);
		}
		List<Long> result = new ArrayList<Long>();
		for (Long id : tweetIDSet)
			result.add(id);
		logger.info("Before return retrieveAllRelevantTweetID, size = " + result.size());
		return result;

	}
	

	/**
	 * Second pass of search
	 * @param queryLine
	 * @return
	 * @throws ParseException
	 * @throws IOException
	 */
	public List<TrecResult> retrieveAllRelevantDocuments(String topicNumber, String queryLine) throws ParseException, IOException{
		QueryParser parser = new QueryParser(Version.LUCENE_42, "content", analyzer);
		Query query = parser.parse(queryLine);

		TopDocs results = searcher.search(query, GlobalProperty.getInstance().getK());
		ScoreDoc[] hits = results.scoreDocs;
		List<TrecResult> result = new ArrayList<TrecResult>();

		//only return the tweet ID fields
		int count = 0;
		Set<Long> tweetIDSet = new HashSet<Long>(); //to remove duplicate tweet id
		for (ScoreDoc doc : hits){
			Long tweetID = Long.parseLong((searcher.doc(doc.doc).get("tweetID")));
			float score = doc.score / results.getMaxScore();
			if (!tweetIDSet.contains(tweetID)){
				tweetIDSet.add(tweetID);
				TrecResult newResult = new TrecResult(topicNumber,tweetID,count++,score,"alphaRun");
				result.add(newResult);
			}
		}

		return result;

	}

	public static void main(String[] args){

		HTMLRetrieval dr;
		try {
			dr = new HTMLRetrieval();
			System.out.println(dr.retrieveLinkedTweetByID(34952194402811904L));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}



	}



}
