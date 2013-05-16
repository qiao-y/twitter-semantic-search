package edu.columbia.watson.twitter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.apache.mahout.math.Vector;
import org.w3c.dom.DOMException;
import org.xml.sax.SAXException;

import edu.columbia.watson.twitter.AnswerRanking.IDCosinePair;


/**
 * Entry point of the searching pipeline
 * @author qiaoyu
 *
 */

public class SearchMain {

	private static Logger logger = Logger.getLogger(SearchMain.class);

	private DocumentRetrieval documentFetcher = null;
	private QueryVectorization qv = null;
	private AnswerRanking ar = null;
	private QueryExpansion qe = null;
	
	public SearchMain() throws IOException{
		documentFetcher = new DocumentRetrieval();
		qv = new QueryVectorization();
		ar = new AnswerRanking();
		qe = new QueryExpansion();
	}
	
	
	public String normalize(String query){
		logger.info("before normalization, query = " + query);
		String [] split = query.split(" ");
		StringBuilder sb = new StringBuilder();
		for (String term : split){
			String temp = term.toLowerCase();
			boolean use = true;
			for (int i = 0 ; i < temp.length() ; ++i)
				if (!('a' <= temp.charAt(i) && temp.charAt(i) <= 'z')){
					use = false;
					break;
				}
			if (!use)
				continue;
			sb.append(temp);
			sb.append(" ");
		}
		logger.info("after normalization, query = " + sb.toString());
		return sb.toString();	
	}

	public List<ReadableResult> run(Long linkedID, String queryText) throws IOException, org.apache.lucene.queryparser.classic.ParseException, SQLException{
		
		QueryClause query = new QueryClause("Q01",queryText,linkedID,new Date());
		String linkedTweet = "";
		try {
			linkedTweet = documentFetcher.retrieveLinkedTweetByID(linkedID);
			if (linkedTweet.equals("")){
				logger.info("NOT FOUND: " + linkedID);
				linkedTweet = query.getQuery();
			}
		} catch (SQLException e) {
			logger.error("Error getting linked tweet, tweet id = " + linkedID);
			logger.error(e);
		}
		
		String expandedQuery = query.getQuery() + " " + normalize(linkedTweet);
		String doubleExpandedQuery = qe.expandQuery(expandedQuery);
		
		List<Long> relevantID = documentFetcher.retrieveAllRelevantTweetID(expandedQuery);
		logger.info("Before query: " + query.getQueryNumber() + " linked tweet = " + linkedTweet);
		Vector queryVector = qv.getLSAQueryVector(doubleExpandedQuery);
		List<IDCosinePair> answerList = ar.getTopKAnswer(queryVector, relevantID, doubleExpandedQuery, query.getLinkedTweetID());
		logger.info("After query: " + query.getQueryNumber() + " linked tweet = " + linkedTweet);
		List<ReadableResult> result = new ArrayList<ReadableResult>();
		for (IDCosinePair pair : answerList){
			result.add(new ReadableResult(documentFetcher.retrieveLinkedTweetByID(pair.getID()),pair.getID(),pair.getCosine().floatValue()));
		}
		return result;
	}
		
	
	public void run(String topicFileName, String outputFileName) throws DOMException, ParserConfigurationException, SAXException, IOException, ParseException, org.apache.lucene.queryparser.classic.ParseException, SQLException {
		logger.info("Initializing query parser class");
		List<QueryClause> queryList = QueryParser.getAllQueriesFromFile(topicFileName);
		logger.info("Initializing document retrieval class");
		DocumentRetrieval documentFetcher = new DocumentRetrieval();
		BufferedWriter out = new BufferedWriter(new FileWriter(outputFileName));
		
		for (QueryClause query : queryList){
			Long linkedID = query.getLinkedTweetID();
			String linkedTweet = "";
			
			try {
				linkedTweet = documentFetcher.retrieveLinkedTweetByID(linkedID);
				if (linkedTweet.equals("")){
					logger.info("NOT FOUND: " + linkedID);
					linkedTweet = query.getQuery();
				}
			} catch (SQLException e) {
				logger.error("Error getting linked tweet, tweet id = " + linkedID);
				logger.error(e);
			}
			
			String expandedQuery = query.getQuery() + " " + normalize(linkedTweet);
			String doubleExpandedQuery = qe.expandQuery(expandedQuery);
			
			List<Long> relevantID = documentFetcher.retrieveAllRelevantTweetID(expandedQuery);
			logger.info("Before query: " + query.getQueryNumber() + " linked tweet = " + linkedTweet);
			Vector queryVector = qv.getLSAQueryVector(doubleExpandedQuery);
			List<IDCosinePair> answerList = ar.getTopKAnswer(queryVector, relevantID, doubleExpandedQuery, query.getLinkedTweetID());
			logger.info("After query: " + query.getQueryNumber() + " linked tweet = " + linkedTweet);
			//List<TrecResult> result = new ArrayList<TrecResult>();
			int rank = 0;
			for (IDCosinePair pair : answerList){
				if (pair.getID() <= query.getLinkedTweetID()){
					TrecResult result = new TrecResult(query.getQueryNumber(), pair.getID(), rank++, pair.getCosine().floatValue(), "alphaRun");
					out.write(result.toString());
				}
			}
		}
		out.close();
	}

	public void runBaseline(String topicFileName, String outputFileName) throws DOMException, ParserConfigurationException, SAXException, IOException, ParseException, org.apache.lucene.queryparser.classic.ParseException{
		logger.info("Initializing query parser class");
		List<QueryClause> queryList = QueryParser.getAllQueriesFromFile(topicFileName);
		logger.info("Initializing document retrieval class");
		DocumentRetrieval luceneHelper = new DocumentRetrieval();
		BufferedWriter out = new BufferedWriter(new FileWriter(outputFileName));
		for (QueryClause query : queryList){
			Long linkedID = query.getLinkedTweetID();
			String linkedTweet = "";
			try {
				linkedTweet = luceneHelper.retrieveLinkedTweetByID(linkedID);
			} catch (SQLException e) {
				logger.error("Error getting linked tweet, tweet id = " + linkedID);
				logger.error(e);
			}
			logger.info("Before query: " + query.getQueryNumber());
			List<TrecResult> result = luceneHelper.retrieveAllRelevantDocuments(query.getQueryNumber(), query.getQuery() + " " + linkedTweet);
			logger.info("After query: " + query.getQueryNumber());
			for (TrecResult item : result){
				if (item.getTweetID() < query.getLinkedTweetID())
					//if less than tweetqueryid and earlier than query time
					out.write(item.toString());
			}
		}
		out.close();
	}


	public static void main(String [] args) throws DOMException, ParserConfigurationException, SAXException, IOException, ParseException, org.apache.lucene.queryparser.classic.ParseException, SQLException
	{
/*		if (args.length!= 2){
			System.err.println("Usage: run.sh edu.columbia.watson.twitter.SearchMain query_file output_file");
			return;
		}*/
		SearchMain driver = new SearchMain();

		System.out.println(driver.run(34952194402811904L,"BBC World Service staff cuts"));
		//driver.run(args[0],args[1]);
	}

}
