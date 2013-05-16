package edu.columbia.watson.twitter;

/**
 * With reference to: Apache Lucene Demo program
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

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
	private String indexDir = GlobalProperty.getInstance().getHtmlIndexPath();
	private IndexReader reader;
	private IndexSearcher searcher;
	private Analyzer analyzer;
	private QueryParser parser;
	
	public HTMLRetrieval() throws IOException{
		try {
			reader = DirectoryReader.open(FSDirectory.open(new File(indexDir)));
		} catch (IOException e) {
			logger.error("Error creating index reader, index dir = " + indexDir);
			logger.error(e);
			throw e;
		}
		searcher = new IndexSearcher(reader);
		analyzer = new StandardAnalyzer(Version.LUCENE_42);
		parser = new QueryParser(Version.LUCENE_42, "contents", analyzer);
		logger.info("Done initializing HTMLRetrieval");
	}


	public void test() throws IOException, ParseException
	{
		BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
		String queryText = stdin.readLine();
		
		Query query = parser.parse(queryText);

		TopDocs results = searcher.search(query, 10);
		ScoreDoc[] hits = results.scoreDocs;
		logger.info("HTML Hit size: " + hits.length); 
		for (ScoreDoc doc : hits){
			String filePath = searcher.doc(doc.doc).get("path").trim();
			int startIndex = filePath.lastIndexOf('/');
			int endIndex = filePath.lastIndexOf('.');
			Long id = Long.valueOf(filePath.substring(startIndex + 1, endIndex).trim());
			logger.info("filepath = " + filePath + ", id = " + id + ", score = " + doc.score);
		}
	}

	public Map<Long,Float> getHTMLScores(String queryText, Float maxScore)throws ParseException, IOException{
		
		Query query = parser.parse(queryText);

		TopDocs results = searcher.search(query, 10000);
		ScoreDoc[] hits = results.scoreDocs;
 		logger.info("HTML Hit size: " + hits.length);
 		
		Map<Long,Float> result = new HashMap<Long,Float>();
 		maxScore = hits[0].score;
 		
		for (ScoreDoc doc : hits){
			String filePath = searcher.doc(doc.doc).get("path").trim();
			int startIndex = filePath.lastIndexOf('/');
			int endIndex = filePath.lastIndexOf('.');
			Long id = Long.valueOf(filePath.substring(startIndex + 1, endIndex).trim());
			result.put(id, doc.score);
		}
		return result;
	}


	public float getLinkedHtmlScore(String queryText, long tweetID) throws ParseException, IOException{
		QueryParser parser = new QueryParser(Version.LUCENE_42, "contents", analyzer);
		Query query = parser.parse(queryText);

		TopDocs results = searcher.search(query, 100 * GlobalProperty.getInstance().getK());
		ScoreDoc[] hits = results.scoreDocs;
		logger.info("HTML Hit size: " + hits.length); 
		for (ScoreDoc doc : hits){
			String filePath = searcher.doc(doc.doc).get("path").trim();
			int startIndex = filePath.lastIndexOf('/');
			int endIndex = filePath.lastIndexOf('.');
			Long id = Long.valueOf(filePath.substring(startIndex + 1, endIndex).trim());
			//logger.info("filepath = " + filePath + ", id = " + id + ", score = " + doc.score);
			if (id == tweetID){
				return doc.score / hits[0].score;
			}
		}
		return 0.0f;
	}

	public static void main(String[] args) throws IOException, ParseException{

		HTMLRetrieval dr = new HTMLRetrieval();
		dr.test();
	}



}
