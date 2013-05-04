package edu.columbia.watson.twitter;

/**
 * With reference to: Apache Lucene Demo program
 */
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

public class DocumentRetrieval {

	//private static final int MAX_RETURN_DOC_NUMBER = 160000000;
	private static Logger logger = Logger.getLogger(DocumentRetrieval.class);
	private String indexDir = GlobalProperty.getInstance().getIndexDir();
	private IndexReader reader;
	private IndexSearcher searcher;
	private Analyzer analyzer;
	
	
	public DocumentRetrieval() throws IOException{
		try {
			reader = DirectoryReader.open(FSDirectory.open(new File(indexDir)));
		} catch (IOException e) {
			logger.error("Error creating index reader, index dir = " + indexDir);
			logger.error(e);
			throw e;
		}
		searcher = new IndexSearcher(reader);
		analyzer = new StandardAnalyzer(Version.LUCENE_42);
	}

	/**
	 * First pass of search 
	 * @param tweetID
	 * @return Corresponding tweet
	 * @throws ParseException 
	 * @throws IOException 
	 */
	public List<Long> retrieveLinkedTweetByID(long tweetID) throws ParseException, IOException {
		QueryParser parser = new QueryParser(Version.LUCENE_42, "tweetID", analyzer);
		String queryString = String.valueOf(tweetID);
		Query query = parser.parse(queryString);

		//XXX: test this value. should be 1
		TopDocs results = searcher.search(query, 10);
		ScoreDoc[] hits = results.scoreDocs;
		List<Long> result = new ArrayList<Long>();

		//only return the tweet ID fields
		//actually in addition we still have the absolute path saved
		for (ScoreDoc doc : hits){
			Long retrievedTweetID = Long.parseLong((searcher.doc(doc.doc).get("tweetID")));
			result.add(retrievedTweetID);
		}
		return result;
	}
	
	
	/**
	 * Second pass of search
	 * @param queryLine
	 * @return
	 * @throws ParseException
	 * @throws IOException
	 */
	public List<Long> retrieveAllRelevantDocuments(QueryClause queryLine) throws ParseException, IOException{
		QueryParser parser = new QueryParser(Version.LUCENE_42, "content", analyzer);
		String queryString = queryLine.getQuery();
		Query query = parser.parse(queryString);

		TopDocs results = searcher.search(query, GlobalProperty.getInstance().getK());
		ScoreDoc[] hits = results.scoreDocs;
		List<Long> result = new ArrayList<Long>();

		//only return the tweet ID fields
		//actually in addition we still have the absolute path saved
		for (ScoreDoc doc : hits){
			Long tweetID = Long.parseLong((searcher.doc(doc.doc).get("tweetID")));
			result.add(tweetID);
		}

		return result;

	}


}
