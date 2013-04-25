package edu.columbia.watson.twitter;

/**
 * With reference to: Apache Lucene Demo program
 */
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

	private static final int MAX_RETURN_DOC_NUMBER = 160000000;
	private QueryClause queryLine;
		
	public DocumentRetrieval(QueryClause theQuery){
		queryLine = theQuery;
	}
	
	public QueryClause getQueryLine() {
		return queryLine;
	}

	public void setQueryLine(QueryClause queryLine) {
		this.queryLine = queryLine;
	}

	public List<Long> retrieveAllRelevantDocuments() throws ParseException, IOException{
		
		String indexDir = GlobalProperty.getInstance().getIndexDir();
		String queryString = queryLine.getQuery();
		
		IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(indexDir)));
		IndexSearcher searcher = new IndexSearcher(reader);
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
		QueryParser parser = new QueryParser(Version.LUCENE_42, "content", analyzer);
		Query query = parser.parse(queryString);
		
		TopDocs results = searcher.search(query, MAX_RETURN_DOC_NUMBER);
		ScoreDoc[] hits = results.scoreDocs;
		List<Long> result = new ArrayList<Long>();
		
		//only return the tweet ID fields
		//actully in addtion we still have the absolute path saved
		for (ScoreDoc doc : hits){
			Long tweetID = Long.parseLong((searcher.doc(doc.doc).get("tweetID")));
			result.add(tweetID);
		}
		
		return result;

	}
	
	
	public static void main(String [] args) throws ParseException, IOException
	{
		DocumentRetrieval test = new DocumentRetrieval(new QueryClause("hello"));
		List<Long> result = test.retrieveAllRelevantDocuments();
		for (Long zizi : result)
			System.out.println(zizi);
				
	}
	
	
}