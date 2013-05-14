/**
 * Use to build lucene index. One document for each each tweet. Stores the tweet id as a long field.
 * With reference to Lucene demo program
 * XXX: need to use log4j for logging
 */

package edu.columbia.watson.twitter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class LuceneIndexBuilder {
	private static final String docsPath = "/mnt/corpus/post_process_clean";
	private static final String indexPath = "/mnt/corpus/index";

	public static void main(String args[]) throws IOException{
		final File docDir = new File(docsPath);
		if (!docDir.exists() || !docDir.canRead()) {
			System.out.println("Document directory '" + docDir.getAbsolutePath()+ "' does not exist or is not readable, please check the path");
			System.exit(1);
		}

		System.out.println("Indexing to directory '" + indexPath + "'...");

		Directory dir = FSDirectory.open(new File(indexPath));
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);

		IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_42, analyzer);
		iwc.setOpenMode(OpenMode.CREATE);

		IndexWriter writer = new IndexWriter(dir, iwc);

		System.out.println("Ready to index");

		String[] files = docDir.list();
		// an IO error could occur
		if (files != null) {
			for (String currentFile : files) {
				BufferedReader in = new BufferedReader(new FileReader(docDir.getAbsolutePath() + "/" + currentFile));
				String line;
				while ((line = in.readLine()) != null){
					Document doc = new Document();
					String [] splitted = line.split("\t");
					if (splitted.length < 7)
						continue;
					//doc.add(new StringField("path", currentFile, Field.Store.YES));
					doc.add(new TextField("content", splitted[6], Field.Store.NO));
					doc.add(new LongField("tweetID", Long.parseLong(splitted[0]), Field.Store.YES));
					writer.addDocument(doc);
					writer.commit();
				}
				in.close();
				System.out.println("File " + currentFile + " done");
			}
		}
		writer.close();
		System.out.println("All done!");

	}


}
