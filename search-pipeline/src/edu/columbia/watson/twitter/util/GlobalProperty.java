/**
 * @author qiaoyu
 * Global property, including corpus files, index files, etc.
 */
package edu.columbia.watson.twitter.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** NOTE: This class is NOT thread-safe
 */
public class GlobalProperty {
	private static GlobalProperty instance = null;
	private String corpusDir;
	private String indexDir;
	
	public static GlobalProperty getInstance() {
		if (instance == null)
			instance = new GlobalProperty();
		return instance;
	}
	
	
	private GlobalProperty() {
		Properties prop = new Properties();
		try {
			InputStream in = new FileInputStream("environment.properties");
			prop.load(in);
			in.close();
		} catch (IOException e) {
			System.err.println("Error loading property file!");
			e.printStackTrace();
		}
		
		corpusDir = prop.getProperty("Corpus_Dir");
		indexDir = prop.getProperty("Corpus_Index");
	}

	public String getCorpusDir() {
		return corpusDir;
	}

	public String getIndexDir() {
		return indexDir;
	}

	
}
