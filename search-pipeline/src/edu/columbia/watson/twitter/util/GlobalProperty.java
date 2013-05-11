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
	private String dicPath;
	private String docFreqPath;
	private String sigmaIMultUTPath;
	private String tempDir;
	private String mySqlConnectionString;
	private String mySqlUserName;
	private String mySqlPassword;
	private int K;
	private int docNum;
	
	public static final double EPSILON = 1e-6;
	
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
		dicPath = prop.getProperty("Dictionary_File");
		docFreqPath = prop.getProperty("DocFreq_File");
		sigmaIMultUTPath = prop.getProperty("SigmaI_Mult_UT_File");
		tempDir = prop.getProperty("Temp_Dir");
		K = Integer.valueOf(prop.getProperty("k"));
		docNum = Integer.valueOf(prop.getProperty("Document_Num"));
	}
	
	public String getDocFreqPath() {
		return docFreqPath;
	}

	public String getCorpusDir() {
		return corpusDir;
	}

	public String getIndexDir() {
		return indexDir;
	}

	public String getDicPath() {
		return dicPath;
	}
	
	public String getSigmaIMultUTPath() {
		return sigmaIMultUTPath;
	}
	
	public String getTempDir() {
		return tempDir;
	}

	public int getK() {
		return K;
	}
	
	public int getDocNum() {
		return docNum;
	}

	public String getMySqlConnectionString() {
		return mySqlConnectionString;
	}

	public String getMySqlUserName() {
		return mySqlUserName;
	}

	public String getMySqlPassword() {
		return mySqlPassword;
	}
	
}
