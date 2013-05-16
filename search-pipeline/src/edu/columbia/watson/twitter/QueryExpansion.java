package edu.columbia.watson.twitter;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import cmu.arktweetnlp.Tagger;
import cmu.arktweetnlp.Tagger.TaggedToken;
import edu.columbia.watson.twitter.util.GlobalProperty;
import edu.smu.tspell.wordnet.Synset;
import edu.smu.tspell.wordnet.SynsetType;
import edu.smu.tspell.wordnet.WordNetDatabase;

public class QueryExpansion {

	private Tagger tagger = new Tagger();
	private static Logger logger = Logger.getLogger(QueryExpansion.class);
	private WordNetDatabase database = WordNetDatabase.getFileInstance();
	
	public QueryExpansion() throws IOException
	{
		tagger.loadModel("/cmu/arktweetnlp/model.20120919");
		System.setProperty("wordnet.database.dir",GlobalProperty.getInstance().getWordNetPath());
		//System.setProperty("wordnet.database.dir", "/Users/qiaoyu/Documents/E6998_Semantic_Tech_In_IBM_Watson/twitter-semantic-search/search-pipeline/lib/dict");
		logger.info("Done initializing QueryExpansion");
	}
	
	public String expandQuery(String originalQuery){
		StringBuilder result = new StringBuilder();
		
		List<TaggedToken> taggedTokens = tagger.tokenizeAndTag(originalQuery);

		for (TaggedToken token : taggedTokens) {
			if (token.tag.matches("N|O|^|S|Z|L|M|Y|X|!")){
				result.append(" ");
				result.append(token.token);
				Synset[] synsets = database.getSynsets(token.token, SynsetType.NOUN);
				if (synsets.length > 0){
					String[] temp = synsets[0].getWordForms();
					for (int j = 0 ; j < Math.min(temp.length,3) ; ++j){
						result.append(" ");
						result.append(temp[j]);
					}
				}	
			}
			else if (token.tag.matches("V|T")){
				result.append(" ");
				result.append(token.token);
				Synset[] synsets = database.getSynsets(token.token, SynsetType.VERB);
				if (synsets.length > 0){
					String[] temp = synsets[0].getWordForms();
					for (int j = 0 ; j < Math.min(temp.length,3) ; ++j){
						result.append(" ");
						result.append(temp[j]);
					}
				}	
			}
			else if (token.tag.matches("A")){
				result.append(" ");
				result.append(token.token);
				Synset[] synsets = database.getSynsets(token.token, SynsetType.ADJECTIVE);
				if (synsets.length > 0){
					String[] temp = synsets[0].getWordForms();
					for (int j = 0 ; j < Math.min(temp.length,3) ; ++j){
						result.append(" ");
						result.append(temp[j]);
					}
				}	
			}
			else if (token.tag.matches("R")){
				result.append(" ");
				result.append(token.token);
				Synset[] synsets = database.getSynsets(token.token, SynsetType.ADVERB);
				if (synsets.length > 0){
					String[] temp = synsets[0].getWordForms();
					for (int j = 0 ; j < Math.min(temp.length,3) ; ++j){
						result.append(" ");
						result.append(temp[j]);
					}
				}	
			}
		}
		logger.info("Before expansion, query = " + originalQuery + ", after expansion, query = " + result.toString().trim());
		return  result.toString().trim();
	}
	
	
	public static void main(String[] args) throws IOException{
		QueryExpansion qe = new QueryExpansion();
		System.out.println(qe.expandQuery("The Political Power of Social Media | Foreign Affairs: http://fam.ag/i5A7Av"));
		
		
	}
	
	
}
