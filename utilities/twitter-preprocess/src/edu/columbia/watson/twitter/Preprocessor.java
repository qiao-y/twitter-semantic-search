/**
 * @author qiaoyu
 * date: Apr 20, 2013
 */

package edu.columbia.watson.twitter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

//output format:
//tweet id, userid, time, hashtag(null), location(null), url(null), tweet
public class Preprocessor {
	private static String urlRegex = "\\(?\\b(http://|www[.])[-A-Za-z0-9+&@#/%?=~_()|!:,.;]*[-A-Za-z0-9+&@#/%=~_()|]";
	
	public static String parseHTML(String rawText)
	{
		 return Jsoup.parse(rawText).text();
	}
	
	public static void initLanguageDetector() throws LangDetectException
	{
		DetectorFactory.loadProfile("./profiles");
	}
	
	public static void main(String args[])
	{
		if (args.length < 1){
			System.err.println("Usage: run.sh edu.columbia.watson.twitter.Preprocessor filename");
			return;
		}
		
		
		try {
			initLanguageDetector();
		} catch (LangDetectException e1) {
			System.err.println("Error initing language detector!");
			e1.printStackTrace();
			return;
		}
		
		
		BufferedReader in;
		try {
			in = new BufferedReader(new FileReader(args[0]));
			while (in.ready()) {	
			  String line = in.readLine();
			  String splitted[] = line.split("\t");
			  String id = splitted[0];
			  String user = splitted[1];
			  String rawTweet = splitted[2];
			  String time = splitted.length >= 4 ? splitted[3] : "";
			  
			  //detect language
			  Detector detector;
			try {
				detector = DetectorFactory.create();
				detector.append(rawTweet);
				if (!detector.detect().equals("en"))
					continue;
				//System.out.println(detector.detect());
			} catch (LangDetectException e) {
				//swallow the exception
			}
			 		  
			  //extract geo location
			  String location = "";
			  if (splitted.length >= 5){
				  int index = splitted[4].lastIndexOf("from");
				  location = index != -1 ? splitted[4].substring(index + 5) : "";
			  }
			  
			  String tweet = parseHTML(rawTweet);	//remove HTML tags
			  
			  //eliminate retweets
			  if (tweet.length() >= 2 && tweet.charAt(0) == 'R' && tweet.charAt(1) == 'T')
				  continue;
			  
			  String hashTag = "";
			  String[] tweetSplitted = tweet.split(" ");
			  tweet = "";	//ready to reuse
			  for (String tweetPart : tweetSplitted){		//remove @ tags and extract # tags
				  if (tweetPart.length() <= 0)
					  break;
				  boolean all_number = true;
				  //remove numbers
				  for (int i = 0 ; i < tweetPart.length() ; ++i)
					  if (!('0' <= tweetPart.charAt(i) && tweetPart.charAt(i) <= '9')){
						  all_number = false;
						  break;
					  }
				  if (all_number)
					  continue;
				  char firstChar = tweetPart.charAt(0);
				  if (firstChar == '#'){
					  //tweet += tweetPart.substring(1);
					  hashTag = tweetPart;
				  }
				  else if (firstChar != '@')
					  tweet += tweetPart + " ";
			  }			  

			  //extract URLs
			  //http://blog.houen.net/java-get-url-from-string/
			  Pattern p = Pattern.compile(urlRegex);
			  Matcher m = p.matcher(tweet);
			  String urlStr = "";
			  if (m.find()) {
				  urlStr = m.group();
				  if (urlStr.startsWith("(") && urlStr.endsWith(")")){
					  urlStr = urlStr.substring(1, urlStr.length() - 1);
				  }
			  }
			  if (id == "")
				  continue;
			  System.out.println(id + "\t" + user + "\t" +  time + "\t" + hashTag + "\t" + location + "\t" + urlStr + "\t" + tweet + "\t");
			}
			in.close();
		} catch (IOException e) {
			System.err.println("IO exception!");
			e.printStackTrace();
		} 

	}
}
