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

//output format:
//tweet id, userid, tweet, time, hashtag(null), location(null), url(null)
public class Preprocessor {
	private static String urlRegex = "\\(?\\b(http://|www[.])[-A-Za-z0-9+&@#/%?=~_()|!:,.;]*[-A-Za-z0-9+&@#/%=~_()|]";
	
	public static String parseHTML(String rawText)
	{
		 return Jsoup.parse(rawText).text();
	}
	
	public static void main(String args[])
	{
		if (args.length < 1){
			System.err.print("Usage: run.sh edu.columbia.watson.twitter.Preprocessor filename");
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
				//  System.out.println(urlStr);
			  }
			  
			  System.out.println(id +"\t" + user + "\t" + tweet + "\t" +  time + "\t" + hashTag + "\t" + location + "\t" + urlStr);
			}
			in.close();
		} catch (IOException e) {
			System.out.println("IO exception!");
			e.printStackTrace();
		}

	}
}
