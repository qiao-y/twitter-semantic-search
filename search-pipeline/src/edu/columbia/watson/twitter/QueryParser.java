/**
 * @author qiaoyu 
 * 2013.5.4
 */

package edu.columbia.watson.twitter;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/** XML parser from http://www.mkyong.com/java/how-to-read-xml-file-in-java-dom-parser/  **/

public class QueryParser {

	private static Logger logger = Logger.getLogger(QueryParser.class);
	private static final SimpleDateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");

	public static List<QueryClause> getAllQueriesFromFile(String queryFileName) throws ParserConfigurationException, SAXException, IOException, DOMException, ParseException{	
		logger.info("Before parsing query file: " + queryFileName);
		File topicFile = new File(queryFileName);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(topicFile);
		logger.info("Document built");
		doc.getDocumentElement().normalize();

		List<QueryClause> result = new ArrayList<QueryClause>();
		NodeList nodeList = doc.getElementsByTagName("top");
		for (int i = 0 ; i < nodeList.getLength() ; ++i){
			Node item = nodeList.item(i);
			if (item.getNodeType() == Node.ELEMENT_NODE) {
				Element element = (Element)item;
				String queryNumber = element.getElementsByTagName("num").item(0).getTextContent().trim().substring(8);
				String query = element.getElementsByTagName("query").item(0).getTextContent().trim();
				Date queryTime =  df.parse(element.getElementsByTagName("querytime").item(0).getTextContent().trim());
				Long tweetID = Long.valueOf(element.getElementsByTagName("querytweettime").item(0).getTextContent().trim());

				QueryClause queryClause = new QueryClause(queryNumber,query,tweetID,queryTime);
				logger.info("Parsing query: " + queryClause.toString());
				result.add(queryClause);
			}
		}
		logger.info("Done parsing query, totoal size = " + result.size());
		return result;
	}

	public static void main(String [] args) throws DOMException, ParserConfigurationException, SAXException, IOException, ParseException{
		QueryParser.getAllQueriesFromFile("/Users/qiaoyu/Documents/E6998_Semantic_Tech_In_IBM_Watson/twitter-semantic-search/search-pipeline/data/2012.xml");

	}

}
