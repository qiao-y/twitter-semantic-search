package edu.columbia.watson.twitter;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CorpusPersister {

	public static void main(String[] args) throws  FileNotFoundException {
		if (args.length < 1){
			System.err.println("Usage: run.sh edu.columbia.watson.twitter.CorpusPersister corpus_dir");
		}

		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Connection conn = null;

		try {
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/twitter-semantic-search", "root", "twitter-semantic-search");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}

        String query = "INSERT INTO tweet_corpus (id, tweet) VALUES(?,?)";
        PreparedStatement pstmt;
		try {
			pstmt = conn.prepareStatement(query);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
        
		BufferedReader in = new BufferedReader(new FileReader(args[0]));
		String line;
		try {
			while ((line = in.readLine()) != null){
				String [] splitted = line.split("\t");
				if (splitted.length < 7)
					continue;
				String tweet = splitted[6];
				Long tweetID = Long.parseLong(splitted[0]);
				pstmt.setLong(1, tweetID);
				pstmt.setString(2, tweet);
				pstmt.execute();
			}
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


}
