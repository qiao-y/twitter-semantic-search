package edu.columbia.watson.twitter;

public class ReadableResult {

	private String tweet;
	private Long tweetID;
	private float score;
	public String getTweet() {
		return tweet;
	}
	public void setTweet(String tweet) {
		this.tweet = tweet;
	}
	public Long getTweetID() {
		return tweetID;
	}
	public void setTweetID(Long tweetID) {
		this.tweetID = tweetID;
	}
	public float getScore() {
		return score;
	}
	public void setScore(float score) {
		this.score = score;
	}
	public ReadableResult(String tweet, Long tweetID, float score) {
		super();
		this.tweet = tweet;
		this.tweetID = tweetID;
		this.score = score;
	}
	
	
	
}
