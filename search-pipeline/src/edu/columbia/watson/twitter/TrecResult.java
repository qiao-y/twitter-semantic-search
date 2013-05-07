package edu.columbia.watson.twitter;

/**
 * for TREC 2011 microblog track,
 * c.f. https://sites.google.com/site/microblogtrack/2011-guidelines
 * @author qiaoyu
 *
 */
public class TrecResult {
	private String topicNumber;
	private static final String iteration = "Q0";
	private long tweetID;
	private int rank;
	private float score;
	private String run;
	
	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append(topicNumber);
		sb.append(" ");
		sb.append(iteration);
		sb.append(" ");
		sb.append(tweetID);
		sb.append(" ");
		sb.append(rank);
		sb.append(" ");
		sb.append(score);
		sb.append(" ");
		sb.append(run);
		sb.append("\n");
		return sb.toString();
	}
	
	public TrecResult(String topicNumber, long tweetID, int rank, float score,
			String run) {
		super();
		this.topicNumber = topicNumber;
		this.tweetID = tweetID;
		this.rank = rank;
		this.score = score;
		this.run = run;
	}
	public String getTopicNUmber() {
		return topicNumber;
	}
	public void setTopicNUmber(String topicNumber) {
		this.topicNumber = topicNumber;
	}
	public long getTweetID() {
		return tweetID;
	}
	public void setTweetID(long tweetID) {
		this.tweetID = tweetID;
	}
	public int getRank() {
		return rank;
	}
	public void setRank(int rank) {
		this.rank = rank;
	}
	public float getScore() {
		return score;
	}
	public void setScore(float score) {
		this.score = score;
	}
	public String getRun() {
		return run;
	}
	public void setRun(String run) {
		this.run = run;
	}
	
	
	
	
}
