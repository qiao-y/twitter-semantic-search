package edu.columbia.watson.twitter;

import java.util.Date;

/**
 * Entity class for a query. 
 */
public class QueryClause {
	private String queryNumber;
	private String query;
	private Long linkedTweetID;
	private String linkedTweet;
	private Date queryTime;
	
	public QueryClause(String queryNumber, String query, Long linkedTweetID,
			/*String linkedTweet,*/ Date queryTime) {
		this.queryNumber = queryNumber;
		this.query = query;
		this.linkedTweetID = linkedTweetID;
		//this.linkedTweet = linkedTweet;
		this.queryTime = queryTime;
	}
	
	public String getQueryNumber() {
		return queryNumber;
	}
	public void setQueryNumber(String queryNumber) {
		this.queryNumber = queryNumber;
	}
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	public Long getLinkedTweetID() {
		return linkedTweetID;
	}
	public void setLinkedTweetID(Long linkedTweetID) {
		this.linkedTweetID = linkedTweetID;
	}
	public String getLinkedTweet() {
		return linkedTweet;
	}
	public void setLinkedTweet(String linkedTweet) {
		this.linkedTweet = linkedTweet;
	}
	public Date getQueryTime() {
		return queryTime;
	}
	public void setQueryTime(Date queryTime) {
		this.queryTime = queryTime;
	}

	@Override
	public String toString() {
		return "QueryClause [queryNumber=" + queryNumber + ", query=" + query
				+ ", linkedTweetID=" + linkedTweetID + ", linkedTweet="
				+ linkedTweet + ", queryTime=" + queryTime + "]";
	}
	
	
}
