package edu.columbia.watson.twitter;
/**
 * Entity class for a query. Currently simply represented as a string
 * @author qiaoyu
 * XXX: Refine this interface
 */
public class QueryClause {
	private String query;
	
	public QueryClause(String theQuery){
		query = theQuery;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}
	
	
	
}
