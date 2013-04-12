package edu.columbia.watson.twitter;

/**
 * date: April 6, 2013
 * @author qiaoyu
 *
 */
public class Tweet {
	private String id;
	private String user;
	private String text;
	private String date;
	private String position;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}

	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getPosition() {
		return position;
	}
	public void setPosition(String position) {
		this.position = position;
	}

	@Override
	public String toString() {
		return id + "\t" + user + "\t" + text + "\t" + date +"\t" + position;
//		return "Tweet [id=" + id + ", user=" + user + ", text=" + text
//				+ ", date=" + date + ", position=" + position + "]";
	}
	public Tweet(String id, String user, String text, String date,
			String position) {
		super();
		this.id = id;
		this.user = user;
		this.text = text;
		this.date = date;
		this.position = position;
	
	}
	


}
