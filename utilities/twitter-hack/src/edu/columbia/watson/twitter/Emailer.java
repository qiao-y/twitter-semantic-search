/**
 * @author qiaoyu
 * date: April 7, 2013
 * send out email upon completion or error
 * c.f. http://www.mkyong.com/java/javamail-api-sending-email-via-gmail-smtp-example/
 * TODO: use configuration file to load all the parameters - very ugly now
 */
package edu.columbia.watson.twitter;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
 

public class Emailer
{
   public static void sendmail(String content, String to, String from, String subject)
   {    
		final String username = "dummy@gmail.com";
		final String password = "dummy";

		Properties props = new Properties();
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");

		Session session = Session.getInstance(props,
		  new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(username, password);
			}
		  });

		try {

			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress(from));
			message.setRecipients(Message.RecipientType.TO,
				InternetAddress.parse(to));
			message.setSubject(subject);
			message.setText(subject);

			Transport.send(message);

			//System.out.println("Done");

		} catch (MessagingException e) {
			throw new RuntimeException(e);
		}
	}

}
