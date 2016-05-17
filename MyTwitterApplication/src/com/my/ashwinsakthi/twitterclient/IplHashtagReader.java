package com.my.ashwinsakthi.twitterclient;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

import java.util.Map;

import java.text.DateFormat;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.Tweet;
import twitter4j.conf.ConfigurationBuilder;

public class IplHashtagReader {

	static Properties prop=new Properties();
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {

		try {
			System.out.println(System.getProperty("user.dir"));
		ConfigurationBuilder cb = new ConfigurationBuilder();
		prop.load(new FileInputStream("config.properties"));
		
		
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey(prop.getProperty("oauth.consumerKey"))
		  .setOAuthConsumerSecret(prop.getProperty("oauth.consumerSecret"))
		  .setOAuthAccessToken(prop.getProperty("oauth.accessToken"))
		  .setOAuthAccessTokenSecret(prop.getProperty("oauth.accessTokenSecret"));
		
		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();
		String regex = "#(.*?) ";
		Pattern p1 = Pattern.compile(regex);
		

		
			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			Calendar cal = Calendar.getInstance();
			
			Date date = cal.getTime();
			
			String sinceDate="";
			double time=0.00;
			int dateHours=date.getHours();
			
			/*if(date.getHours()==0){
				dateHours=0;
				 time= String.valueOf((23)).concat(":00-")
				.concat(String.valueOf(date.getHours()).concat(":00"));
				 cal.add(Calendar.DATE, -1);
				 date=cal.getTime();
				 sinceDate = dateFormat.format(date);
			}
			else{
				time = String.valueOf((date.getHours() - 1)).concat(":00-")
				.concat(String.valueOf(date.getHours()).concat(":00"));	
				
				sinceDate = dateFormat.format(date);
			}*/			
			
				time = date.getHours();
			
			
			sinceDate = dateFormat.format(date);
			
			System.out.println("time "+time);
			System.out.println("sinceDate "+sinceDate);

			Map<String, Integer> tagCountMap = new HashMap<String, Integer>();
			
			Date latestTweetDate=null;
			
			Date lastThreadTweetDate=null;
			
			int newDate=0;
			
			while((newDate=new Date().getHours())==date.getHours()){
				
				int page=1;
				
				for ( page = 1; page <= 15; page++) {

					Query query = new Query("#IPL");

					query.setRpp(100); // set tweets per page to 1000
					query.setPage(page);
					query.setSince(sinceDate);

					QueryResult qr = twitter.search(query);
					List<Tweet> qrTweets = qr.getTweets();
					
			//		System.out.println("qrTweets size"+qrTweets.size());
					// break out of the loop early if there are no more tweets
				
					if (qrTweets.size() == 0)
						break;
					
			//		FileWriter f1 = new FileWriter(new File("C:/temp/tweets/file"+i+".txt"));

			//		BufferedWriter bw = new BufferedWriter(f1);
					boolean isFirst=true;
					for (Tweet t : qrTweets) {
				//	i++;
						

						if(page==1 && isFirst){
							
							isFirst=false;
							
							lastThreadTweetDate=latestTweetDate;
							
							latestTweetDate=t.getCreatedAt();
						}

						if(lastThreadTweetDate!=null && ((lastThreadTweetDate==t.getCreatedAt() || lastThreadTweetDate.after(t.getCreatedAt())))){
							break;
						}
						if (t.getCreatedAt().getHours() < (dateHours )) {
							break;
						}					
						
						
						int count = 0;
						if (t.getCreatedAt().getHours() == (dateHours )) {						
							
							String matchTag = "";
							Matcher match = p1.matcher(t.getText());
							while (match.find()) {
						
								System.out.println(t.getCreatedAt()+" : "+t.getText());		
					//			bw.write(t.getCreatedAt()+" : "+t.getText());
					//			bw.write("\n");

								matchTag = match.group(1).trim().toUpperCase();

								//If Map contains the data increment the count value else put a new Key.
								if (tagCountMap.keySet().contains(matchTag)) {
									count = tagCountMap.get(matchTag);
									tagCountMap.put(matchTag, ++count);
								} else {
									tagCountMap.put(matchTag, 1);
								}
							}
						}

					}
					//bw.flush();
					//bw.close();
					
				}
				
				Thread.sleep(7*60*1000);
				
			}
			
			TwitterDAO twitterDAO = new TwitterDAO();

			//Check if process for the hour is complete only if its not complete for the hour it can be run.
		
/*			if (!twitterDAO.checkProcessDone(time)) {
				twitterDAO.insertTagData(time, tagCountMap,dateHours);
			} else {
				throw new ProcessCompleteException(
						"Process already completed for this hour !!! ");
			}*/
			
			twitterDAO.insertTagData(time, tagCountMap,dateHours);
			
		}catch (TwitterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}/* catch (ProcessCompleteException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			System.out.println(e.getLocalizedMessage());
		}*/ catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		finally {
			System.out.println("Process Completed");
		}
	}
}
