package com.my.ashwinsakthi.twitterclient;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class TwitterDAO {

	static Properties prop=new Properties();
	
	/**
	 * @param time
	 * @param tagCountMap
	 * 
	 * Method that inserts the required data into DB
	 * @param dateHours 
	 */
	@SuppressWarnings("deprecation")
	void insertTagData(double time,Map<String, Integer> tagCountMap, int dateHours) {
		
		PreparedStatement prStmt = null;
		java.sql.Connection con = null;
		try{	
		prop.load(new FileInputStream("config.properties"));
		
		String URL = prop.getProperty("url");

		String USER = prop.getProperty("dbuser");
		String PASS = prop.getProperty("dbpassword");
		

	
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(URL, USER, PASS);
			con.setAutoCommit(false);
			// prepared statement declaration
			String sqlQuery="";
			
			if(time==23){
				sqlQuery = "insert into PopularHashTags values (trunc(sysdate-1),?,?,?,sysdate)";	
			}
			else{
				sqlQuery = "insert into PopularHashTags values (trunc(sysdate),?,?,?,sysdate)";	
			}
			
			prStmt = con.prepareStatement(sqlQuery);
			String hashTag="";
			Iterator<String> it = tagCountMap.keySet().iterator();
			
			while (it.hasNext()) {
				hashTag = it.next();

				prStmt.setDouble(1, time);
				prStmt.setString(2, hashTag);
				prStmt.setInt(3, tagCountMap.get(hashTag));
				prStmt.addBatch();				
			}
			
			prStmt.executeBatch();
			con.commit();
		}
		
		catch (SQLException sqle) {
			
			System.out.println((sqle.getMessage()));

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			// closing connection			
			try {
				prStmt.close();
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} 
		
	}	
	
	/**
	 *  
	 * @param time
	 * @return
	 * 
	 * Method that checks if process is complete already or not.
	 */
	boolean checkProcessDone(String time){
		java.sql.Connection con = null;
		
		boolean isProcessComplete=false;		
		PreparedStatement prStmt = null;
		ResultSet rset = null;
		
		try{
			prop.load(new FileInputStream("config.properties"));
			
			String URL = prop.getProperty("url");

			String USER = prop.getProperty("dbuser");
			String PASS = prop.getProperty("dbpassword");
			
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(URL, USER, PASS);
			con.setAutoCommit(false);
			// prepared statement declaration
			String sqlQuery = "select time from HashTagDetails where Date_Last_Updated=(select max(Date_Last_Updated) from HashTagDetails)";
			prStmt = con.prepareStatement(sqlQuery);			
			rset=prStmt.executeQuery();
			
			if(rset.next()){
				if(rset.getString("time").trim().equals(time.trim())){
					isProcessComplete=true;
				}
			}
			
		}
		
		catch (SQLException sqle) {
			
			System.out.println((sqle.getMessage()));

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			// closing connection
			try {
				prStmt.close();
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		return isProcessComplete;
	}
	
	
	

}
