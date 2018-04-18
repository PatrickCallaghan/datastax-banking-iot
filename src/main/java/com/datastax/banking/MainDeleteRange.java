package com.datastax.banking;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.banking.dao.TransactionDao;
import com.datastax.demo.utils.PropertyHelper;

public class MainDeleteRange {

	private static Logger logger = LoggerFactory.getLogger(MainDeleteRange.class);
	
	private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd hh:MM:ss");

	public MainDeleteRange() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		
		TransactionDao dao = new TransactionDao(contactPointsStr.split(","));
		try {
			dao.deleteRange(formatter.parse("2018/17/04 18:00:00"));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new MainDeleteRange();

		System.exit(0);
	}

}
