package com.datastax.banking;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.banking.dao.TransactionDao;
import com.datastax.demo.utils.PropertyHelper;

public class MainDeleteRange {

	private static Logger logger = LoggerFactory.getLogger(MainDeleteRange.class);
	
	private static SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");

	public MainDeleteRange() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		
		TransactionDao dao = new TransactionDao(contactPointsStr.split(","));
		try {
			dao.deleteRange(formatter.parse("16-04-2018 12:00:00"));
		} catch (ParseException e) {			
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
