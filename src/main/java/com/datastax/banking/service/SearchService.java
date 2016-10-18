package com.datastax.banking.service;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.DateTime;

import com.datastax.banking.dao.TransactionDao;
import com.datastax.banking.model.Transaction;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;

public class SearchService {

	public static TransactionDao dao;
	private long timerSum = 0;
	private AtomicLong timerCount= new AtomicLong();

	public SearchService() {		
		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		
		if (dao==null){
			SearchService.dao = new TransactionDao(contactPointsStr.split(","));
		}
	}	
	
	public double getTimerAvg(){
		return timerSum/timerCount.get();
	}

	public List<Transaction> getTransactionsByTagAndDate(String ccNo, Set<String> search, DateTime from, DateTime to) {
		
		Timer timer = new Timer();
		List<Transaction> transactions;

		//If the to and from dates are within the 3 months we can use the latest transactions table as it should be faster.		
		if (from.isAfter(DateTime.now().minusMonths(3))){
			transactions = dao.getTransactionsForCCNoTagsAndDate(ccNo, search, from, to);
		}else{
			transactions = dao.getLatestTransactionsForCCNoTagsAndDate(ccNo, search, from, to);
		}
			
		timer.end();
		timerSum += timer.getTimeTakenMillis();
		timerCount.incrementAndGet();
		return transactions;
	}
	
	public List<Transaction> getTransactionsForCCNoTagsAndDateSolr(String ccNo, Set<String> tags, DateTime from, DateTime to) {
		
		Timer timer = new Timer();
		List<Transaction> transactions;

		transactions = dao.getTransactionsForCCNoTagsAndDateSolr(ccNo, tags, from, to);
		timer.end();
		timerSum += timer.getTimeTakenMillis();
		timerCount.incrementAndGet();
		return transactions;
	}
}
