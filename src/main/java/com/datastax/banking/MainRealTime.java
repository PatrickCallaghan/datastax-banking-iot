package com.datastax.banking;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.banking.dao.TransactionDao;
import com.datastax.banking.data.TransactionGenerator;
import com.datastax.banking.model.Transaction;
import com.datastax.demo.utils.KillableRunner;
import com.datastax.demo.utils.PropertyHelper;

public class MainRealTime {

	private static Logger logger = LoggerFactory.getLogger(MainRealTime.class);

	public MainRealTime() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String noOfCreditCardsStr = PropertyHelper.getProperty("noOfCreditCards", "100000");
		String noOfTransactionsStr = PropertyHelper.getProperty("noOfTransactions", "1000000");
		int noOfDays = Integer.parseInt(PropertyHelper.getProperty("noOfDays", "0"));
		
		BlockingQueue<Transaction> queue = new ArrayBlockingQueue<Transaction>(1000);
		List<KillableRunner> tasks = new ArrayList<>();
		
		//Executor for Threads
		int noOfThreads = Integer.parseInt(PropertyHelper.getProperty("noOfThreads", "1"));
		ExecutorService executor = Executors.newFixedThreadPool(noOfThreads);
		TransactionDao dao = new TransactionDao(contactPointsStr.split(","));

		int noOfTransactions = Integer.parseInt(noOfTransactionsStr);
		int noOfCreditCards = Integer.parseInt(noOfCreditCardsStr);

		logger.info("Writing " + noOfTransactions + " transactions for " + noOfCreditCards + " credit cards.");

		for (int i = 0; i < noOfThreads; i++) {
			
			KillableRunner task = new TransactionWriter(dao, queue);
			executor.execute(task);
			tasks.add(task);
		}
				
		while (true){
			try{
				queue.put(TransactionGenerator.createRandomTransaction(noOfCreditCards,noOfDays, new Transaction()));
				sleep(5);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	
	}

	private void sleep(int i) {
		
		try {
			Thread.sleep(i);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new MainRealTime();

		System.exit(0);
	}

}
