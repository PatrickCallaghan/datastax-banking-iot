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
import com.datastax.demo.utils.ThreadUtils;
import com.datastax.demo.utils.Timer;

public class Main {

	private static Logger logger = LoggerFactory.getLogger(Main.class);

	public Main() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String noOfCreditCardsStr = PropertyHelper.getProperty("noOfCreditCards", "10000");
		String noOfTransactionsStr = PropertyHelper.getProperty("noOfTransactions", "100000");
		int noOfDays = Integer.parseInt(PropertyHelper.getProperty("noOfDays", "360"));
		
		BlockingQueue<Transaction> queue = new ArrayBlockingQueue<Transaction>(1000);
		List<KillableRunner> tasks = new ArrayList<>();
		
		//Executor for Threads
		int noOfThreads = Integer.parseInt(PropertyHelper.getProperty("noOfThreads", "8"));
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
		
		Timer timer = new Timer();
		for (int i = 0; i < noOfTransactions; i++) {
			
			try{
				queue.put(TransactionGenerator.createRandomTransaction(noOfCreditCards,noOfDays));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		timer.end();
		ThreadUtils.shutdown(tasks, executor);
		System.exit(0);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Main();

		System.exit(0);
	}

}
