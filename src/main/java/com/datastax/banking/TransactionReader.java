package com.datastax.banking;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.joda.time.DateTime;

import com.datastax.banking.dao.TransactionDao;
import com.datastax.banking.model.Transaction;
import com.datastax.banking.service.SearchService;
import com.datastax.demo.utils.KillableRunner;

public class TransactionReader implements KillableRunner {

	private volatile boolean shutdown = false;
	private TransactionDao dao;
	private BlockingQueue<Transaction> queue;

	public TransactionReader(TransactionDao dao, BlockingQueue<Transaction> queue) {
		this.queue = queue;
		this.dao =dao;
	}

	@Override
	public void run() {
		Transaction transaction;
		while(!shutdown){				
			transaction = queue.poll(); 
			
			if (transaction!=null){
				try {
					dao.getLatestTransactionsForCCNo(transaction.getCreditCardNo());					
				} catch (Exception e) {
					e.printStackTrace();
				}
		
			}				
		}				
	}
	
	@Override
    public void shutdown() {
        shutdown = true;
    }
}