package com.datastax.banking;

import java.util.concurrent.BlockingQueue;

import com.datastax.banking.dao.TransactionDao;
import com.datastax.banking.model.Transaction;
import com.datastax.demo.utils.KillableRunner;

public class TransactionWriter implements KillableRunner {

	private volatile boolean shutdown = false;
	private TransactionDao dao;
	private BlockingQueue<Transaction> queue;

	public TransactionWriter(TransactionDao dao, BlockingQueue<Transaction> queue) {
		this.dao = dao;
		this.queue = queue;
	}

	@Override
	public void run() {
		Transaction transaction;
		while(!shutdown){				
			transaction = queue.poll(); 
			
			if (transaction!=null){
				try {
					this.dao.insertTransactionAsync(transaction);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}				
		}				
	}
	
	@Override
    public void shutdown() {
		while(!queue.isEmpty())
			
		shutdown = true;
    }
}
