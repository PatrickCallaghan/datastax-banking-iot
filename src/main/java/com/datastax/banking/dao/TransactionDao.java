package com.datastax.banking.dao;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.banking.model.Transaction;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/** 
 * Inserts into 2 tables 
 * @author patrickcallaghan
 *
 */
public class TransactionDao {

	private static Logger logger = LoggerFactory.getLogger(TransactionDao.class);
	private Session session;

	private static String keyspaceName = "datastax_banking_iot";

	private static String transactionTable = keyspaceName + ".transactions";
	private static String latestTransactionTable = keyspaceName + ".latest_transactions";

	private static final String INSERT_INTO_TRANSACTION = "Insert into "
			+ transactionTable
			+ " (cc_no, year, transaction_time, transaction_id, location, merchant, amount, user_id, status, notes, tags) values (?,?,?,?,?,?,?,?,?,?,?);";
	private static final String INSERT_INTO_LATEST_TRANSACTION = "Insert into "
			+ latestTransactionTable
			+ " (cc_no, transaction_time, transaction_id, location, merchant, amount, user_id, status, notes, tags) values (?,?,?,?,?,?,?,?,?,?) ";

	private static final String GET_TRANSACTIONS_BY_ID = "select * from " + transactionTable
			+ " where cc_no = ? and year = ?";
	private static final String GET_TRANSACTIONS_BY_CCNO = "select * from " + transactionTable
			+ " where cc_no = ? and year = ? and transaction_time >= ? and transaction_time < ?";
	private static final String GET_LATEST_TRANSACTIONS_BY_CCNO = "select * from " + latestTransactionTable
			+ " where cc_no = ? and transaction_time >= ? and transaction_time < ?";

	
	private PreparedStatement insertTransactionStmt;
	private PreparedStatement insertLatestTransactionStmt;
	private PreparedStatement getTransactionById;
	private PreparedStatement getTransactionByCCno;
	private PreparedStatement getLatestTransactionByCCno;

	private AtomicLong count = new AtomicLong(0);

	public TransactionDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder().addContactPoints(contactPoints).build();

		this.session = cluster.connect();

		try {
			this.insertTransactionStmt = session.prepare(INSERT_INTO_TRANSACTION);
			this.insertLatestTransactionStmt = session.prepare(INSERT_INTO_LATEST_TRANSACTION);

			this.getTransactionById = session.prepare(GET_TRANSACTIONS_BY_ID);
			this.getTransactionByCCno = session.prepare(GET_TRANSACTIONS_BY_CCNO);
			this.getLatestTransactionByCCno = session.prepare(GET_LATEST_TRANSACTIONS_BY_CCNO);

			this.insertLatestTransactionStmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			this.insertTransactionStmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

		} catch (Exception e) {
			e.printStackTrace();
			session.close();
			cluster.close();
		}
	}

	public void saveTransaction(Transaction transaction) {
		insertTransactionAsync(transaction);
	}

	public void insertTransactionAsync(Transaction transaction) {
		
		int year = new DateTime().withMillis(transaction.getTransactionTime().getTime()).getYear();
		
//		ResultSetFuture future = session.executeAsync(this.insertTransactionStmt.bind(transaction.getCreditCardNo(), year, 
//				transaction.getTransactionTime(), transaction.getTransactionId(), transaction.getLocation(),
//				transaction.getMerchant(), transaction.getAmount(), transaction.getUserId(), transaction.getStatus(),
//				transaction.getNotes(), transaction.getTags()));
		ResultSetFuture future1 = session.executeAsync(this.insertLatestTransactionStmt.bind(
				transaction.getCreditCardNo(), transaction.getTransactionTime(), transaction.getTransactionId(),
				transaction.getLocation(), transaction.getMerchant(), transaction.getAmount(), transaction.getUserId(),
				transaction.getStatus(), transaction.getNotes(), transaction.getTags()));

		//future.getUninterruptibly();
		future1.getUninterruptibly();

		long total = count.incrementAndGet();

		if (total % 10000 == 0) {
			logger.info("Total transactions processed : " + total);
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public Transaction getTransaction(String transactionId) {

		ResultSetFuture rs = this.session.executeAsync(this.getTransactionById.bind(transactionId));

		Row row = rs.getUninterruptibly().one();
		if (row == null) {
			throw new RuntimeException("Error - no transaction for id:" + transactionId);
		}

		return rowToTransaction(row);
	}

	private Transaction rowToTransaction(Row row) {

		Transaction t = new Transaction();

		t.setAmount(row.getDouble("amount"));
		t.setCreditCardNo(row.getString("cc_no"));
		t.setMerchant(row.getString("merchant"));
		t.setLocation(row.getString("location"));
		t.setTransactionId(row.getString("transaction_id"));
		t.setTransactionTime(row.getDate("transaction_time"));
		t.setUserId(row.getString("user_id"));
		t.setNotes(row.getString("notes"));
		t.setStatus(row.getString("status"));
		t.setTags(row.getSet("tags", String.class));

		return t;
	}

	public List<Transaction> getLatestTransactionsForCCNoTagsAndDate(String ccNo, Set<String> tags, DateTime from,
			DateTime to) {
		ResultSet resultSet = this.session.execute(getLatestTransactionByCCno.bind(ccNo, from.toDate(), to.toDate()));
		return processResultSet(resultSet, tags);
	}
	
	public List<Transaction> getTransactionsForCCNoTagsAndDate(String ccNo, Set<String> tags, DateTime from,
			DateTime to) {
		ResultSet resultSet = this.session.execute(getTransactionByCCno.bind(ccNo, from.toDate(), to.toDate()));
		
		return processResultSet(resultSet, tags);
	}

	private List<Transaction> processResultSet(ResultSet resultSet, Set<String> tags) {
		List<Row> rows = resultSet.all();
		List<Transaction> transactions = new ArrayList<Transaction>();

		for (Row row : rows) {

			Transaction transaction = rowToTransaction(row);	
			
			if (tags !=null && tags.size() !=0){
								
				Iterator<String> iter = tags.iterator();
				
				//Check to see if any of the search tags are in the tags of the transaction.
				while (iter.hasNext()) {
					String tag = iter.next();
					
					if (transaction.getTags().contains(tag)) {
						transactions.add(transaction);
						break;
					}
				}
			}else{
				transactions.add(transaction);
			}
		}
		return transactions;
	}
}
