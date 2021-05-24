package com.datastax.banking.dao;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.banking.model.Transaction;
import com.datastax.demo.utils.MovingAverage;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.TupleValue;

/**
 * Inserts into 2 tables
 * 
 * @author patrickcallaghan
 *
 */
public class TransactionDao {

	private static Logger logger = LoggerFactory.getLogger(TransactionDao.class);
	private CqlSession session;

	private static String keyspaceName = "datastax_banking";
	
	private static String transactionTable = keyspaceName + ".transactions";
	private static String latestTransactionTable = keyspaceName + ".latest_transactions";

	private static final String INSERT_INTO_TRANSACTION = "Insert into "
			+ transactionTable
			+ " (cc_no, year, transaction_time, transaction_id, location, merchant, amount, user_id, status, notes, tags) values (?,?,?,?,?,?,?,?,?,?,?);";
	private static final String INSERT_INTO_LATEST_TRANSACTION = "Insert into "
			+ latestTransactionTable
			+ " (cc_no, transaction_time, transaction_id, location, merchant, amount, user_id, status, notes, tags, items_) values (?,?,?,?,?,?,?,?,?,?,?) ";
	private static final String GET_LATEST_TRANSACTIONS_BY_CCNO = "select * from " + latestTransactionTable
			+ " where cc_no = ?";
	
	private static final String GET_TRANSACTIONS_BY_CCNO = "select * from " + transactionTable
			+ " where cc_no = ? and year = ? and transaction_time >= ? and transaction_time < ?";
	
	private static final String GET_LATEST_TRANSACTIONS_BY_CCNO_DATE = "select * from " + latestTransactionTable
			+ " where cc_no = ? and transaction_time >= ? and transaction_time < ?";

//	private static final String FILTER_ALL = "select (location, ?, transaction_id,transaction_time, user_id, amount, merchant, status)"
//			+ " as result from " + latestTransactionTable + " where cc_no = ?";
//	
	private static final String DELETE_RANGE = "delete from " + latestTransactionTable + " where cc_no = ? and transaction_time < ?";
	
	
	private PreparedStatement filter;
	private PreparedStatement filterAll;
	private PreparedStatement insertTransactionStmt;
	private PreparedStatement insertLatestTransactionStmt;
	private PreparedStatement insertLatestTransactionStmt1;
	private PreparedStatement getTransactionById;
	private PreparedStatement getTransactionByCCno;
	private PreparedStatement getLatestTransactionByCCno;
	private PreparedStatement getLatestTransactionByCCnoDate;
	private PreparedStatement deleteRange;

	private MovingAverage ma = new MovingAverage(50);

	private AtomicLong count = new AtomicLong(0);
	private long max = 0;

	private int counter = 0;

	public TransactionDao(String[] contactPoints) {

	    CqlSessionBuilder builder = CqlSession.builder();
	    builder.withLocalDatacenter("core_dc")
	    .withAuthCredentials("viewer", "Viewer!")
	    .addContactPoint(new InetSocketAddress(contactPoints[0], 9042));		
		session = builder.build();

		try {
//			this.filterAll = session.prepare(FILTER_ALL);	
			this.insertTransactionStmt = session.prepare(INSERT_INTO_TRANSACTION);
			this.insertLatestTransactionStmt = session.prepare(INSERT_INTO_LATEST_TRANSACTION);

			this.getLatestTransactionByCCno = session.prepare(GET_LATEST_TRANSACTIONS_BY_CCNO);
			this.getTransactionByCCno = session.prepare(GET_TRANSACTIONS_BY_CCNO);
			this.getLatestTransactionByCCnoDate = session.prepare(GET_LATEST_TRANSACTIONS_BY_CCNO_DATE);
			this.deleteRange = session.prepare(DELETE_RANGE);

			
		} catch (Exception e) {
			e.printStackTrace();
			session.close();
		}
	}

	public void saveTransaction(Transaction transaction) {
		insertTransactionAsync(transaction);
	}

	public void insertTransactionAsync(Transaction transaction) {

		session.execute(this.insertLatestTransactionStmt.bind(transaction.getCreditCardNo(),
				transaction.getTransactionTime().toInstant(), transaction.getTransactionId(), transaction.getLocation(),
				transaction.getMerchant(), transaction.getAmount(), transaction.getUserId(), transaction.getStatus(),
				transaction.getNotes(), transaction.getTags(), transaction.getItems()));

		
		// do stuff
		long total = count.incrementAndGet();

		if (total % 1000 == 0) {
			logger.info("Total transactions processed : " + total)
			;
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

//	private void printHostInfo() {
//		Collection<Host> connectedHosts = session.getState().getConnectedHosts();
//
//		for (Host host : connectedHosts) {
//			logger.info("Open Connections(" + host.getAddress() + ") : " + session.getState().getOpenConnections(host));
//			logger.info("In Flight Queries(" + host.getAddress() + ") : " + session.getState().getInFlightQueries(host));
//		}
//	}
//
//	private String printStats() {
//
//		return (cluster.getMetrics().getErrorMetrics().getSpeculativeExecutions().getCount() + ","
//				+ cluster.getMetrics().getRequestsTimer().getCount() + ","
//				+ format(this.responseSizes.getSnapshot().get95thPercentile()) + ", "
//				+ format(this.responseSizes.getSnapshot().get99thPercentile()) + ", "
//				+ format(this.responseSizes.getSnapshot().get999thPercentile()) + ", "
//				+ format(this.responseSizes.getSnapshot().getMax()) + " Mean : " + format(this.responseSizes
//				.getSnapshot().getMean()));
//
//	}

	public String format(double d) {
		return String.format("%.2f", d / 1000);
	}

	public Transaction getTransaction(String transactionId) {

		ResultSet rs = this.session.execute(this.getTransactionById.bind(transactionId));

		Row row = rs.one();
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
		t.setTransactionTime(Date.from(row.getInstant("transaction_time")));
		t.setUserId(row.getString("user_id"));
		t.setNotes(row.getString("notes"));
		t.setStatus(row.getString("status"));
		t.setTags(row.getSet("tags", String.class));
		t.setItems(row.getMap("items_", String.class, Double.class));

		return t;
	}

	public List<Transaction> getLatestTransactionsForCCNo(String ccNo) {

		long start = System.nanoTime();

		ResultSet resultSet = this.session.execute(getLatestTransactionByCCno.bind(ccNo));

		long end = System.nanoTime();
		long microseconds = (end - start) / 1000;

		if (microseconds > max) {
			max = microseconds;
			logger.info("Max : " + max);
		}

		counter++;		

		return processResultSet(resultSet, null);
	}

	public List<Transaction> getLatestTransactionsForCCNoTagsAndDate(String ccNo, Set<String> tags, DateTime from,
			DateTime to) {
		ResultSet resultSet = this.session.execute(getLatestTransactionByCCno.bind(ccNo, from.toDate(), to.toDate()));
		return processResultSet(resultSet, tags);
	}

	public List<Transaction> getTransactionsForCCNoTagsAndDate(String ccNo, Set<String> tags, DateTime from, DateTime to) {
		ResultSet resultSet = this.session
				.execute(getLatestTransactionByCCnoDate.bind(ccNo, from.toDate(), to.toDate()));

		return processResultSet(resultSet, tags);
	}

//	public List<Transaction> getTransactionsForCCNoTagsAndDateSolr(String ccNo, Set<String> tags, DateTime from,
//			DateTime to) {
//		String location = TransactionGenerator.locations.get(new Double(Math.random()
//				* TransactionGenerator.locations.size()).intValue());
//		String issuer = TransactionGenerator.issuers
//				.get(new Double(Math.random() * TransactionGenerator.issuers.size()).intValue());
//
//		String cql = "select * from datastax_banking_iot.latest_transactions where cc_no='" + ccNo
//				+ "' and solr_query = " + "'{\"q\":\"cc_no:" + ccNo + "\", \"fq\":\"tags:Home AND " + "location:"
//				+ location
//				+ " AND amount:[100 TO 3000] AND transaction_time:[2016-05-20T17:33:18Z TO *] \"}' limit  1000;";
//
//		// String cql =
//		// "select * from datastax_banking_iot.latest_transactions where cc_no='"
//		// + ccNo + "' and solr_query = "
//		// + "'{\"q\":\"cc_no:" + ccNo + "\", \"fq\":\"tags:Home\","
//		// + "\"fq\":\"location:" + location + "\","
//		// + "\"fq\":\"amount:[100 TO 3000]\","
//		// +
//		// "\"fq\":\"transaction_time:[2016-05-20T17:33:18Z TO *] \"}' limit  1000;";
//
//		ResultSet resultSet = this.session.execute(cql);
//		return processResultSet(resultSet, tags);
//	}

//	public List<Transaction> getTransactionsForCCNoTagsAndDateSolr1(String ccNo) {
//		Timer timer1 = new Timer();
//		timer1.start();
//		String cql = "select * from datastax_banking_iot.latest_transactions where cc_no='"
//				+ ccNo
//				+ "' and solr_query = "
//				+ "'{\"q\":\"cc_no:"
//				+ ccNo
//				+ "\", \"fq\":\"cc_no:"
//				+ ccNo
//				+ " AND tags:Home AND transaction_time:[2016-05-20T17:33:18Z TO 2016-06-20T17:33:18Z] \"}' limit  1000;";
//
//		ResultSet resultSet = this.session.execute(cql);
//		return processResultSet(resultSet, null);
//	}
//
//	public List<Transaction> getTransactionsForCCNoTagsAndDateSolr2(String ccNo) {
//		Timer timer1 = new Timer();
//		timer1.start();
//
//		String cql = "select * from datastax_banking_iot.latest_transactions where cc_no='" + ccNo
//				+ "' and solr_query = " + "'{\"q\":\"*:*\", \"fq\":\"cc_no:" + ccNo + "\", \"fq\":\"tags:Home\","
//				+ "\"fq\":\"transaction_time:[2016-05-20T17:33:18Z TO 2016-06-20T17:33:18Z] \"}' limit  1000;";
//
//		ResultSet resultSet = this.session.execute(cql);
//		return processResultSet(resultSet, null);
//	}

	public List<String> getIdsForCCAndFilter(String filter, String ccNo){
		
		
		List<String> ids = new ArrayList<String>();
		ResultSet results = this.session.execute(this.filter.bind(filter, ccNo));
		
		Row row = results.one();
				
		Set<TupleValue> set = row.getSet("result", TupleValue.class);
		
		for (TupleValue tupleValue : set){

			String transactionId = tupleValue.getString(0); 			
			ids.add(transactionId);
		}
		return ids;
	}
	
	public List<Transaction> getTransactionIdsForCCAndFilter(String filter, String ccNo){		
		
		List<Transaction> transactions = new ArrayList<Transaction>();
		ResultSet results = this.session.execute(this.filterAll.bind(filter, ccNo));
		
		Row row = results.one();
				
		Set<TupleValue> set = row.getSet("result", TupleValue.class);
		
		for (TupleValue tupleValue : set){

			logger.info(tupleValue.toString());
			Transaction t = new Transaction();
			t.setTransactionId(tupleValue.getString(0));
			t.setTransactionTime(Date.from(row.getInstant(1)));
			t.setUserId(tupleValue.getString(2));
			t.setLocation(tupleValue.getString(3));
			t.setAmount(tupleValue.getDouble(4));
			t.setMerchant(tupleValue.getString(5));
			t.setStatus(tupleValue.getString(6));
		}
		return transactions;
	}
	
	private List<Transaction> processResultSet(ResultSet resultSet, Set<String> tags) {
		List<Row> rows = resultSet.all();
		List<Transaction> transactions = new ArrayList<Transaction>();

		for (Row row : rows) {

			Transaction transaction = rowToTransaction(row);

			if (tags != null && tags.size() != 0) {

				Iterator<String> iter = tags.iterator();

				// Check to see if any of the search tags are in the tags of the
				// transaction.
				while (iter.hasNext()) {
					String tag = iter.next();

					if (transaction.getTags().contains(tag)) {
						transactions.add(transaction);
						break;
					}
				}
			} else {
				transactions.add(transaction);
			}
		}
		return transactions;
	}

	public void deleteRange(Date date) {
		logger.info("Deleting for :" + date.toString());
		
		List<Row> rows = session.execute("Select distinct cc_no from " + latestTransactionTable).all();
		
		for (Row row: rows){
			
			String ccNo = row.getString("cc_no");
				
			session.execute(this.deleteRange.bind(ccNo, date));	
			logger.info("Deleted for " + ccNo + " for date :" + date.toString());
		}		
	}
}
