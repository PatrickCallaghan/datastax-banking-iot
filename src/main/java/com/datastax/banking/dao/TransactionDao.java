	package com.datastax.banking.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.datastax.banking.data.TransactionGenerator;
import com.datastax.banking.model.Transaction;
import com.datastax.demo.utils.MovingAverage;
import com.datastax.demo.utils.Timer;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.ConstantSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

/**
 * Inserts into 2 tables
 * 
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

	private static final String GET_LATEST_TRANSACTIONS_BY_CCNO = "select * from " + latestTransactionTable
			+ " where cc_no = ?";
	private static final String GET_TRANSACTIONS_BY_CCNO = "select * from " + transactionTable
			+ " where cc_no = ? and year = ? and transaction_time >= ? and transaction_time < ?";
	private static final String GET_LATEST_TRANSACTIONS_BY_CCNO_DATE = "select * from " + latestTransactionTable
			+ " where cc_no = ? and transaction_time >= ? and transaction_time < ?";

	private PreparedStatement insertTransactionStmt;
	private PreparedStatement insertLatestTransactionStmt;
	private PreparedStatement getTransactionById;
	private PreparedStatement getTransactionByCCno;
	private PreparedStatement getLatestTransactionByCCno;
	private PreparedStatement getLatestTransactionByCCnoDate;
	
	private MovingAverage ma = new MovingAverage(50);

	private AtomicLong count = new AtomicLong(0);
	private long max = 0;
	
	private final MetricRegistry metrics = new MetricRegistry();
	private final Histogram responseSizes = metrics.histogram(MetricRegistry.name(TransactionDao.class, "latencies"));
	private Cluster cluster;
	private int counter;

	public TransactionDao(String[] contactPoints) {

		ConstantSpeculativeExecutionPolicy policy =
			    new ConstantSpeculativeExecutionPolicy(5,3);
		
		cluster = Cluster.builder()
				.addContactPoints(contactPoints)
				.withSpeculativeExecutionPolicy(policy)				
				.withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
				.build();
		
		this.session = cluster.connect();
		
		try {
			this.insertTransactionStmt = session.prepare(INSERT_INTO_TRANSACTION);
			this.insertLatestTransactionStmt = session.prepare(INSERT_INTO_LATEST_TRANSACTION);

			this.getLatestTransactionByCCno = session.prepare(GET_LATEST_TRANSACTIONS_BY_CCNO);
			this.getTransactionByCCno = session.prepare(GET_TRANSACTIONS_BY_CCNO);
			this.getLatestTransactionByCCnoDate = session.prepare(GET_LATEST_TRANSACTIONS_BY_CCNO_DATE);

			this.insertLatestTransactionStmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			this.insertTransactionStmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

			this.getLatestTransactionByCCno.setIdempotent(true);
			this.insertLatestTransactionStmt.setIdempotent(true);
			this.insertTransactionStmt.setIdempotent(true);
			
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

		// ResultSetFuture future =
		// session.executeAsync(this.insertTransactionStmt.bind(transaction.getCreditCardNo(),
		// year,
		// transaction.getTransactionTime(), transaction.getTransactionId(),
		// transaction.getLocation(),
		// transaction.getMerchant(), transaction.getAmount(),
		// transaction.getUserId(), transaction.getStatus(),
		// transaction.getNotes(), transaction.getTags()));
		
		
		long start = System.nanoTime();
		
		session.execute(this.insertLatestTransactionStmt.bind(
				transaction.getCreditCardNo(), transaction.getTransactionTime(), transaction.getTransactionId(),
				transaction.getLocation(), transaction.getMerchant(), transaction.getAmount(), transaction.getUserId(),
				transaction.getStatus(), transaction.getNotes(), transaction.getTags()));

		// do stuff
		long end = System.nanoTime();
		long microseconds = (end - start)/1000;
		
		//responseSizes.update(microseconds);
		
		if (microseconds > max){
			max = microseconds;
			logger.info("Max : " + max);
		}
		
		long total = count.incrementAndGet();

		if (total % 10000 == 0) {
			logger.info("Total transactions processed : " + total   + " - " + printStats());
			//printHostInfo();
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private void printHostInfo(){
		Collection<Host> connectedHosts = session.getState().getConnectedHosts();
		
		for (Host host : connectedHosts){
			logger.info("Open Connections(" + host.getAddress() + ") : " + session.getState().getOpenConnections(host));
			logger.info("In Flight Queries(" + host.getAddress() + ") : " + session.getState().getInFlightQueries(host));
		}
	}
	
	private String printStats() {		
		
		return (cluster.getMetrics().getErrorMetrics().getSpeculativeExecutions().getCount() + "," +
				cluster.getMetrics().getRequestsTimer().getCount() + "," +
				format(this.responseSizes.getSnapshot().get95thPercentile()) + ", " + format(this.responseSizes.getSnapshot().get99thPercentile())  + ", " + 
				format(this.responseSizes.getSnapshot().get999thPercentile()) + ", " + format(this.responseSizes.getSnapshot().getMax()) + 
				" Mean : " + format(this.responseSizes.getSnapshot().getMean()));
		
	}

	public String format(double d){
		return String.format( "%.2f", d/1000);
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
		t.setTransactionTime(row.getTimestamp("transaction_time"));
		t.setUserId(row.getString("user_id"));
		t.setNotes(row.getString("notes"));
		t.setStatus(row.getString("status"));
		t.setTags(row.getSet("tags", String.class));

		return t;
	}

	public List<Transaction> getLatestTransactionsForCCNo(String ccNo) {
		
		long start = System.nanoTime();
		
		ResultSetFuture resultSet = this.session.executeAsync(getLatestTransactionByCCno.bind(ccNo));
		
		long end = System.nanoTime();
		long microseconds = (end - start)/1000;
		
		if (microseconds > max){
			max = microseconds;
			logger.info("Max : " + max);
		}
		
		counter++;
		if (counter % 10000==0){
			logger.info(printStats());
		}
		
		return processResultSet(resultSet.getUninterruptibly(), null);
	}

	public List<Transaction> getLatestTransactionsForCCNoTagsAndDate(String ccNo, Set<String> tags, DateTime from,
			DateTime to) {
		ResultSet resultSet = this.session.execute(getLatestTransactionByCCno.bind(ccNo, from.toDate(), to.toDate()));
		return processResultSet(resultSet, tags);
	}

	public List<Transaction> getTransactionsForCCNoTagsAndDate(String ccNo, Set<String> tags, DateTime from, DateTime to) {
		ResultSet resultSet = this.session.execute(getLatestTransactionByCCnoDate.bind(ccNo, from.toDate(), to.toDate()));

		return processResultSet(resultSet, tags);
	}

	public List<Transaction> getTransactionsForCCNoTagsAndDateSolr(String ccNo, Set<String> tags, DateTime from, DateTime to) {
		String location = TransactionGenerator.locations.get(new Double(Math.random() * TransactionGenerator.locations.size()).intValue());
		String issuer = TransactionGenerator.issuers.get(new Double(Math.random() * TransactionGenerator.issuers.size()).intValue());
	
		String cql = "select * from datastax_banking_iot.latest_transactions where cc_no='" + ccNo + "' and solr_query = "
				+ "'{\"q\":\"cc_no:" + ccNo + "\", \"fq\":\"tags:Home AND "
						+ "location:" + location + " AND amount:[100 TO 3000] AND transaction_time:[2016-05-20T17:33:18Z TO *] \"}' limit  1000;";
		
//		String cql = "select * from datastax_banking_iot.latest_transactions where cc_no='" + ccNo + "' and solr_query = "
//				+ "'{\"q\":\"cc_no:" + ccNo + "\", \"fq\":\"tags:Home\","
//						+ "\"fq\":\"location:" + location + "\","
//								+ "\"fq\":\"amount:[100 TO 3000]\","
//								+ "\"fq\":\"transaction_time:[2016-05-20T17:33:18Z TO *] \"}' limit  1000;";

		
		ResultSet resultSet = this.session.execute(cql);
		logger.info(printStats());
		
		return processResultSet(resultSet, tags);
	}
		
	public List<Transaction> getTransactionsForCCNoTagsAndDateSolr1(String ccNo) {
		Timer timer1 = new Timer();
		timer1.start();
		String cql = "select * from datastax_banking_iot.latest_transactions where cc_no='" + ccNo + "' and solr_query = "
				+ "'{\"q\":\"cc_no:" + ccNo + "\", \"fq\":\"cc_no:" + ccNo + " AND tags:Home AND transaction_time:[2016-05-20T17:33:18Z TO 2016-06-20T17:33:18Z] \"}' limit  1000;";
		
		ResultSet resultSet = this.session.execute(cql);
		timer1.end();
		long millis = timer1.getTimeTakenMillis();
		ma.newNum(millis);
		System.out.println(ma.getAvg());
		logger.info(printStats());
		return processResultSet(resultSet, null);
	}
	
	public List<Transaction> getTransactionsForCCNoTagsAndDateSolr2(String ccNo) {
		Timer timer1 = new Timer();
		timer1.start();
		
		String cql = "select * from datastax_banking_iot.latest_transactions where cc_no='" + ccNo + "' and solr_query = "
				+ "'{\"q\":\"*:*\", \"fq\":\"cc_no:" + ccNo + "\", \"fq\":\"tags:Home\","
								+ "\"fq\":\"transaction_time:[2016-05-20T17:33:18Z TO 2016-06-20T17:33:18Z] \"}' limit  1000;";

		
		ResultSet resultSet = this.session.execute(cql);
		timer1.end();
		long millis = timer1.getTimeTakenMillis();
		ma.newNum(millis);
		System.out.println(ma.getAvg());
		logger.info(printStats());
		return processResultSet(resultSet, null);
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
}
