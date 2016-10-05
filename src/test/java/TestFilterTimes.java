import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.banking.dao.TransactionDao;
import com.datastax.banking.data.TransactionGenerator;
import com.datastax.banking.model.Transaction;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;


public class TestFilterTimes {
	
	private static Logger logger = LoggerFactory.getLogger(TestFilterTimes.class);
	
	@Test
	public void testFilterAllAnd(){
		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String noOfCreditCardsStr = PropertyHelper.getProperty("noOfCreditCards", "1000000");
		String noOfRequestsStr = PropertyHelper.getProperty("noOfRequests", "10000");

		TransactionDao dao = new TransactionDao(contactPointsStr.split(","));
		BlockingQueue<Transaction> queue = new ArrayBlockingQueue<Transaction>(50);
		int noOfThreads = Integer.parseInt(PropertyHelper.getProperty("noOfThreads", "1"));
		ExecutorService executor = Executors.newFixedThreadPool(noOfThreads);

		int noOfCreditCards = Integer.parseInt(noOfCreditCardsStr);
		int noOfRequests = Integer.parseInt(noOfRequestsStr);
		
		Timer timer = new Timer();
		for (int i = 0; i < noOfRequests; i++) {
			String search = TransactionGenerator.tagList.get(new Double(Math.random() * TransactionGenerator.tagList.size()).intValue());
			Set<String> tags = new HashSet<String>();
			tags.add(search);

			Transaction t = new Transaction();
			t.setCreditCardNo("" + TransactionGenerator.getCreditCardNo(noOfCreditCards));
			
			dao.getTransactionsForCCNoTagsAndDateSolr1(t.getCreditCardNo());
		}
		timer.end();
		
		logger.info("Time taken for " + noOfRequests + " requests "  + timer.getTimeTakenMillis() + "ms");
		System.exit(0);
	}
}
