package com.datastax.banking.data;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.joda.time.DateTime;

import com.datastax.banking.model.Transaction;

public class TransactionGenerator {

	private static DateTime date = new DateTime().minusDays(10).withTimeAtStartOfDay();
	private static final long DAY_MILLIS = 1000 * 60 *60 * 24;
	
	public static Transaction createRandomTransaction(int noOfCreditCards, int noOfDays, Transaction transaction) {

		long noOfMillis = noOfDays * DAY_MILLIS;
		
		long creditCardNo = getCreditCardNo(noOfCreditCards);

		int noOfItems = new Double(Math.ceil(Math.random() * 5)).intValue();

		createItemsAndAmount(noOfItems, transaction);
		
		String location = locations.get(new Double(Math.random() * locations.size()).intValue());

		int randomIssuer = new Double(Math.random() * issuers.size()).intValue();
		String issuer = issuers.get(randomIssuer);
		String note = notes.get(randomIssuer);
		String tag = tagList.get(randomIssuer);
		Set<String> tags = new HashSet<String>();
		tags.add(note);
		tags.add(tag);

		long millis = DateTime.now().getMillis() - (new Double(Math.random() * noOfMillis).longValue() + 1l);
		DateTime newDate = DateTime.now().withMillis(millis);

		transaction.setCreditCardNo(new Long(creditCardNo).toString());
		transaction.setMerchant(issuer);
		transaction.setTransactionId(UUID.randomUUID().toString());
		transaction.setTransactionTime(date.toDate());
		transaction.setLocation(location);
		transaction.setNotes(note);
		transaction.setTags(tags);
		transaction.setTransactionTime(newDate.toDate());
		return transaction;
	}


	public static long getCreditCardNo(int noOfCreditCards) {
		long creditCardNo = new Double(Math.ceil(Math.random() * noOfCreditCards)).longValue();

		// Allow for some skew
		if (Math.random() < .05)
			creditCardNo = creditCardNo % 1000;

		creditCardNo = creditCardNo + 1234123412341233l;
		return creditCardNo;
	}

	
	/**
	 * Creates a random transaction with some skew for some accounts.
	 * @param noOfCreditCards
	 * @return
	 */
	
	private static void createItemsAndAmount(int noOfItems, Transaction transaction) {
		Map<String, Double> items = new HashMap<String, Double>();
		double totalAmount = 0;

		for (int i = 0; i < noOfItems; i++) {

			double amount = new Double(Math.random() * 1000);
			items.put("item" + i, amount);

			totalAmount += amount;
		}
		transaction.setAmount(totalAmount);
		transaction.setItems(items);		
	}
	
	public static List<String> locations = Arrays.asList("London", "Manchester", "Liverpool", "Glasgow", "Dundee",
			"Birmingham");

	public static List<String> issuers = Arrays.asList("Tesco", "Sainsbury", "AsdaWal-MartStores", "Morrisons",
			"Marks&Spencer", "Boots", "JohnLewis", "Waitrose", "Argos", "Co-op", "Currys", "PCWorld", "B&Q",
			"Somerfield", "Next", "Spar", "Amazon", "Costa", "Starbucks", "BestBuy", "Wickes", "TFL", "NationalRail",
			"PizzaHut", "LocalPub");

	public static List<String> notes = Arrays.asList("Shopping", "Shopping", "Shopping", "Shopping", "Shopping",
			"Pharmacy", "HouseHold", "Shopping", "Household", "Shopping", "Tech", "Tech", "Diy", "Shopping", "Clothes",
			"Shopping", "Amazon", "Coffee", "Coffee", "Tech", "Diy", "Travel", "Travel", "Eating out", "Eating out");

	public static List<String> tagList = Arrays.asList("Home", "Home", "Home", "Home", "Home", "Home", "Home", "Home",
			"Work", "Work", "Work", "Home", "Home", "Home", "Work", "Work", "Home", "Work", "Work", "Work", "Work",
			"Work", "Work", "Work", "Work");

}
