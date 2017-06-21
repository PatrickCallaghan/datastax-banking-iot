Banking-IoT
========================
See here for use case and requirements - https://gist.github.com/PatrickCallaghan/68ae4aa415982e383188

A bank wants to help locate and tag all their expenses/transactions in their bank account to help them categorise their spending. The users will be able to tag any expense/transaction to allow for efficient retrieval and reporting. There will be 10 millions customers with on average 500 transactions a year. Some business customers may have up to 10,000 transactions a year. The client wants the tagged items to show up in searches in less than a second to give users a seamless experience between devices.

This requires DataStax Enterprise running in Solr mode.

To create the schema, run the following

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup" -DcontactPoints=localhost
	
To create some transactions, run the following 
	
	mvn clean compile exec:java -Dexec.mainClass="com.datastax.banking.Main"  -DcontactPoints=localhost

You can use the following parameters to change the default no of transactions and credit cards 
	
	-DnoOfTransactions=10000000 -DnoOfCreditCards=100000
	
To create the solr core, run 

	bin/dsetool create_core datastax_banking_iot.latest_transactions schema=schema.xml solrconfig=solr_config.xml	 reindex=true 

An example of cql queries would be

For the latest transaction table we can run the following types of queries
```
use datastax_banking_iot;

select * from latest_transactions where cc_no = '1234123412341234';

select * from latest_transactions where cc_no = '1234123412341234' and transaction_time > '2017-04-08';

select * from latest_transactions where cc_no = '1234123412341234' and transaction_time > '2017-04-08' and transaction_time < '2017-04-20';
```

We can also use functions like sum to aggregate the totals from a time range. 

```
use datastax_banking_iot;

select sum(amount) from latest_transactions where cc_no = '1234123412341234' and transaction_time > '2017-04-08';
```

As part of the setup, we have created a user defined function (UDF) which will group any text column with the amounts to help us to see spending usage. So we can ask the following 
```
select group_and_total(merchant, amount) from latest_transactions where cc_no='1234123412341833' ;

select group_and_total(location, amount) from latest_transactions where cc_no='1234123412341833' and transaction_time > '2017-04-08' and transaction_time < '2017-04-20'; ;

```

For the (historic) transaction table we need to add the year into our queries.

```
select * from transactions where cc_no = '1234123412341234' and year = 2016;

select * from transactions where cc_no = '1234123412341234' and year = 2016 and transaction_time > '2017-01-31';

select * from transactions where cc_no = '1234123412341234' and year = 2016 and transaction_time > '2017-01-31' and transaction_time < '2017-04-27';
``` 	
Using the solr_query

Get all the latest transactions from PC World in London (This is accross all credit cards and users)
```
select * from latest_transactions where solr_query = 'merchant:PC+World location:London' limit  100;
```
Get all the latest transactions for credit card '1' that have a tag of Work. 
```
select * from latest_transactions where solr_query = '{"q":"cc_no:1234123412341234", "fq":"tags:Work"}' limit  1000;
```
Gell all the transaction for credit card '1' that have a tag of Work and are within the last month
```
select * from latest_transactions where solr_query = '{"q":"cc_no:1234123412341234", "fq":"tags:Work AND transaction_time:[NOW-30DAY TO *]"}' limit  1000;
```
To use the webservice, start the web server using 
```
mvn jetty:run -DcontactPoints=node0 -Djetty.port=8081
```
Open a browser and use a url like 
```
http://{servername}:8081/datastax-banking-iot/rest/gettransactions/{creditcardno}/{from}/{to}
```
Note : the from and to are dates in the format yyyyMMdd hh:mm:ss - eg 
```
http://localhost:8081/datastax-banking-iot/rest/gettransactions/1234123412341234/20170401/20170420/
```

To run the requests run the following 
	
	mvn clean compile exec:java -Dexec.mainClass="com.datastax.banking.RunRequests" -DcontactPoints=localhost

To change the no of requests and no of credit cards add the following 

	-DnoOfRequests=100000  -DnoOfCreditCards=1000000
	
To remove the tables and the schema, run the following.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown"
    
    
