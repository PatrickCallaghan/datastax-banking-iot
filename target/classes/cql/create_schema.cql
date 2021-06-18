create keyspace if not exists datastax_banking WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' };

use datastax_banking; 
create table if not exists latest_transactions(
	cc_no text,
	transaction_time timestamp,
 	transaction_id text,
 	user_id text,
	location text,
	items_ map<text, double>,
	merchant text,
	amount double,
	status text,
	notes text,
	tags set<text>,
	PRIMARY KEY (cc_no, transaction_time) 
) WITH CLUSTERING ORDER BY (transaction_time desc) 
and default_time_to_live = 2592000 
and gc_grace_seconds = 10; 

create table if not exists transactions(
	cc_no text,	
	year int,
	transaction_time timestamp,
 	transaction_id text,
 	user_id text,
	location text,
	items_ map<text, double>,
	merchant text,
	amount double,
	status text,
	notes text,
	tags set<text>,
	PRIMARY KEY ((cc_no, year), transaction_time)
) WITH CLUSTERING ORDER BY (transaction_time desc);


