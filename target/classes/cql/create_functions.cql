//
CREATE OR REPLACE FUNCTION datastax_banking_iot.state_group_and_total( state map<text, double>, type text, amount double)
	CALLED ON NULL INPUT	
	RETURNS map<text, double>
	LANGUAGE java AS 'Double total = (Double) state.get(type);  if (total == null) total = amount; else total = total + amount; state.put(type, total); return state;';
//
CREATE OR REPLACE AGGREGATE datastax_banking_iot.group_and_total(text, double)  SFUNC state_group_and_total  STYPE map<text, double>  INITCOND {};


//
CREATE OR REPLACE FUNCTION datastax_banking_iot.filter_location ( state set<text>, location text, filter text, transaction_id text) 
CALLED ON NULL INPUT 
RETURNS set<text>
LANGUAGE java AS 
  'if (location.contains(filter)) {  state.add(transaction_id); } return state;'; 

//
CREATE OR REPLACE AGGREGATE datastax_banking_iot.filter_location(text, text, text) 
SFUNC filter_location  STYPE set<text>   INITCOND {};

//
CREATE OR REPLACE FUNCTION datastax_banking_iot.filter_location_full (state set<frozen<tuple<text,timestamp,text, text,double, text, text>>>, 
location text, filter text, transaction_id text, transaction_time timestamp, user_id text, amount double, merchant text, status text) 
CALLED ON NULL INPUT 
RETURNS set<frozen<tuple<text,timestamp,text, text, double, text, text>>>
LANGUAGE java AS 	
  'if (location.contains(filter)) {   
  	com.datastax.driver.core.TupleType tupleType = com.datastax.driver.core.TupleType.of(com.datastax.driver.core.ProtocolVersion.NEWEST_SUPPORTED, 
  	com.datastax.driver.core.CodecRegistry.DEFAULT_INSTANCE, 
  	com.datastax.driver.core.DataType.text(), 
  	com.datastax.driver.core.DataType.timestamp(), 
  	com.datastax.driver.core.DataType.text(), 
  	com.datastax.driver.core.DataType.text(), 
  	com.datastax.driver.core.DataType.cdouble(), 
  	com.datastax.driver.core.DataType.text(),
  	com.datastax.driver.core.DataType.text()); 
  	
  	TupleValue v = tupleType.newValue(transaction_id, transaction_time, user_id, location, amount, merchant, status);  
  	state.add(v); 
  } return state;'; 

//
CREATE OR REPLACE AGGREGATE datastax_banking_iot.filter_location_full(text, text, text, timestamp, text, double, text, text) 
SFUNC filter_location_full  STYPE set<frozen<tuple<text,timestamp,text, text, double, text, text>>>  INITCOND {};
	 
