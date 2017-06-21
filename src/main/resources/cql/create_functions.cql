//
CREATE FUNCTION datastax_banking_iot.state_group_and_total( state map<text, double>, type text, amount double)
	CALLED ON NULL INPUT	
	RETURNS map<text, double>
	LANGUAGE java AS 'Double total = (Double) state.get(type);  if (total == null) total = amount; else total = total + amount; state.put(type, total); return state;';
//
CREATE OR REPLACE AGGREGATE datastax_banking_iot.group_and_total(text, double)  SFUNC state_group_and_total  STYPE map<text, double>  INITCOND {};