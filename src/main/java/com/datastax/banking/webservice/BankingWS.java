package com.datastax.banking.webservice;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import javax.jws.WebService;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.banking.model.Transaction;
import com.datastax.banking.service.SearchService;
import com.datastax.banking.service.SearchService;

@WebService
@Path("/")
public class BankingWS {

	private Logger logger = LoggerFactory.getLogger(BankingWS.class);
	private SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyyMMdd");

	//Service Layer.
	private SearchService service = new SearchService();
	
	@GET
	@Path("/gettransactions/{creditcardno}/{from}/{to}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getMovements(@PathParam("creditcardno") String ccNo, @PathParam("from") String fromDate,
			@PathParam("to") String toDate) {
		
		DateTime from = DateTime.now();
		DateTime to = DateTime.now();
		try {
			from = new DateTime(inputDateFormat.parse(fromDate));
			to = new DateTime(inputDateFormat.parse(toDate));
		} catch (ParseException e) {
			String error = "Caught exception parsing dates " + fromDate + "-" + toDate;
			
			logger.error(error);
			return Response.status(Status.BAD_REQUEST).entity(error).build();
		}
				
		List<Transaction> result = service.getTransactionsByTagAndDate(ccNo, null, from, to);
		logger.info("Returned response");
		return Response.status(Status.OK).entity(result).build();
	}
	
}
