package com.datastax.demo;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.FileUtils;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;

public abstract class RunCQLFile {

	private static Logger logger = LoggerFactory.getLogger(RunCQLFile.class);
	static String CREATE_KEYSPACE;
	static String DROP_KEYSPACE;

	private CqlSession session;
	private String CQL_FILE;

	RunCQLFile(String cqlFile) {

		logger.info("Running file " + cqlFile);
		this.CQL_FILE = cqlFile;

		String contactPointsStr = System.getProperty("contactPoints");
		if (contactPointsStr == null) {
			contactPointsStr = "127.0.0.1";
		}

	    CqlSessionBuilder builder = CqlSession.builder();
	    builder.withLocalDatacenter("core_dc").addContactPoint(new InetSocketAddress(contactPointsStr, 9042));		
		session = builder.build();
	}

	void internalSetup(boolean runAll) {
		this.runfile(runAll);
	}

	void runfile(boolean runAll) {
		String readFileIntoString = FileUtils.readFileIntoString(CQL_FILE);

		if (runAll) {
			
			String[] commands = readFileIntoString.split("//");

			for (String command : commands) {

				String cql = command.trim();

				this.run(cql);
			}
		} else {
			String[] commands = readFileIntoString.split(";");

			for (String command : commands) {

				String cql = command.trim();

				if (cql.isEmpty()) {
					continue;
				}

				if (cql.toLowerCase().startsWith("drop")) {
					this.runAllowFail(cql);
				} else {
					this.run(cql);
				}
			}
		}
	}

	void runAllowFail(String cql) {
		try {
			run(cql);
		} catch (InvalidQueryException e) {
			logger.warn("Ignoring exception - " + e.getMessage());
		}
	}

	void run(String cql) {
		if (cql.isEmpty()) return;
		logger.info("Running : " + cql);
		session.execute(cql);
	}

	void sleep(int i) {
		try {
			Thread.sleep(i);
		} catch (Exception e) {
		}
	}

	void shutdown() {
		session.close();
	}
}
