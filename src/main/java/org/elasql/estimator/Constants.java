package org.elasql.estimator;

public class Constants {
	
	public static final String[] OU_NAMES = new String[] {
         "OU0 - Broadcast",
         "OU0 - ROUTE",
         "OU1 - Generate Plan",
         "OU2 - Initialize Thread",
         "OU3 - Acquire Locks",
         "OU4 - Read from Local",
         "OU5M - Read from Remote",
         "OU6 - Execute Arithmetic Logic",
         "OU7 - Write to Local",
         "OU8 - Commit"
	};
	
	// Field names in CSV files
	public static final String FIELD_NAME_ID = "Transaction ID";
	public static final String FIELD_NAME_IS_MASTER = "Is Master";
	public static final String FIELD_NAME_IS_DIST = "Is Distributed";
	public static final String FIELD_NAME_START_TIME = "Start Time";
	
	// File names
	public static final String FILE_NAME_FEATURE = "transaction-features";
	public static final String FILE_NAME_LATENCY_PREFIX = "transaction-latency-server";
}
