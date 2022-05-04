package org.elasql.estimator;

import java.util.ArrayList;
import java.util.List;

import smile.data.type.DataTypes;
import smile.data.type.StructField;
import smile.data.type.StructType;

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
	
	public static final String[] NON_ARRAY_FEATURES = new String[] {
        "Tx Type",
        "Dependency - Max Depth",
        "Dependency - First Layer Tx Count",
        "Dependency - Total Tx Count",
        "Number of Insert Records",
        "Number of Overflows in Fusion Table"
	};
	
	public static final String[] ARRAY_FEATURES = new String[] {
        "Read Data Distribution",
        "Read Data in Cache Distribution",
        "Update Data Distribution",
        "System CPU Load",
        "Process CPU Load",
        "System Load Average",
        "Thread Active Count",
        "I/O Read Bytes",
        "I/O Write Bytes",
        "I/O Queue Length",
        "Number of Read Record in Last 100 us",
        "Number of Read Record Excluding Cache in Last 100 us",
        "Number of Update Record in Last 100 us",
        "Number of Insert Record in Last 100 us",
        "Number of Commit Tx in Last 100 us",
        "Number of Read Record in Last 500 us",
        "Number of Read Record Excluding Cache in Last 500 us",
        "Number of Update Record in Last 500 us",
        "Number of Insert Record in Last 500 us",
        "Number of Commit Tx in Last 500 us",
        "Number of Read Record in Last 1000 us",
        "Number of Read Record Excluding Cache in Last 1000 us",
        "Number of Update Record in Last 1000 us",
        "Number of Insert Record in Last 1000 us",
        "Number of Commit Tx in Last 1000 us"
	};
	
	public static final StructType FEATURE_SCHEMA;
	
	// Field names in CSV files
	public static final String FIELD_NAME_ID = "Transaction ID";
	public static final String FIELD_NAME_IS_MASTER = "Is Master";
	public static final String FIELD_NAME_IS_DIST = "Is Distributed";
	public static final String FIELD_NAME_START_TIME = "Start Time";
	
	// File names
	public static final String FILE_NAME_FEATURE = "transaction-features";
	public static final String FILE_NAME_LATENCY_PREFIX = "transaction-latency-server";
	
	static {
		List<StructField> featureFields = new ArrayList<StructField>();
		
		for (int i = 0; i < NON_ARRAY_FEATURES.length; i++) {
			featureFields.add(new StructField(NON_ARRAY_FEATURES[i],
					DataTypes.DoubleType));
		}
		for (int i = 0; i < ARRAY_FEATURES.length; i++) {
			featureFields.add(new StructField(ARRAY_FEATURES[i],
					DataTypes.DoubleType));
		}
		
		List<StructField> featureFieldsWithId = new ArrayList<StructField>();
		featureFieldsWithId.add(new StructField(FIELD_NAME_ID, DataTypes.LongType));
		featureFieldsWithId.addAll(featureFields);
		
		FEATURE_SCHEMA = new StructType(featureFields);
	}
}
