package org.elasql.estimator.data;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasql.estimator.Constants;

import smile.data.DataFrame;
import smile.data.Tuple;
import smile.data.type.DataTypes;
import smile.data.type.StructType;

public class TotalLatencyDataSet {
	
	public static TotalLatencyDataSet load(File dataSetDir, int serverCount) {
		DataFrame featureDf = loadFeatureFile(dataSetDir);
		DataFrame[] latencyDfs = new DataFrame[serverCount]; 
		for (int serverId = 0; serverId < serverCount; serverId++) {
			latencyDfs[serverId] = loadLatencyFile(dataSetDir, serverId);
		}
		File dependencyFile = new File(dataSetDir, Constants.FILE_NAME_DEPENDENCY);
		TransactionDependencies dependencies = TransactionDependencies.load(dependencyFile);
		return new TotalLatencyDataSet(featureDf, latencyDfs, dependencies);
	}
	
	private static DataFrame loadFeatureFile(File rawDataDir) {
		String featureFileName = String.format("%s.csv",
				Constants.FILE_NAME_FEATURE);
		File featureFilePath = new File(rawDataDir, featureFileName);
		
		// Load the features that start time > warm up time
		return CsvLoader.load(featureFilePath.toPath(), tuple -> true);
	}
	
	private static DataFrame loadLatencyFile(File rawDataDir, int serverId) {
		String labelFileName = String.format("%s-%d.csv",
				Constants.FILE_NAME_LATENCY_PREFIX, serverId);
		File labelFilePath = new File(rawDataDir, labelFileName);
		
		// Load the labels that is as a master transaction
		return CsvLoader.load(labelFilePath.toPath(), tuple -> 
			tuple.getBoolean(Constants.FIELD_NAME_IS_MASTER)
		);
	}
	
	private static class ServerRidPair {
		int serverId;
		int rowId;
	}
	
	// Data
	private DataFrame featureDf;
	private DataFrame[] latencyDfs;
	private TransactionDependencies dependencies;
	
	// Indices
	private long startTxNum, endTxNum;
	private int serverCount;
	private Map<Long, Integer> txNumToFeatureRid;
	private Map<Long, ServerRidPair> txNumToLatencyId;
	
	// Schema
	private StructType featureDfSchema;
	private StructType serverFeatureSchema;
	
	public TotalLatencyDataSet(
		DataFrame featureDf,
		DataFrame[] latencyDfs,
		TransactionDependencies dependencies
	) {
		this.featureDf = featureDf;
		this.latencyDfs = latencyDfs;
		this.dependencies = dependencies;
		
		buildUpFeatureIndex();
		buildUpLatencyIndices();
		setUpSchemas();
	}
	
	public long getStartTxNum() {
		return startTxNum;
	}
	
	public long getEndTxNum() {
		return endTxNum;
	}
	
	public Tuple[] getFeature(Long txNum) {
		Integer rowId = txNumToFeatureRid.get(txNum);
		if (rowId == null)
			return null;
		Tuple tuple = featureDf.get(rowId);
		return separateArrayFeatures(tuple);
	}
	
	public Long getStartTime(Long txNum) {
		int rowId = txNumToFeatureRid.get(txNum);
		return featureDf.getLong(rowId, Constants.FIELD_NAME_START_TIME);
	}
	
	public Double getLatency(Long txNum) {
		ServerRidPair pair = txNumToLatencyId.get(txNum);
		return latencyDfs[pair.serverId]
				.getDouble(pair.rowId, Constants.FIELD_NAME_TOTAL_LATENCY);
	}
	
	public Integer getRoute(Long txNum) {
		ServerRidPair pair = txNumToLatencyId.get(txNum);
		if (pair == null)
			return null;
		return pair.serverId;
	}
	
	public List<Long> getDependencies(Long txNum) {
		return dependencies.getDependencies(txNum);
	}
	
	private void buildUpFeatureIndex() {
		txNumToFeatureRid = new HashMap<Long, Integer>();
		startTxNum = Long.MAX_VALUE;
		endTxNum = Long.MIN_VALUE;
		
		for (int rowId = 0; rowId < featureDf.nrows(); rowId++) {
			Long txNum = featureDf.getLong(rowId, Constants.FIELD_NAME_ID);
			txNumToFeatureRid.put(txNum, rowId);
			
			// Find the start and end tx numbers
			if (txNum < startTxNum)
				startTxNum = txNum;
			if (txNum > endTxNum)
				endTxNum = txNum;
		}
	}
	
	private void buildUpLatencyIndices() {
		txNumToLatencyId = new HashMap<Long, ServerRidPair>();
		serverCount = latencyDfs.length;
		
		for (int serverId = 0; serverId < serverCount; serverId++) {
			DataFrame latencyDf = latencyDfs[serverId];
			for (int rowId = 0; rowId < latencyDf.nrows(); rowId++) {
				ServerRidPair pair = new ServerRidPair();
				pair.serverId = serverId;
				pair.rowId = rowId;
				Long txNum = latencyDf.getLong(rowId, Constants.FIELD_NAME_ID);
				txNumToLatencyId.put(txNum, pair);
			}
		}
	}
	
	private void setUpSchemas() {
		featureDfSchema = featureDf.schema();
		serverFeatureSchema = Preprocessor
				.newSchemaWithSeparatedArrays(featureDfSchema);
	}
	
	private Tuple[] separateArrayFeatures(Tuple tuple) {
		Tuple[] serverTuples = new Tuple[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++) {
			Object[] row = new Object[featureDfSchema.length()];
			
			// Pick out server i's data from arrays
			for (int i = 0; i < featureDfSchema.length(); i++) {
	        	if (featureDfSchema.field(i).type == DataTypes.DoubleArrayType) {
	        		double[] array = (double[]) tuple.get(i);
	        		row[i] = array[serverId];
	        	} else {
	        		row[i] = tuple.get(i);
	        	}
	        }
			
			serverTuples[serverId] = Tuple.of(row, serverFeatureSchema);
		}
		
		return serverTuples;
	}
}
