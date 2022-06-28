package org.elasql.estimator.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import smile.data.Tuple;
import smile.data.type.StructType;

/**
 * Implements the sum-max inference model.
 * 
 * @author Pin-Yu Wang, Yu-Shan Lin
 */
public class SumMaxSequentialModel {
	
	private List<SingleServerMasterModel> serverOuModels;
	private int serverCount;
	private Map<Long, Double> prevEndTimePredictions;
	
	// For the last predicted transaction
	private long lastTxNum;
	private double[] lastTxnEndTimePredictions; // for each server
	private boolean lastRouteDecided = true;
	
	public SumMaxSequentialModel(List<SingleServerMasterModel> models) {
		serverOuModels = models;
		serverCount = models.size();
		prevEndTimePredictions = new HashMap<Long, Double>();
		lastTxNum = 0;
		lastTxnEndTimePredictions = new double[serverCount];
	}
	
	public double predictOuLatency(String ouName, int serverId, Tuple features) {
		return serverOuModels.get(serverId).predict(ouName, features);
	}
	
	public double[] predictNextTxnLatency(long txNum, List<Long> dependentTxns, 
			long txnStartTime, Tuple[] serverFeatures) {
		if (txNum <= lastTxNum)
			throw new RuntimeException("Transaction features should be fed in the increasing order"
					+ " (last tx: " + lastTxNum + ", current tx: " + txNum + ")");
		if (!lastRouteDecided)
			throw new RuntimeException("The route for the tx." + lastTxNum + " have not been decided yet");
		
		double[] latencyPredictions = new double[serverCount];
		
		double dependentEndTime = maxOverDependentTxns(dependentTxns);
		
		for (int serverId = 0; serverId < serverCount; serverId++) {
			Tuple features = serverFeatures[serverId];
			double ou3EndTime = txnStartTime + predictLatencyTilOu3(serverId, features);
			ou3EndTime = Math.max(dependentEndTime, ou3EndTime);
			double totalEndTime = ou3EndTime + predictLatencyAfterOu3(serverId, features);

			lastTxnEndTimePredictions[serverId] = totalEndTime;
			latencyPredictions[serverId] = totalEndTime - txnStartTime;
		}
		lastTxNum = txNum;
		lastRouteDecided = false;
		
		return latencyPredictions;
	}
	
	public void decideLastTxnDest(long txNum, int routeDest) {
		if (lastTxNum != txNum) {
			throw new RuntimeException(String.format(
					"expect to get the route for tx.%d, but we got tx.%d's route.",
					lastTxNum, txNum));
		}
		
		prevEndTimePredictions.put(lastTxNum, lastTxnEndTimePredictions[routeDest]);
		lastRouteDecided = true;
	}
	
	public StructType schema() {
		return serverOuModels.get(0).schema();
	}
	
	private double predictLatencyTilOu3(int serverId, Tuple features) {
		SingleServerMasterModel serverModel = serverOuModels.get(serverId);
		double ou0b = serverModel.predict("OU0 - Broadcast", features);
		double ou0r = serverModel.predict("OU0 - ROUTE", features);
		double ou1 = serverModel.predict("OU1 - Generate Plan", features);
		double ou2 = serverModel.predict("OU2 - Initialize Thread", features);
		return ou0b + ou0r + ou1 + ou2;
	}
	
	private double maxOverDependentTxns(List<Long> dependentTxns) {
		double maxEndTime = 0.0;
		for (Long dependentTxn : dependentTxns) {
			Double prevEndTime = prevEndTimePredictions.get(dependentTxn);
			if (prevEndTime == null) {
				throw new RuntimeException("There is no record for " + dependentTxn);
			}
			maxEndTime = Math.max(maxEndTime, prevEndTime.doubleValue());
		}
		return maxEndTime;
	}
	
	private double predictLatencyAfterOu3(int serverId, Tuple features) {
		SingleServerMasterModel serverModel = serverOuModels.get(serverId);
		double ou4 = serverModel.predict("OU4 - Read from Local", features);
		double ou5m = serverModel.predict("OU5M - Read from Remote", features);
		double ou6 = serverModel.predict("OU6 - Execute Arithmetic Logic", features);
		double ou7 = serverModel.predict("OU7 - Write to Local", features);
		double ou8 = serverModel.predict("OU8 - Commit", features);
		return ou4 + ou5m + ou6 + ou7 + ou8;
	}
}
