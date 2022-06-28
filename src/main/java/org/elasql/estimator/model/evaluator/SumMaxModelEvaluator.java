package org.elasql.estimator.model.evaluator;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.estimator.data.TotalLatencyDataSet;
import org.elasql.estimator.model.SumMaxSequentialModel;

import smile.data.Tuple;

public class SumMaxModelEvaluator extends ModelEvaluator {
	private static Logger logger = Logger.getLogger(SumMaxModelEvaluator.class.getName());
	
	public static SumMaxModelEvaluator newWithServerNumber(int serverNum) {
		StringBuilder sb = new StringBuilder();
		
		sb.append("TxNum, Route, True Latency, Target Server Prediction");
		for (int serverId = 0; serverId < serverNum; serverId++) {
			sb.append(", Server " + serverId + " Prediction");
		}
		
		return new SumMaxModelEvaluator(sb.toString());
	}
	
	public SumMaxModelEvaluator(String header) {
		super(header);
	}
	
	public void evaluateModel(TotalLatencyDataSet dataSet,
			SumMaxSequentialModel model) {
		long testedCount = 0;
		
		for (long txNum = dataSet.getStartTxNum(); txNum <= dataSet.getEndTxNum(); txNum++) {
			Long boxedTxNum = txNum;
			Tuple[] features = dataSet.getFeature(boxedTxNum);
			if (features == null) {
				if (logger.isLoggable(Level.WARNING)) {
					logger.warning(String.format("No feature data for transaction %d.",
							txNum));
					continue;
				}
			}
			
			Long startTime = dataSet.getStartTime(boxedTxNum);
			List<Long> dependencies = dataSet.getDependencies(boxedTxNum);
			Integer route = dataSet.getRoute(boxedTxNum);
			
			if (route == null) {
				if (logger.isLoggable(Level.WARNING)) {
					logger.warning(String.format("No latency data for transaction %d.",
							txNum));
					continue;
				}
			}
			
			double[] sumMaxPrediction = model.predictNextTxnLatency(
					txNum, dependencies, startTime, features);
			model.decideLastTxnDest(txNum, route);
			Double trueLatency = dataSet.getLatency(boxedTxNum);
			
			writeRow(txNum, route, trueLatency, sumMaxPrediction);
			
			testedCount++;
			if (testedCount % 100000 == 0) {
				if (logger.isLoggable(Level.WARNING)) {
					logger.warning(String.format("%d transactions are tested.",
							testedCount));
				}
			}
		}
	}
	
	private void writeRow(long txNum, int route, double trueLatency, double[] predictions) {
		StringBuilder sb = new StringBuilder();
		
		sb.append(String.format("%d, %d, %f, %f", txNum, route, trueLatency, predictions[route]));
		
		for (double prediction : predictions) {
			sb.append(String.format(", %f", prediction));
		}
		
		reportBuilder.writeRow(sb.toString());
	}
}
