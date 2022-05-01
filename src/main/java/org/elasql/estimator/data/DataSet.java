package org.elasql.estimator.data;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.elasql.estimator.Config;
import org.elasql.estimator.Constants;
import org.elasql.estimator.NewConstants;

import smile.data.DataFrame;

public class DataSet {

	/**
	 * Read the data set from the given path and separate the data set
	 * for each server.
	 * 
	 * @param rawDataDir
	 * @return
	 */
	public static List<DataSet> loadFromRawData(Config config, File rawDataDir) {
		DataFrame featureDf = loadFeatureFile(rawDataDir);
		for (int serverId = 0; serverId < config.serverNum(); serverId++) {
			DataFrame labelDf = loadLabelFile(rawDataDir, serverId);
		}
		
//		List<DataSet> dataSets = new ArrayList<DataSet>(config.serverNum());
//		
//		for (int serverId = 0; serverId < config.serverNum(); serverId++) {
//			DataFrame featureDataFrame = loadFeatureFile(rawDataDir, serverId);
//			DataFrame labelDataFrame = loadLabelFile(rawDataDir, serverId);
//			DataSet dataSet = new DataSet(featureDataFrame, labelDataFrame,
//					config.outlinerStdThreshold());
//			dataSets.add(dataSet);
//		}
//		
//		return dataSets;
	}
	
	private static DataFrame loadFeatureFile(File rawDataDir) {
		File featureFilePath = new File(rawDataDir, NewConstants.FILE_NAME_FEATURE);
		return CsvLoader.load(featureFilePath.toPath());
	}
	
	private static DataFrame loadLabelFile(File rawDataDir, int serverId) {
		String labelFileName = String.format("%s-%d.csv",
				NewConstants.FILE_NAME_LATENCY_PREFIX, serverId);
		File labelFilePath = new File(rawDataDir, labelFileName);
		return CsvLoader.load(labelFilePath.toPath());
	}
	
	private DataFrame features;
	private DataFrame labels;
	private double outlinerStdThreshold;
	
	public DataSet(DataFrame features, DataFrame labels, double outlinerStdThreshold) {
		this.features = features;
		this.labels = labels;
		this.outlinerStdThreshold = outlinerStdThreshold;
	}
	
	public List<DataSet> trainTestSplit(int trainingDataSize) {
		List<DataSet> dataSets = new ArrayList<DataSet>();
		
		DataFrame trainX = features.slice(0, trainingDataSize);
		DataFrame trainY = labels.slice(0, trainingDataSize);
		dataSets.add(new DataSet(trainX, trainY, outlinerStdThreshold));
		
		DataFrame testX = features.slice(trainingDataSize, features.nrows());
		DataFrame testY = labels.slice(trainingDataSize, labels.nrows());
		dataSets.add(new DataSet(testX, testY, outlinerStdThreshold));
		
		return dataSets;
	}
	
	public DataFrame toTrainingDataFrame(String labelField) {
		// Remove the id field
		DataFrame df = features.drop(Constants.ID_FIELD_NAME);
		
		// Merge the label column
		DataFrame trainingDf = df.merge(labels.column(labelField));
		
		// Filter outliners
		double mean = labelMean(labelField);
		double std = labelStd(labelField);
		double upperBound = mean + std * outlinerStdThreshold;
		double lowerBound = mean - std * outlinerStdThreshold;
		trainingDf = DataFrame.of(trainingDf.stream().filter(
				row -> row.getInt(labelField) > lowerBound && 
				row.getInt(labelField) < upperBound));
		
		return trainingDf;
	}
	
	public DataFrame getFeatures() {
		return features;
	}
	
	public double[] getLabelVectorInDouble(String labelField) {
		return labels.column(labelField).toDoubleArray();
	}
	
	public double labelMean(String labelField) {
		int[] nums = labels.column(labelField).toIntArray();
		return mean(nums);
	}
	
	public double labelStd(String labelField) {
		int[] nums = labels.column(labelField).toIntArray();
		double mean = mean(nums);
		double sum = 0.0;
		for (int num : nums) {
			sum += (num - mean) * (num - mean);
		}
		return Math.sqrt(sum / nums.length);
	}
	
	private double mean(int[] nums) {
		double sum = 0.0;
		for (int num : nums) {
			sum += num;
		}
		return sum / nums.length;
	}
}
