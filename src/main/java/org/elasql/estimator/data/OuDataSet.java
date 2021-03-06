package org.elasql.estimator.data;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.elasql.estimator.Constants;

import smile.data.DataFrame;

public class OuDataSet {
	
	/**
	 * Read the data set from the given path and separate the data set
	 * for each server.
	 * 
	 * @param rawDataDir
	 * @return
	 */
	public static List<OuDataSet> loadFromRawData(int serverNum, long dataStartTime,
			long dataEndTime, double outlinerStdThreshold, File rawDataDir) {
		List<OuDataSet> dataSets = new ArrayList<OuDataSet>(serverNum);
		
		DataFrame featureDf = loadFeatureFile(rawDataDir, dataStartTime,
				dataEndTime);
		for (int serverId = 0; serverId < serverNum; serverId++) {
			DataFrame labelDf = loadLabelFile(rawDataDir, serverId);
			DataFrame[] dfs = Preprocessor.preprocess(featureDf, labelDf, serverId);
			OuDataSet dataSet = new OuDataSet(dfs[0], dfs[1], outlinerStdThreshold);
			dataSets.add(dataSet);
		}
		
		return dataSets;
	}
	
	private static DataFrame loadFeatureFile(File rawDataDir, long startTime, long endTime) {
		String featureFileName = String.format("%s.csv",
				Constants.FILE_NAME_FEATURE);
		File featureFilePath = new File(rawDataDir, featureFileName);
		
		// Load the features that start time > warm up time
		return CsvLoader.load(featureFilePath.toPath(), tuple -> {
			long txStartTime = tuple.getLong(Constants.FIELD_NAME_START_TIME);
			return txStartTime > startTime && txStartTime < endTime;
		});
	}
	
	private static DataFrame loadLabelFile(File rawDataDir, int serverId) {
		String labelFileName = String.format("%s-%d.csv",
				Constants.FILE_NAME_LATENCY_PREFIX, serverId);
		File labelFilePath = new File(rawDataDir, labelFileName);
		
		// Load the labels that is as a master transaction
		return CsvLoader.load(labelFilePath.toPath(), tuple -> 
			tuple.getBoolean(Constants.FIELD_NAME_IS_MASTER)
		);
	}
	
	private DataFrame features;
	private DataFrame labels;
	private double outlinerStdThreshold;
	
	public OuDataSet(DataFrame features, DataFrame labels, double outlinerStdThreshold) {
		this.features = features;
		this.labels = labels;
		this.outlinerStdThreshold = outlinerStdThreshold;
	}
	
	public OuDataSet[] trainTestSplit(double trainingDataRatio) {
		int trainingDataSize = (int) (features.size() * trainingDataRatio);
		
		DataFrame trainX = features.slice(0, trainingDataSize);
		DataFrame trainY = labels.slice(0, trainingDataSize);
		OuDataSet trainSet = new OuDataSet(trainX, trainY, outlinerStdThreshold);
		
		DataFrame testX = features.slice(trainingDataSize, features.nrows());
		DataFrame testY = labels.slice(trainingDataSize, labels.nrows());
		OuDataSet testSet = new OuDataSet(testX, testY, outlinerStdThreshold);
		
		return new OuDataSet[] {trainSet, testSet};
	}
	
	public DataFrame toTrainingDataFrame(String labelField) {
		// Remove the id field and start time field
		DataFrame df = features.drop(Constants.FIELD_NAME_ID,
				Constants.FIELD_NAME_START_TIME);
		
		// Merge the label column
		DataFrame trainingDf = df.merge(labels.column(labelField));
		
		// Filter outliners
		double mean = labelMean(labelField);
		double std = labelStd(labelField);
		double upperBound = mean + std * outlinerStdThreshold;
		double lowerBound = mean - std * outlinerStdThreshold;
		trainingDf = DataFrame.of(trainingDf.stream().filter(
				row -> row.getDouble(labelField) > lowerBound && 
				row.getDouble(labelField) < upperBound));
		
		return trainingDf;
	}
	
	public OuDataSet union(OuDataSet dataSet) {
		DataFrame newFeatures = features.union(dataSet.features);
		DataFrame newLabels = labels.union(dataSet.labels);
		return new OuDataSet(newFeatures, newLabels, outlinerStdThreshold);
	}
	
	public int size() {
		return features.size();
	}
	
	public DataFrame getFeatures() {
		return features;
	}
	
	public DataFrame getLabels() {
		return labels;
	}
	
	public double[] getLabels(String labelField) {
		return labels.column(labelField).toDoubleArray();
	}
	
	public double labelMean(String labelField) {
		double[] nums = labels.column(labelField).toDoubleArray();
		return mean(nums);
	}
	
	public double labelStd(String labelField) {
		double[] nums = labels.column(labelField).toDoubleArray();
		double mean = mean(nums);
		double sum = 0.0;
		for (double num : nums) {
			sum += (num - mean) * (num - mean);
		}
		return Math.sqrt(sum / nums.length);
	}
	
	private double mean(double[] nums) {
		double sum = 0.0;
		for (double num : nums) {
			sum += num;
		}
		return sum / nums.length;
	}
}
