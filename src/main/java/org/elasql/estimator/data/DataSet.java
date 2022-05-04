package org.elasql.estimator.data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasql.estimator.Config;
import org.elasql.estimator.Constants;
import org.elasql.estimator.NewConstants;

import smile.data.DataFrame;

public class DataSet {

	public static void main(String[] args) throws IOException {
		File dataDir = new File("R:\\Research\\2021-Hermes-Control\\java-workspace\\estimator\\test\\training-data");
		File configFile = new File("R:\\Research\\2021-Hermes-Control\\java-workspace\\estimator\\test\\config.toml");
		Config config = Config.load(configFile);
		loadFromRawData(config, dataDir);
	}

	/**
	 * Read the data set from the given path and separate the data set
	 * for each server.
	 * 
	 * @param rawDataDir
	 * @return
	 */
	public static List<DataSet> loadFromRawData(Config config, File rawDataDir) {
		List<DataSet> dataSets = new ArrayList<DataSet>(config.serverNum());
		
		DataFrame featureDf = loadFeatureFile(rawDataDir, config.warmUpEndTime());
		for (int serverId = 0; serverId < config.serverNum(); serverId++) {
			DataFrame labelDf = loadLabelFile(rawDataDir, serverId);
			DataFrame[] dfs = Preprocessor.preprocess(featureDf, labelDf, serverId);
			DataSet dataSet = new DataSet(dfs[0], dfs[1],
					config.outlinerStdThreshold());
			dataSets.add(dataSet);
		}
		
		return dataSets;
	}
	
	private static DataFrame loadFeatureFile(File rawDataDir, long warmUpEndTime) {
		String featureFileName = String.format("%s.csv",
				NewConstants.FILE_NAME_FEATURE);
		File featureFilePath = new File(rawDataDir, featureFileName);
		
		// Load the features that start time > warm up time
		return CsvLoader.load(featureFilePath.toPath(), tuple -> {
			long startTime = tuple.getLong(NewConstants.FIELD_NAME_START_TIME);
			return startTime > warmUpEndTime;
		});
	}
	
	private static DataFrame loadLabelFile(File rawDataDir, int serverId) {
		String labelFileName = String.format("%s-%d.csv",
				NewConstants.FILE_NAME_LATENCY_PREFIX, serverId);
		File labelFilePath = new File(rawDataDir, labelFileName);
		
		// Load the labels that is as a master transaction
		return CsvLoader.load(labelFilePath.toPath(), tuple -> 
			tuple.getBoolean(NewConstants.FIELD_NAME_IS_MASTER)
		);
	}
	
	private DataFrame features;
	private DataFrame labels;
	private double outlinerStdThreshold;
	
	public DataSet(DataFrame features, DataFrame labels, double outlinerStdThreshold) {
		this.features = features;
		this.labels = labels;
		this.outlinerStdThreshold = outlinerStdThreshold;
	}
	
	public DataSet[] trainTestSplit(double trainingDataRatio) {
		int trainingDataSize = (int) (features.size() * trainingDataRatio);
		
		DataFrame trainX = features.slice(0, trainingDataSize);
		DataFrame trainY = labels.slice(0, trainingDataSize);
		DataSet trainSet = new DataSet(trainX, trainY, outlinerStdThreshold);
		
		DataFrame testX = features.slice(trainingDataSize, features.nrows());
		DataFrame testY = labels.slice(trainingDataSize, labels.nrows());
		DataSet testSet = new DataSet(testX, testY, outlinerStdThreshold);
		
		return new DataSet[] {trainSet, testSet};
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
				row -> row.getDouble(labelField) > lowerBound && 
				row.getDouble(labelField) < upperBound));
		
		return trainingDf;
	}
	
	public DataFrame getFeatures() {
		return features;
	}
	
	public double[] getLabelVectorInDouble(String labelField) {
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
