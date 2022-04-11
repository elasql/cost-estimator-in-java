package org.elasql.estimator.data;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.elasql.estimator.Config;

import smile.data.DataFrame;
import smile.io.CSV;

public class DataSet {

	/**
	 * Read the data set from the given path and separate the data set
	 * for each server.
	 * 
	 * @param dataSetDir
	 * @return
	 */
	public static List<DataSet> load(Config config, File dataSetDir) {
		List<DataSet> dataSets = new ArrayList<DataSet>(config.serverNum());
		
		for (int serverId = 0; serverId < config.serverNum(); serverId++) {
			DataFrame featureDataFrame = loadFeatureFile(dataSetDir, serverId);
			DataFrame labelDataFrame = loadLabelFile(dataSetDir, serverId);
			DataSet dataSet = new DataSet(featureDataFrame, labelDataFrame,
					config.outlinerStdThreshold());
			dataSets.add(dataSet);
		}
		
		return dataSets;
	}
	
	private static DataFrame loadFeatureFile(File dataSetDir, int serverId) {
		String featureFileName = String.format("server-%d-features.csv", serverId);
		File featureFilePath = new File(dataSetDir, featureFileName);
		return loadCsvAsDataFrame(featureFilePath.toPath());
	}
	
	private static DataFrame loadLabelFile(File dataSetDir, int serverId) {
		String labelFileName = String.format("server-%d-labels.csv", serverId);
		File labelFilePath = new File(dataSetDir, labelFileName);
		return loadCsvAsDataFrame(labelFilePath.toPath());
	}
	
	private static DataFrame loadCsvAsDataFrame(Path path) {
		DataFrame df = null;
		try {
			CSVFormat.Builder builder = CSVFormat.Builder.create();
			builder.setHeader(); // Make it infer the header names from the first row
			CSV csv = new CSV(builder.build());
			df = csv.read(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return df;
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
		// Remove Transaction ID
		DataFrame df = features.drop("Transaction ID");
		
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
