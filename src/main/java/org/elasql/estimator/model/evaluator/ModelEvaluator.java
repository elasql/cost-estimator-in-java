package org.elasql.estimator.model.evaluator;

import java.io.File;
import java.io.IOException;

import org.elasql.estimator.Constants;
import org.elasql.estimator.data.DataSet;
import org.elasql.estimator.model.SingleServerMasterModel;
import org.elasql.estimator.utils.ReportBuilder;

import smile.data.DataFrame;
import smile.data.type.StructField;
import smile.data.type.StructType;

public class ModelEvaluator {
	
	protected ReportBuilder reportBuilder;
	
	public ModelEvaluator(StructType schema) {
		StringBuilder sb = new StringBuilder();
		
		sb.append("Server ID, OU Name, Data Set Size, Mean, STD, MAE, MRE");
		for (StructField field : schema.fields()) {
			sb.append("," + field.name);
		}
		
		reportBuilder = new ReportBuilder(sb.toString());
	}
	
	protected double calcMeanAbsoluteError(double[] predictions, double[] labels) {
		double sum = 0.0;
		for (int i = 0; i < predictions.length; i++) {
			sum += Math.abs(predictions[i] - labels[i]);
		}
		return sum / predictions.length;
	}
	
	protected double calcMeanRelativeError(double[] predictions, double[] labels) {
		double sum = 0.0;
		for (int i = 0; i < predictions.length; i++) {
			double diff = Math.abs(predictions[i] - labels[i]);
			double errorRate = diff / labels[i];
			
			// In some cases, labels[i] might be 0
			if (!Double.isInfinite(errorRate))
				sum += errorRate;
		}
		return sum / predictions.length;
	}
	
	public void generateReport(File reportPath) {
		try {
			reportBuilder.writeToFile(reportPath);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void evaluateModel(int serverId, DataSet dataSet,
			SingleServerMasterModel model) {
		DataFrame features = dataSet.getFeatures();
		for (String ouName : Constants.OU_NAMES) {
			double[] predictions = model.predict(ouName, features);
			
			StringBuilder sb = new StringBuilder();
			
			sb.append(String.format("%d, %s, %d, %f, %f, %f, %f",
				serverId,
				ouName,
				features.size(),
				dataSet.labelMean(ouName),
				dataSet.labelStd(ouName),
				calcMeanAbsoluteError(predictions, dataSet.getLabels(ouName)),
				calcMeanRelativeError(predictions, dataSet.getLabels(ouName))
			));
			
			for (double importance : model.importance(ouName))
				sb.append(String.format(", %f", importance));
			
			reportBuilder.writeRow(sb.toString());
		}
	}
}
