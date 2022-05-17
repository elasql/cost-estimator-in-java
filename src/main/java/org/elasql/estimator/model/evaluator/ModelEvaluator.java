package org.elasql.estimator.model.evaluator;

import java.io.File;
import java.io.IOException;

import org.elasql.estimator.utils.ReportBuilder;

public abstract class ModelEvaluator {
	
	protected ReportBuilder reportBuilder;
	
	protected ModelEvaluator(String reportHeader) {
		reportBuilder = new ReportBuilder(reportHeader);
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
}
