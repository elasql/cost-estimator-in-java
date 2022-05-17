package org.elasql.estimator.model;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.estimator.Constants;
import org.elasql.estimator.data.DataSet;
import org.elasql.estimator.utils.ReportBuilder;

import smile.data.DataFrame;
import smile.data.formula.Formula;
import smile.regression.RandomForest;
import smile.validation.CrossValidation;
import smile.validation.RegressionValidations;

public class ModelTrainer {
	private static Logger logger = Logger.getLogger(ModelTrainer.class.getName());
	
	private static final String REPORT_HEADER = "OU Name, Tree Count, Max Depth, CV Mean Fit Time, CV Mean MSE";
	
	private static final int MAX_TREE_COUNT = 256;
	private static final int MAX_DEPTH = 32;

	private int foldCountForCv;
	private ReportBuilder reportBuilder;
	
	public ModelTrainer(int foldCountForCv) {
		this.foldCountForCv = foldCountForCv;
	}
	
	public SingleServerMasterModel trainWithGridSearch(DataSet trainingSet) {
		// Create a new training report
		newReport();
		
		Map<String, RandomForest> models = new HashMap<String, RandomForest>();

		// For each OU
		for (int ouId = 0; ouId < Constants.OU_NAMES.length; ouId++) {
			String ouName = Constants.OU_NAMES[ouId];
			RandomForest forest = trainWithGridSearch(ouName, trainingSet);
			models.put(ouName, forest);
		}
		
		return new SingleServerMasterModel(models);
	}
	
	private RandomForest trainWithGridSearch(String ouName, DataSet trainingSet) {
		Formula formula = Formula.lhs(ouName);
		DataFrame df = trainingSet.toTrainingDataFrame(ouName);
		
		double minMse = Double.MAX_VALUE;
		int bestTreeCount = 0;
		int bestMaxDepth = 0;
		
		if (logger.isLoggable(Level.INFO))
			logger.info(String.format("Grid searching a random forest for '%s'...", ouName));
		
		// Grid search
		for (int treeCount = 1; treeCount <= MAX_TREE_COUNT; treeCount *= 2) {
			for (int maxDepth = 1; maxDepth <= MAX_DEPTH; maxDepth *= 2) {
				double mse = crossValidation(ouName, formula, df,
						treeCount, maxDepth);
				
				if (mse < minMse) {
					minMse = mse;
					bestTreeCount = treeCount;
					bestMaxDepth = maxDepth;
				}
			}
		}
		
		if (logger.isLoggable(Level.INFO))
			logger.info(String.format(
					"Found the best parameters for '%s' are (tree count = %d, max depth = %d)",
					ouName, bestTreeCount, bestMaxDepth));
		
		// Train a model with the best parameters
		return RandomForest.fit(Formula.lhs(ouName), df,
				bestTreeCount, df.ncols() / 3, bestMaxDepth, 100, 5, 1.0);
	}
	
	private double crossValidation(String ouName,
			Formula formula, DataFrame df, int treeCount, int maxDepth) {
		RegressionValidations<RandomForest> rv = CrossValidation.regression(
			foldCountForCv, formula, df, (f, d) -> RandomForest.fit(f, d)
		);
		recordResult(ouName, treeCount, maxDepth, rv.avg.fitTime, rv.avg.mse);
		return rv.avg.mse;
	}
	
	private void newReport() {
		reportBuilder = new ReportBuilder(REPORT_HEADER);
	}
	
	private void recordResult(String ouName, int treeCount, int maxDepth, double fitTime, double mse) {
		reportBuilder.writeRow(String.format("%s, %d, %d, %f, %f",
				ouName, treeCount, maxDepth, fitTime, mse));
	}
	
	public void generateTrainingReport(File reportPath) {
		try {
			reportBuilder.writeToFile(reportPath);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
