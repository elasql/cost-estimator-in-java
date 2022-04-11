package org.elasql.estimator.model;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.estimator.Constants;
import org.elasql.estimator.data.DataSet;

import smile.data.DataFrame;
import smile.data.formula.Formula;
import smile.regression.RandomForest;
import smile.validation.RegressionMetrics;

public class SingleServerMasterModel {
	private static Logger logger = Logger.getLogger(SingleServerMasterModel.class.getName());
	
	public static SingleServerMasterModel fit(DataSet trainingSet) {
		Map<String, RandomForest> models = new HashMap<String, RandomForest>();
		
		for (int ouId = 0; ouId < Constants.OU_NAMES.length; ouId++) {
			String ouName = Constants.OU_NAMES[ouId];
			
			if (logger.isLoggable(Level.INFO))
				logger.info(String.format("Training a random forest for '%s'...", ouName));
			
			DataFrame df = trainingSet.toTrainingDataFrame(ouName);
			RandomForest forest = RandomForest.fit(Formula.lhs(ouName), df,
					200, df.ncols() / 3, 2, 100, 5, 1.0);
			
			if (logger.isLoggable(Level.INFO)) {
				RegressionMetrics metrics = forest.metrics();
				logger.info(String.format("Training completed. (took %f milliseconds, RMSE: %f)",
						metrics.fitTime, metrics.rmse));
			}
			
			models.put(ouName, forest);
		}
		
		return new SingleServerMasterModel(models);
	}
	
	private Map<String, RandomForest> ouModels;
	
	private SingleServerMasterModel(Map<String, RandomForest> models) {
		this.ouModels = models;
	}
	
	public double testMeanAbsoluteError(String ouName, DataFrame testFeatures, double[] testLabels) {
		RandomForest model = ouModels.get(ouName);
		double[] predictions = model.predict(testFeatures);
		
		double sum = 0.0;
		for (int i = 0; i < predictions.length; i++) {
			sum += Math.abs(predictions[i] - testLabels[i]);
		}
		
		return sum / predictions.length;
	}
}
