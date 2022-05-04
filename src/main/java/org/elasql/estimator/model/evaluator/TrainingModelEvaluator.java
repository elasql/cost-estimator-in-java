package org.elasql.estimator.model.evaluator;

import java.io.File;
import java.io.IOException;

import org.elasql.estimator.Constants;
import org.elasql.estimator.data.DataSet;
import org.elasql.estimator.model.SingleServerMasterModel;

import smile.data.DataFrame;

public class TrainingModelEvaluator extends ModelEvaluator {
	
	public TrainingModelEvaluator() {
		super("Server ID, OU Name, Train Size, Train Mean, Train STD, Train MAE, Train MRE, "
				+ "Test Size, Test Mean, Test STD, Test MAE, Test MRE");
	}
	
	public void evaluateModel(int serverId, DataSet trainSet, DataSet testSet,
			SingleServerMasterModel model) {
		DataFrame trainFeatures = trainSet.getFeatures();
		DataFrame testFeatures = testSet.getFeatures();
		for (String ouName : Constants.OU_NAMES) {
			double[] trainPredictions = model.predict(ouName, trainFeatures);
			double[] testPredictions = model.predict(ouName, testFeatures);
			
			reportBuilder.writeRow(String.format("%d, %s, %d, %f, %f, %f, %f, %d, %f, %f, %f, %f",
				serverId,
				ouName,
				trainFeatures.size(),
				trainSet.labelMean(ouName),
				trainSet.labelStd(ouName),
				calcMeanAbsoluteError(trainPredictions, trainSet.getLabels(ouName)),
				calcMeanRelativeError(trainPredictions, trainSet.getLabels(ouName)),
				testFeatures.size(),
				testSet.labelMean(ouName),
				testSet.labelStd(ouName),
				calcMeanAbsoluteError(testPredictions, testSet.getLabels(ouName)),
				calcMeanRelativeError(testPredictions, testSet.getLabels(ouName))
			));
		}
	}
}
