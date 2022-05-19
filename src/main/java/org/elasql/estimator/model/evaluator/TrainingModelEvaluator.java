package org.elasql.estimator.model.evaluator;

import org.elasql.estimator.Constants;
import org.elasql.estimator.data.DataSet;
import org.elasql.estimator.model.SingleServerMasterModel;

import smile.data.DataFrame;

public class TrainingModelEvaluator extends ModelEvaluator {
	
	public TrainingModelEvaluator() {
		super("Server ID, OU Name, Train Size, Train Mean, Train STD, Train MAE, Train MRE");
	}
	
	public void evaluateModel(int serverId, DataSet dataSet,
			SingleServerMasterModel model) {
		DataFrame features = dataSet.getFeatures();
		for (String ouName : Constants.OU_NAMES) {
			double[] predictions = model.predict(ouName, features);
			
			reportBuilder.writeRow(String.format("%d, %s, %d, %f, %f, %f, %f",
				serverId,
				ouName,
				features.size(),
				dataSet.labelMean(ouName),
				dataSet.labelStd(ouName),
				calcMeanAbsoluteError(predictions, dataSet.getLabels(ouName)),
				calcMeanRelativeError(predictions, dataSet.getLabels(ouName))
			));
		}
	}
}
