package org.elasql.estimator.model.evaluator;

import org.elasql.estimator.Constants;
import org.elasql.estimator.data.OuDataSet;
import org.elasql.estimator.model.SingleServerMasterModel;

import smile.data.DataFrame;
import smile.data.type.StructField;
import smile.data.type.StructType;

public class OuModelEvaluator extends ModelEvaluator {
	
	public static OuModelEvaluator newWithFeatureSchema(StructType schema) {
		StringBuilder sb = new StringBuilder();
		
		sb.append("Server ID, OU Name, Data Set Size, Mean, STD, MAE, MRE");
		for (StructField field : schema.fields()) {
			sb.append("," + field.name);
		}
		
		return new OuModelEvaluator(sb.toString());
	}
	
	protected OuModelEvaluator(String header) {
		super(header);
	}
	
	public void evaluateModel(int serverId, OuDataSet dataSet,
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
