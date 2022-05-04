package org.elasql.estimator.model;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.estimator.Constants;
import org.elasql.estimator.data.DataSet;

import smile.data.DataFrame;
import smile.data.Tuple;
import smile.data.formula.Formula;
import smile.regression.RandomForest;
import smile.validation.RegressionMetrics;

public class SingleServerMasterModel implements Serializable {
	private static Logger logger = Logger.getLogger(SingleServerMasterModel.class.getName());
	
	private static final long serialVersionUID = 20220412001L;
	
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
	
	public static SingleServerMasterModel loadFromFile(File modelFilePath) throws IOException, ClassNotFoundException {
		SingleServerMasterModel model = null;
		try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(modelFilePath))) {
			model = (SingleServerMasterModel) in.readObject();
		}
		return model;
	}
	
	private Map<String, RandomForest> ouModels;
	
	private SingleServerMasterModel(Map<String, RandomForest> models) {
		this.ouModels = models;
	}
	
	public double predict(String ouName, Tuple features) {
		RandomForest model = ouModels.get(ouName);
		return model.predict(features);
	}
	
	public double[] predict(String ouName, DataFrame features) {
		RandomForest model = ouModels.get(ouName);
		return model.predict(features);
	}
	
	public void saveToFile(File savePath) throws IOException {
		try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(savePath))) {
			out.writeObject(this);
			out.flush();
		}
	}
}
