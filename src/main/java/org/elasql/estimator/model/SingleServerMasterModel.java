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

import org.elasql.estimator.Constants;
import org.elasql.estimator.data.OuDataSet;

import smile.data.DataFrame;
import smile.data.Tuple;
import smile.data.formula.Formula;
import smile.data.type.StructType;
import smile.regression.RandomForest;

public class SingleServerMasterModel implements Serializable {
	private static final long serialVersionUID = 20220412001L;
	
	public static SingleServerMasterModel fit(OuDataSet trainingSet,
			ModelParameters modelParameters) {
		Map<String, RandomForest> models = new HashMap<String, RandomForest>();

		// For each OU
		for (int ouId = 0; ouId < Constants.OU_NAMES.length; ouId++) {
			String ouName = Constants.OU_NAMES[ouId];
			Formula formula = Formula.lhs(ouName);
			DataFrame df = trainingSet.toTrainingDataFrame(ouName);
			RandomForest forest = RandomForest.fit(formula, df,
					modelParameters.treeCount(ouName), df.ncols() / 3,
					modelParameters.maxDepth(ouName), 100, 5, 1.0);
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
	
	SingleServerMasterModel(Map<String, RandomForest> models) {
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
	
	public double[] importance(String ouName) {
		RandomForest model = ouModels.get(ouName);
		return model.importance();
	}
	
	public void saveToFile(File savePath) throws IOException {
		try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(savePath))) {
			out.writeObject(this);
			out.flush();
		}
	}
	
	public StructType schema() {
		return ouModels.values().iterator().next().schema();
	}
}
