package org.elasql.estimator;

import java.io.File;

import org.elasql.estimator.model.ModelParameters;

import com.moandjiezana.toml.Toml;

public class Config {
	
	private int serverNum;
	private double outlierStdThreshold;
	private long dataStartTime;
	private long dataEndTime;
	private int crossValidationFold;
	private ModelParameters modelParameters;
	
	public static Config load(File file) {
		Toml toml = new Toml().read(file);
		
		Config config = new Config();
		config.serverNum = toml.getTable("global").getLong("server_num").intValue();
		config.outlierStdThreshold = toml.getTable("training").getDouble("outlier_std_threshold").doubleValue();
		config.dataStartTime = toml.getTable("preprocessor").getLong("warmup_time").longValue();
		config.dataEndTime = toml.getTable("preprocessor").getLong("end_time").longValue();
		config.crossValidationFold = toml.getTable("training").getLong("cross_validation_fold").intValue();
		
		ModelParameters.Builder mpBuilder = new ModelParameters.Builder();
		for (Toml table : toml.getTable("global").getTables("model_parameters")) {
			mpBuilder.addParameters(
					table.getString("ouname"),
					table.getLong("tree_count").intValue(),
					table.getLong("max_depth").intValue()
			);
		}
		config.modelParameters = mpBuilder.build();
		
		return config;
	}
	
	public int serverNum() {
		return serverNum;
	}
	
	public double outlierStdThreshold() {
		return outlierStdThreshold;
	}
	
	public long dataStartTime() {
		return dataStartTime;
	}
	
	public long dataEndTime() {
		return dataEndTime;
	}
	
	public int crossValidationFold() {
		return crossValidationFold;
	}
	
	public ModelParameters modelParameters() {
		return modelParameters;
	}
}
