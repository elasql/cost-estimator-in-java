package org.elasql.estimator;

import java.io.File;

import org.elasql.estimator.model.ModelParameters;

import com.moandjiezana.toml.Toml;

public class Config {
	
	private int serverNum;
	private double outlinerStdThreshold;
	private long warmUpEndTime;
	private int crossValidationFold;
	private ModelParameters modelParameters;
	
	public static Config load(File file) {
		Toml toml = new Toml().read(file);
		
		Config config = new Config();
		config.serverNum = toml.getTable("global").getLong("server_num").intValue();
		config.outlinerStdThreshold = toml.getTable("global").getDouble("outlier_std_threshold").doubleValue();
		config.warmUpEndTime = toml.getTable("global").getLong("warm_up_end_time").longValue();
		config.crossValidationFold = toml.getTable("global").getLong("cross_validation_fold").intValue();
		
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
	
	public double outlinerStdThreshold() {
		return outlinerStdThreshold;
	}
	
	public long warmUpEndTime() {
		return warmUpEndTime;
	}
	
	public int crossValidationFold() {
		return crossValidationFold;
	}
	
	public ModelParameters modelParameters() {
		return modelParameters;
	}
}
