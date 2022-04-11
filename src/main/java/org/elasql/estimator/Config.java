package org.elasql.estimator;

import java.io.File;

import com.moandjiezana.toml.Toml;

public class Config {
	
	private int serverNum;
	private int trainingDataSize;
	private double outlinerStdThreshold;
	
	public static Config load(File file) {
		Toml toml = new Toml().read(file);
		
		Config config = new Config();
		config.serverNum = toml.getTable("global").getLong("server_num").intValue();
		config.trainingDataSize = toml.getTable("training").getLong("training_data_size").intValue();
		config.outlinerStdThreshold = toml.getTable("training").getDouble("outlier_std_threshold").doubleValue();
		
		return config;
	}
	
	public int serverNum() {
		return serverNum;
	}
	
	public int trainingDataSize() {
		return trainingDataSize;
	}
	
	public double outlinerStdThreshold() {
		return outlinerStdThreshold;
	}
}
