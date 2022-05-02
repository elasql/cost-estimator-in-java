package org.elasql.estimator;

import java.io.File;

import com.moandjiezana.toml.Toml;

public class Config {
	
	private int serverNum;
	private int trainingDataSize;
	private double outlinerStdThreshold;
	private long warmUpEndTime;
	
	public static Config load(File file) {
		Toml toml = new Toml().read(file);
		
		Config config = new Config();
		config.serverNum = toml.getTable("global").getLong("server_num").intValue();
		config.trainingDataSize = toml.getTable("training").getLong("training_data_size").intValue();
		config.outlinerStdThreshold = toml.getTable("training").getDouble("outlier_std_threshold").doubleValue();
		config.warmUpEndTime = toml.getTable("training").getLong("warm_up_end_time").longValue();
		
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
	
	public long warmUpEndTime() {
		return warmUpEndTime;
	}
}
