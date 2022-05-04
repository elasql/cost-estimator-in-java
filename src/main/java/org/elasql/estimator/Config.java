package org.elasql.estimator;

import java.io.File;

import com.moandjiezana.toml.Toml;

public class Config {
	
	private int serverNum;
	private double trainingDataRatio;
	private double outlinerStdThreshold;
	private long warmUpEndTime;
	
	public static Config load(File file) {
		Toml toml = new Toml().read(file);
		
		Config config = new Config();
		config.serverNum = toml.getTable("global").getLong("server_num").intValue();
		config.trainingDataRatio = toml.getTable("global").getDouble("training_data_ratio").doubleValue();
		config.outlinerStdThreshold = toml.getTable("global").getDouble("outlier_std_threshold").doubleValue();
		config.warmUpEndTime = toml.getTable("global").getLong("warm_up_end_time").longValue();
		
		return config;
	}
	
	public int serverNum() {
		return serverNum;
	}
	
	public double trainingDataRatio() {
		return trainingDataRatio;
	}
	
	public double outlinerStdThreshold() {
		return outlinerStdThreshold;
	}
	
	public long warmUpEndTime() {
		return warmUpEndTime;
	}
}
