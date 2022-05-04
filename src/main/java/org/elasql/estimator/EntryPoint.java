package org.elasql.estimator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.estimator.data.DataSet;
import org.elasql.estimator.model.SingleServerMasterModel;
import org.elasql.estimator.model.evaluator.TrainingModelEvaluator;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "estimator", mixinStandardHelpOptions = true, version = "ElaSQL Estimator 0.1",
		 description = "A cost estimator for ElaSQL transactions")
public class EntryPoint {
	private static Logger logger = Logger.getLogger(EntryPoint.class.getName());
	
	public static void main(String[] args) {
		int exitCode = new CommandLine(new EntryPoint())
				.setSubcommandsCaseInsensitive(true)
				.execute(args);
		System.exit(exitCode);
	}
	
	@Option(names = {"-c", "--config"}, required = false, defaultValue = "config.toml")
	File configFile;
	
	@Command(name = "train", mixinStandardHelpOptions = true)
	public int train(
			@Parameters(paramLabel = "DATA_SET_DIR", description = "path to the data set") File dataSetDir,
			@Parameters(paramLabel = "MODEL_SAVE_DIR", description = "path to save the model") File modelSaveDir
		) {
		
		// Ensure that the output directory exists
		try {
			Files.createDirectories(modelSaveDir.toPath());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		// Load the configurations
		Config config = Config.load(configFile);
		
		// Load the data set
		if (logger.isLoggable(Level.INFO))
			logger.info("Loading and pre-processing data set...");
		
		List<DataSet> dataSets = DataSet.loadFromRawData(config, dataSetDir);
		
		if (logger.isLoggable(Level.INFO))
			logger.info("All data are loaded and processed.");
		
		// For each server
		TrainingModelEvaluator evaluator = new TrainingModelEvaluator();
		for (int serverId = 0; serverId < dataSets.size(); serverId++) {
			
			// Split the data set to training set and testing set
			DataSet dataSet = dataSets.get(serverId);
			DataSet[] trainTestSets = dataSet.trainTestSplit(config.trainingDataRatio());
			DataSet trainSet = trainTestSets[0];
			DataSet testSet = trainTestSets[1];
			
			// Train a master model for each server
			if (logger.isLoggable(Level.INFO))
				logger.info("Training models for server #" + serverId + "...");
			
			SingleServerMasterModel model = SingleServerMasterModel.fit(trainSet);
			
			if (logger.isLoggable(Level.INFO))
				logger.info("Training models for server #" + serverId + " completed");
			
			// Evaluate the model
			evaluator.evaluateModel(serverId, trainSet, testSet, model);
			
			// Save the model
			try {
				File modelFilePath = new File(modelSaveDir, "model-" + serverId + ".bin");
				model.saveToFile(modelFilePath);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		
		// Save the report
		evaluator.generateReport(new File("training-report.csv"));
		
		return 0;
	}
	
//	@Command(name = "test", mixinStandardHelpOptions = true)
//	public int test(
//			@Parameters(paramLabel = "DATA_SET_DIR", description = "path to the testing data set") File dataSetDir,
//			@Parameters(paramLabel = "MODEL_DIR", description = "path to the saved models") File modelDir
//		) {
//		
//		// Load the configurations
//		Config config = Config.load(configFile);
//		
//		if (logger.isLoggable(Level.INFO))
//			logger.info("Loading the data set and the models...");
//
//		// Load the data set
//		List<DataSet> dataSets = DataSet.loadFromRawData(config, dataSetDir);
//		
//		// Load the models
//		List<SingleServerMasterModel> models = new ArrayList<SingleServerMasterModel>();
//		try {
//			for (int serverId = 0; serverId < config.serverNum(); serverId++) {
//				File modelFilePath = new File(modelDir, "model-" + serverId + ".bin");
//				SingleServerMasterModel model = SingleServerMasterModel.loadFromFile(modelFilePath);
//				models.add(model);
//			}
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//		
//		if (logger.isLoggable(Level.INFO))
//			logger.info("All the data and models are loaded");
//		
//		if (logger.isLoggable(Level.INFO))
//			logger.info("Testing the models...");
//		
//		// Test the model with data set
//		ReportBuilder reportBuilder = new ReportBuilder();
//		for (int serverId = 0; serverId < config.serverNum(); serverId++) {
//			DataSet dataSet = dataSets.get(serverId);
//			SingleServerMasterModel model = models.get(serverId);
//
//			// Evaluate the model
//			evaluateModel(serverId, dataSet, model, reportBuilder);
//		}
//		
//		if (logger.isLoggable(Level.INFO))
//			logger.info("Testing completed. Generating a report...");
//		
//		// Save the report
//		try {
//			reportBuilder.writeToFile(new File("testing-report.csv"));
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
//		
//		if (logger.isLoggable(Level.INFO))
//			logger.info("The report is generated.");
//		
//		return 0;
//	}
}
