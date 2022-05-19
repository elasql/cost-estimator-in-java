package org.elasql.estimator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.estimator.data.DataSet;
import org.elasql.estimator.model.GridSearcher;
import org.elasql.estimator.model.SingleServerMasterModel;
import org.elasql.estimator.model.evaluator.TestingModelEvaluator;
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
	
	@Command(name = "grid-search", mixinStandardHelpOptions = true)
	public int gridSearch(
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
			DataSet dataSet = dataSets.get(serverId);
			
			// Train a master model for each server
			if (logger.isLoggable(Level.INFO))
				logger.info(String.format("Grid searching best parameters for the model of server #%d (data set size: %d)...",
						serverId, dataSet.size()));
			
			GridSearcher modelTrainer = new GridSearcher(config.crossValidationFold());
			SingleServerMasterModel model = modelTrainer.gridSearch(dataSet);
			modelTrainer.generateTrainingReport(new File("grid-search-" + serverId + ".csv"));
			
			if (logger.isLoggable(Level.INFO))
				logger.info("Training models for server #" + serverId + " completed");
			
			// Evaluate the model
			evaluator.evaluateModel(serverId, dataSet, model);
			
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
			DataSet dataSet = dataSets.get(serverId);
			
			// Train a master model for each server
			if (logger.isLoggable(Level.INFO))
				logger.info(String.format("Training models for server #%d (data set size: %d)...",
						serverId, dataSet.size()));
			
			SingleServerMasterModel model = SingleServerMasterModel.fit(dataSet, config.modelParameters());
			
			if (logger.isLoggable(Level.INFO))
				logger.info("Training models for server #" + serverId + " completed");
			
			// Evaluate the model
			evaluator.evaluateModel(serverId, dataSet, model);
			
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
	
	@Command(name = "test", mixinStandardHelpOptions = true)
	public int test(
			@Parameters(paramLabel = "DATA_SET_DIR", description = "path to the testing data set") File dataSetDir,
			@Parameters(paramLabel = "MODEL_DIR", description = "path to the saved models") File modelDir
		) {
		
		// Load the configurations
		Config config = Config.load(configFile);
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Loading the data set and the models...");

		// Load the data set
		List<DataSet> dataSets = DataSet.loadFromRawData(config, dataSetDir);
		
		// Load the models
		List<SingleServerMasterModel> models = new ArrayList<SingleServerMasterModel>();
		try {
			for (int serverId = 0; serverId < config.serverNum(); serverId++) {
				File modelFilePath = new File(modelDir, "model-" + serverId + ".bin");
				SingleServerMasterModel model = SingleServerMasterModel.loadFromFile(modelFilePath);
				models.add(model);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		if (logger.isLoggable(Level.INFO))
			logger.info("All the data and models are loaded");
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Testing the models...");
		
		// Test the model with data set
		TestingModelEvaluator evaluator = new TestingModelEvaluator();
		for (int serverId = 0; serverId < config.serverNum(); serverId++) {
			DataSet dataSet = dataSets.get(serverId);
			SingleServerMasterModel model = models.get(serverId);

			// Evaluate the model
			evaluator.evaluateModel(serverId, dataSet, model);
		}
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Testing completed. Generating a report...");
		
		// Save the report
		evaluator.generateReport(new File("testing-report.csv"));
		
		if (logger.isLoggable(Level.INFO))
			logger.info("The report is generated.");
		
		return 0;
	}
}
