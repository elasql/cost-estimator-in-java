package org.elasql.estimator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.estimator.data.TotalLatencyDataSet;
import org.elasql.estimator.data.OuDataSet;
import org.elasql.estimator.model.GridSearcher;
import org.elasql.estimator.model.SingleServerMasterModel;
import org.elasql.estimator.model.SumMaxSequentialModel;
import org.elasql.estimator.model.evaluator.OuModelEvaluator;
import org.elasql.estimator.model.evaluator.SumMaxModelEvaluator;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import smile.data.type.StructType;

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
		
		List<OuDataSet> dataSets = OuDataSet.loadFromRawData(config.serverNum(),
				config.dataStartTime(), config.dataEndTime(), config.outlinerStdThreshold(),
				dataSetDir);
		
		if (logger.isLoggable(Level.INFO))
			logger.info("All data are loaded and processed.");
		
		// For each server
		OuModelEvaluator evaluator = null;
		for (int serverId = 0; serverId < dataSets.size(); serverId++) {
			OuDataSet dataSet = dataSets.get(serverId);
			
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
			if (evaluator == null) {
				StructType featureSchema = model.schema();
				evaluator = OuModelEvaluator.newWithFeatureSchema(featureSchema);
			}
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
		
		List<OuDataSet> dataSets = OuDataSet.loadFromRawData(config.serverNum(),
				config.dataStartTime(), config.dataEndTime(), config.outlinerStdThreshold(),
				dataSetDir);
		
		if (logger.isLoggable(Level.INFO))
			logger.info("All data are loaded and processed.");
		
		// For each server
		OuModelEvaluator evaluator = null;
		for (int serverId = 0; serverId < dataSets.size(); serverId++) {
			OuDataSet dataSet = dataSets.get(serverId);
			
			// Train a master model for each server
			if (logger.isLoggable(Level.INFO))
				logger.info(String.format("Training models for server #%d (data set size: %d)...",
						serverId, dataSet.size()));
			
			SingleServerMasterModel model = SingleServerMasterModel.fit(dataSet, config.modelParameters());
			
			if (logger.isLoggable(Level.INFO))
				logger.info("Training models for server #" + serverId + " completed");
			
			// Evaluate the model
			if (evaluator == null) {
				StructType featureSchema = model.schema();
				evaluator = OuModelEvaluator.newWithFeatureSchema(featureSchema);
			}
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
	
	@Command(name = "train-global", mixinStandardHelpOptions = true)
	public int trainGlobal(
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
		
		List<OuDataSet> dataSets = OuDataSet.loadFromRawData(config.serverNum(),
				config.dataStartTime(), config.dataEndTime(), config.outlinerStdThreshold(),
				dataSetDir);
		
		if (logger.isLoggable(Level.INFO))
			logger.info("All data are loaded and processed.");
		
		// Merge the data sets
		OuDataSet globalSet = dataSets.get(0);
		for (int i = 1; i < dataSets.size(); i++) {
			globalSet = globalSet.union(dataSets.get(i));
		}
		
		// Train a global model
		if (logger.isLoggable(Level.INFO))
			logger.info(String.format("Training a global model (data set size: %d)...",
					globalSet.size()));
		
		SingleServerMasterModel model = SingleServerMasterModel.fit(globalSet, config.modelParameters());
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Training a global model completed");
		
		// Evaluate the model
		OuModelEvaluator evaluator = OuModelEvaluator.newWithFeatureSchema(model.schema());
		evaluator.evaluateModel(0, globalSet, model);
		
		// Save the model
		try {
			File modelFilePath = new File(modelSaveDir, "model-global.bin");
			model.saveToFile(modelFilePath);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		// Save the report
		evaluator.generateReport(new File("training-global-report.csv"));
		
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
		List<OuDataSet> dataSets = OuDataSet.loadFromRawData(config.serverNum(),
				config.dataStartTime(), config.dataEndTime(), config.outlinerStdThreshold(),
				dataSetDir);
		
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
		StructType featureSchema = models.get(0).schema();
		OuModelEvaluator evaluator = OuModelEvaluator.newWithFeatureSchema(featureSchema);
		for (int serverId = 0; serverId < config.serverNum(); serverId++) {
			OuDataSet dataSet = dataSets.get(serverId);
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
	
	@Command(name = "test-global", mixinStandardHelpOptions = true)
	public int testGlobal(
			@Parameters(paramLabel = "DATA_SET_DIR", description = "path to the testing data set") File dataSetDir,
			@Parameters(paramLabel = "MODEL_DIR", description = "path to the saved models") File modelDir
		) {
		
		// Load the configurations
		Config config = Config.load(configFile);
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Loading the data set and the model...");

		// Load the data set
		List<OuDataSet> dataSets = OuDataSet.loadFromRawData(config.serverNum(),
				config.dataStartTime(), config.dataEndTime(), config.outlinerStdThreshold(),
				dataSetDir);
		
		// Merge the data sets
		OuDataSet globalSet = dataSets.get(0);
		for (int i = 1; i < dataSets.size(); i++) {
			globalSet = globalSet.union(dataSets.get(i));
		}
		
		// Load the models
		SingleServerMasterModel model = null;
		try {
			File modelFilePath = new File(modelDir, "model-global.bin");
			model = SingleServerMasterModel.loadFromFile(modelFilePath);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		if (logger.isLoggable(Level.INFO))
			logger.info("All the data and the model are loaded");
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Testing the model...");
		
		// Test the model with data set
		StructType featureSchema = model.schema();
		OuModelEvaluator evaluator = OuModelEvaluator.newWithFeatureSchema(featureSchema);
		evaluator.evaluateModel(0, globalSet, model);
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Testing completed. Generating a report...");
		
		// Save the report
		evaluator.generateReport(new File("testing-global-report.csv"));
		
		if (logger.isLoggable(Level.INFO))
			logger.info("The report is generated.");
		
		return 0;
	}
	
	@Command(name = "test-sum-max", mixinStandardHelpOptions = true)
	public int testSumMax(
			@Parameters(paramLabel = "DATA_SET_DIR", description = "path to the testing data set") File dataSetDir,
			@Parameters(paramLabel = "MODEL_DIR", description = "path to the saved models") File modelDir
		) {
		
		// Load the configurations
		Config config = Config.load(configFile);
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Loading the data set and the models...");
		
		// Load the data set
		TotalLatencyDataSet dataSet = TotalLatencyDataSet.load(
				dataSetDir, config.serverNum());
		
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
		
		// Create a sum-max model
		SumMaxSequentialModel sumMaxModel = new SumMaxSequentialModel(models);
		
		if (logger.isLoggable(Level.INFO))
			logger.info("All the data and models are loaded");
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Testing the models...");
		
		// Perform model evaluation (need a evaluator)
		SumMaxModelEvaluator evaluator = SumMaxModelEvaluator.newWithServerNumber(config.serverNum());
		evaluator.evaluateModel(dataSet, sumMaxModel);
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Testing completed. Generating a report...");
		
		// Save the report
		evaluator.generateReport(new File("sum-max-report.csv"));
		
		if (logger.isLoggable(Level.INFO))
			logger.info("The report is generated.");
		
		return 0;
	}
}
