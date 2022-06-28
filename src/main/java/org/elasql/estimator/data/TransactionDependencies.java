package org.elasql.estimator.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransactionDependencies {
	
	public static TransactionDependencies load(File inputFile) {
		try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
			Map<Long, List<Long>> dependencyMapping = new HashMap<Long, List<Long>>();
			
			// Skip the header
			reader.readLine();
			
			// Read each line which stands for the dependencies for a transaction
			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] tokens = line.split("=>");
				if (!tokens[1].trim().equals("X")) { // skip the line ends with 'X'
					Long txNum = Long.parseLong(tokens[0].trim());
					List<Long> dependencies = new ArrayList<Long>();
					String[] txTokens = tokens[1].split(",");
					for (String txToken : txTokens) {
						Long dependentTx = Long.parseLong(txToken.trim());
						dependencies.add(dependentTx);
					}
					dependencyMapping.put(txNum, dependencies);
				}
			}
			
			return new TransactionDependencies(dependencyMapping);
		} catch (IOException e) {
			throw new RuntimeException("Failed to load transaction dependencies from "
					+ inputFile, e);
		}
	}
	
	private Map<Long, List<Long>> dependencyMapping = new HashMap<Long, List<Long>>();
	
	private TransactionDependencies(Map<Long, List<Long>> dependencyMapping) {
		this.dependencyMapping = dependencyMapping;
	}
	
	public List<Long> getDependencies(Long txNum) {
		List<Long> dependencies = dependencyMapping.get(txNum);
		if (dependencies == null) {
			return Collections.emptyList();
		}
		return dependencies;
	}
}
