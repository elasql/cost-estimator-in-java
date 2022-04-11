package org.elasql.estimator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReportBuilder {
	
	private List<String> rows = new ArrayList<String>();
	
	public ReportBuilder() {
		// Write the header
		rows.add("Server ID, OU Name, Mean, STD, MAE");
	}
	
	public void writeRow(int serverId, String ouName, double mean, double std, double mae) {
		rows.add(String.format("%d, %s, %f, %f, %f", serverId, ouName, mean, std, mae));
	}
	
	public void writeToFile(File outFilePath) throws IOException {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(outFilePath))) {
			for (String row : rows) {
				writer.write(row);
				writer.newLine();
			}
			writer.flush();
		}
	}
}
