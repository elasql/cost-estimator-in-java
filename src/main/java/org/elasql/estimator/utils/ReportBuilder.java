package org.elasql.estimator.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReportBuilder {
	
	private String header;
	private List<String> rows = new ArrayList<String>();
	
	public ReportBuilder(String header) {
		this.header = header;
	}
	
	public void writeRow(String row) {
		rows.add(row);
	}
	
	public void writeToFile(File outFilePath) throws IOException {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(outFilePath))) {
			writer.write(header);
			writer.newLine();
			for (String row : rows) {
				writer.write(row);
				writer.newLine();
			}
			writer.flush();
		}
	}
}
