package org.elasql.estimator.data;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.elasql.estimator.Constants;

import smile.data.DataFrame;
import smile.data.Tuple;
import smile.data.type.DataType;
import smile.data.type.DataTypes;
import smile.data.type.StructField;
import smile.data.type.StructType;

public class CsvLoader {
	
	public static DataFrame load(Path path, Predicate<Tuple> filter) {
		try {
			StructType schema = inferSchema(path);
	        List<Function<String, Object>> valParsers = schema.parser();
	        
	        // Parse CSV row by row
	        try (CSVParser csv = newCsvParser(path)) {
	            List<Tuple> rows = new ArrayList<>();
	            
	            for (CSVRecord record : csv) {
	            	// Skip headers (there may be many)
	            	if (isHeader(record))
	            		continue;
	            	
	            	// Parse data
	            	Tuple tuple = parseCsvRecord(record, schema, valParsers);
	            	
	            	// Add only the row that matches the predicate
	            	if (filter.test(tuple))
	            		rows.add(tuple);
	            }
	            
	            // Wrap the data to a data frame
	            return DataFrame.of(rows, schema);
	        }
        } catch (IOException e) {
        	throw new RuntimeException("Error while reading a CSV file from '" + path + "'", e);
        }
	}
	
	private static CSVParser newCsvParser(Path path) throws IOException {
		return CSVParser.parse(path, StandardCharsets.UTF_8, CSVFormat.DEFAULT);
	}
	
	private static boolean isHeader(CSVRecord record) {
		return record.get(0).trim().equals(Constants.FIELD_NAME_ID);
	}
	
	private static double[] parseDoubleArray(String s) {
		// strip surrounding []
        String[] elements = s.substring(1, s.length() - 1).split(",");
        double[] array = new double[elements.length];
        for (int ei = 0; ei < elements.length; ei++) {
            array[ei] = Double.parseDouble(elements[ei]);
        }
        return array;
	}
	
	private static StructType inferSchema(Path path) throws IOException {
		// Find the last CSV records that might be a header and values
		CSVRecord lastHeader = null;
		CSVRecord lastValues = null;
		try (CSVParser csv = newCsvParser(path)) {
            for (CSVRecord record : csv) {
        		// Since there might be multiple header lines in the file, we treat
        		// the last line starting with 'Transaction ID' as the header
            	if (isHeader(record))
            		lastHeader = record;
            	else
            		lastValues = record;
            }
        }
		
		// Infer the schema
		String[] names = new String[lastValues.size()];
        DataType[] types = new DataType[lastValues.size()];
        for (int i = 0; i < names.length; i++) {
        	String fieldName = lastHeader.get(i).trim();
            names[i] = fieldName;

    		// 'Transaction ID' and 'Start Time' are always LongType
    		// 'Is Master' and 'Is Distributed' are always BooleanType
    		// If starting with '[', assume it is a Double Array.
    		// Otherwise, treat the rest as Double values.
            if (fieldName.equals(Constants.FIELD_NAME_ID) ||
            		fieldName.equals(Constants.FIELD_NAME_START_TIME))
            	types[i] = DataTypes.LongType;
            else if (fieldName.equals(Constants.FIELD_NAME_IS_MASTER) ||
            		fieldName.equals(Constants.FIELD_NAME_IS_DIST))
            	types[i] = DataTypes.BooleanType;
            else {
            	String value = lastValues.get(i).trim();
            	if (value.startsWith("["))
            		types[i] = DataTypes.DoubleArrayType;
            	else
            		types[i] = DataTypes.DoubleType;
            }
        }

		// Build the schema
        StructField[] fields = new StructField[names.length];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = new StructField(names[i], types[i]);
        }
        return DataTypes.struct(fields);
	}
	
	private static Tuple parseCsvRecord(CSVRecord record, StructType schema,
			List<Function<String, Object>> valParsers) {
		StructField[] fields = schema.fields();
		Object[] row = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
        	String valStr = null;
        	
        	// Get the value
        	if (i < record.size()) {
        		valStr = record.get(i).trim();
        	}
            
        	// Fill the field if it does not exists
            if (valStr == null || valStr.isEmpty()) {
            	// We assume the missing values are all double values
            	if (fields[i].type.isDouble())
            		row[i] = 0.0;
            	else
            		throw new RuntimeException("Don't know how to fill values for '" +
            				fields[i].name + "'");
            } else {
            	if (fields[i].type == DataTypes.DoubleArrayType) {
            		// The library does implement a parser for double arrays.
            		// However, it saves all the values into an Object array,
            		// which leads to a bug causing toString() does not work properly.
                    row[i] = parseDoubleArray(valStr);
            	} else {
            		row[i] = valParsers.get(i).apply(valStr);
            	}
            }
        }
        return Tuple.of(row, schema);
	}
}
