package org.elasql.estimator.data;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import smile.data.DataFrame;
import smile.data.Tuple;
import smile.data.type.DataType;
import smile.data.type.DataTypes;
import smile.data.type.StructField;
import smile.data.type.StructType;

public class Preprocessor {

	public static void main(String[] args) {
		File p = new File("D:\\datalab\\research-2021-hermes-control\\data-sets\\estimator\\training-data\\transaction-features.csv");
//		File p = new File("D:\\datalab\\research-2021-hermes-control\\data-sets\\estimator\\training-data\\transaction-latency-server-0.csv");
		DataFrame a = CsvLoader.load(p.toPath());
		System.out.println(a.size());
		System.out.println(a.toString(25));
		DataFrame df = separateArrayFeatures(a, 0);
		System.out.println(df.size());
		System.out.println(df.toString(25));
	}
	
	public static DataFrame separateArrayFeatures(DataFrame features, int serverId) {
		StructType schema = features.schema();
		StructType newSchema = newSchemaWithSeparatedArrays(schema);
		
		List<Tuple> newTuples = new ArrayList<Tuple>(features.nrows());
		Iterator<Tuple> tupleIter = features.stream().iterator();
		while (tupleIter.hasNext()) {
			Tuple tuple = tupleIter.next();
			Object[] row = new Object[schema.length()];
			
			// Pick out server i's data from arrays
			for (int i = 0; i < schema.length(); i++) {
	        	if (schema.field(i).type == DataTypes.DoubleArrayType) {
	        		double[] array = (double[]) tuple.get(i);
	        		row[i] = array[serverId];
	        	} else {
	        		row[i] = tuple.get(i);
	        	}
	        }
			
			newTuples.add(Tuple.of(row, newSchema));
		}
		
		return DataFrame.of(newTuples, newSchema);
	}
	
	private static StructType newSchemaWithSeparatedArrays(StructType schema) {
		String[] names = new String[schema.length()];
        DataType[] newTypes = new DataType[schema.length()];
        for (int i = 0; i < names.length; i++) {
        	StructField field = schema.field(i);
        	names[i] = field.name;
        	if (field.type == DataTypes.DoubleArrayType) {
        		newTypes[i] = DataTypes.DoubleType;
        	} else {
        		newTypes[i] = field.type;
        	}
        }
        StructField[] fields = new StructField[names.length];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = new StructField(names[i], newTypes[i]);
        }
        return DataTypes.struct(fields);
	}
}
