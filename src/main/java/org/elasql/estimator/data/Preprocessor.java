package org.elasql.estimator.data;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.elasql.estimator.NewConstants;

import smile.data.DataFrame;
import smile.data.Tuple;
import smile.data.type.DataType;
import smile.data.type.DataTypes;
import smile.data.type.StructField;
import smile.data.type.StructType;

public class Preprocessor {
	
	public static DataFrame[] preprocess(DataFrame features, DataFrame labels, int serverId) {
		Stream<Tuple> featStream = features.stream();
		Stream<Tuple> labelStream = labels.stream();
		
		// Separate array features
		StructType oldFeatSchema = features.schema();
		StructType newFeatSchema = newSchemaWithSeparatedArrays(oldFeatSchema);
		featStream = separateArrayFeatures(featStream, oldFeatSchema, newFeatSchema, serverId);
		
		// Filter rows that do not appear in both sides
		Iterator<Tuple> featIter = sort(featStream).iterator();
		Iterator<Tuple> labelIter = sort(labelStream).iterator();
		
		// Join tuples
		List<Tuple> newFeatureRows = new ArrayList<Tuple>();
		List<Tuple> newLabelRows = new ArrayList<Tuple>();
		Tuple featTuple = featIter.next();
		Tuple labelTuple = labelIter.next();
		while (featIter.hasNext() && labelIter.hasNext()) {
			long featId = featTuple.getLong(NewConstants.FIELD_NAME_ID);
			long labelId = labelTuple.getLong(NewConstants.FIELD_NAME_ID);
			
			if (featId < labelId) {
				featTuple = featIter.next();
			} else if (featId > labelId) {
				labelTuple = labelIter.next();
			} else {
				newFeatureRows.add(featTuple);
				newLabelRows.add(labelTuple);
				featTuple = featIter.next();
				labelTuple = labelIter.next();
			}
		}
		
		// Create new DataFrame
		DataFrame newFeatures = DataFrame.of(newFeatureRows, newFeatSchema);
		DataFrame newLabels = DataFrame.of(newLabelRows, labels.schema());
		
		// Drop columns
		newFeatures = newFeatures.drop(NewConstants.FIELD_NAME_START_TIME);
		newLabels = newLabels.drop(NewConstants.FIELD_NAME_IS_MASTER,
				NewConstants.FIELD_NAME_IS_DIST);
		
		return new DataFrame[] {newFeatures, newLabels};
	}
	
	private static Stream<Tuple> separateArrayFeatures(Stream<Tuple> stream,
			StructType oldSchema, StructType newSchema, int serverId) {
		return stream.map(tuple -> {
			Object[] row = new Object[oldSchema.length()];
			
			// Pick out server i's data from arrays
			for (int i = 0; i < oldSchema.length(); i++) {
	        	if (oldSchema.field(i).type == DataTypes.DoubleArrayType) {
	        		double[] array = (double[]) tuple.get(i);
	        		row[i] = array[serverId];
	        	} else {
	        		row[i] = tuple.get(i);
	        	}
	        }
			
			return Tuple.of(row, newSchema);
		});
	}
	
	private static Stream<Tuple> sort(Stream<Tuple> stream) {
		return stream.sorted(new Comparator<Tuple>() {
			public int compare(Tuple t1, Tuple t2) {
				long id1 = t1.getLong(NewConstants.FIELD_NAME_ID);
				long id2 = t2.getLong(NewConstants.FIELD_NAME_ID);
				return (int) (id1 - id2);
			}
		});
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
