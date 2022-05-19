package org.elasql.estimator.model;

import java.util.HashMap;
import java.util.Map;

import org.elasql.estimator.Constants;

public class ModelParameters {
	
	public static class Builder {
		Map<String, Integer> treeCounts;
		Map<String, Integer> maxDepths;
		
		public Builder() {
			treeCounts = new HashMap<String, Integer>();
			maxDepths = new HashMap<String, Integer>();
		}
		
		public void addParameters(String ouName, 
				int treeCount, int maxDepth) {
			treeCounts.put(ouName, treeCount);
			maxDepths.put(ouName, maxDepth);
		}
		
		public ModelParameters build() {
			// Check the parameters of all OUs are given
			for (String ouName : Constants.OU_NAMES) {
				if (!treeCounts.containsKey(ouName))
					throw new IllegalArgumentException("The model parameters of OU'" +
							ouName + "' is missing");
			}
			
			return new ModelParameters(treeCounts, maxDepths);
		}
	}
	
	// <OU Name> -> <Parameter>
	private Map<String, Integer> treeCounts;
	private Map<String, Integer> maxDepths;
	
	private ModelParameters(Map<String, Integer> treeCounts,
			Map<String, Integer> maxDepths) {
		this.treeCounts = treeCounts;
		this.maxDepths = maxDepths;
	}
	
	public Integer treeCount(String ouName) {
		return treeCounts.get(ouName);
	}
	
	public Integer maxDepth(String ouName) {
		return maxDepths.get(ouName);
	}
}
