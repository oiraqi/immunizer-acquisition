package org.immunizer.microservices.monitor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;
import java.util.Map.Entry;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonParser;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.google.gson.JsonElement;
import com.google.common.base.Splitter;

public class ModelMapper implements FlatMapFunction<byte[], String> {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Extracts features from invocation Uses build method to build features
	 * recursively for each parameter tree or returned value tree
	 * 
	 * @param invocation
	 * @return The Feature Record
	 */
	public Iterator<String> call(byte[] invocationBytes) {
		JsonParser parser = new JsonParser();
		JsonObject invocation = parser.parse(new String(invocationBytes)).getAsJsonObject();
		int callStackId = invocation.get("callStackId").getAsInt();
		JsonElement parameters = invocation.get("params"), result = null;
		int numberOfParams = parameters.getAsJsonArray().size();
		Vector<String> model = new Vector<String>();
		int[] lengths;

		if (numberOfParams > 0 && invocation.get("_returns").getAsBoolean()) {
			result = invocation.get("result");
			lengths = new int[numberOfParams + 1];
			for (int i = 0; i < numberOfParams; i++) {
				lengths[i] = parameters.getAsJsonArray().get(i).toString().length();
			}
			lengths[numberOfParams] = result.toString().length();

			build(callStackId, "p", "p", parameters, -1, false, numberOfParams, model);
			build(callStackId, "r", "r", result, -1, false, numberOfParams, model);
		} else if (numberOfParams > 0) {
			lengths = new int[numberOfParams];
			for (int i = 0; i < numberOfParams; i++) {
				lengths[i] = parameters.getAsJsonArray().get(i).toString().length();
			}

			build(callStackId, "p", "p", parameters, -1, false, numberOfParams, model);
		} else {
			return null;
		}

		return model.iterator();
	}

	/**
	 * Builds features for each parameter or returned value by walking recursively
	 * through the parameter tree or returned value tree
	 * 
	 * @param callStackId
	 * @param pathToNode
	 * @param aggregatedPathToNode for sibling/relative grouping and comparision
	 * @param jsonElement
	 * @param paramIndex
	 * @param isParentAnArray
	 * @param numberOfParams
	 */
	private void build(int callStackId, String pathToNode, String aggregatedPathToNode, JsonElement jsonElement,
			int paramIndex, boolean isParentAnArray, int numberOfParams, Vector<String> model) {

		if (jsonElement.isJsonNull())
			return;

		if (jsonElement.isJsonArray()) {
			JsonArray jsonArray = jsonElement.getAsJsonArray();
			if (pathToNode.equals("p")) {
				for (int i = 0; i < numberOfParams; i++) {
					build(callStackId, "p" + i, "p" + i, jsonArray.get(i), i, false, numberOfParams, model);
				}
			} else
				for (int i = 0; i < jsonArray.size(); i++)
					/**
					 * While we call build for each element of the array with a different pathToNode
					 * (second parameter: pathToNode + '_' + i), we keep track of the same
					 * aggregatedPathToNode for all of them (third parameter) to group and compare
					 * siblings and relatives
					 */
					build(callStackId, pathToNode + '_' + i, aggregatedPathToNode, jsonArray.get(i), paramIndex, true,
							numberOfParams, model);
		} else if (jsonElement.isJsonObject()) {
			JsonObject jsonObject = jsonElement.getAsJsonObject();
			/*
			 * Iterator<String> keys = jsonObject.keySet().iterator(); while
			 * (keys.hasNext()) { String key = (String) keys.next(); build(callStackId,
			 * pathToNode.isEmpty() ? key : pathToNode + '_' + key,
			 * aggregatedPathToNode.isEmpty() ? key : aggregatedPathToNode + '_' + key,
			 * jsonObject.get(key), paramIndex, isParentAnArray, numberOfParams, model); }
			 */
			Iterator<Entry<String, JsonElement>> entries = jsonObject.entrySet().iterator();
			while (entries.hasNext()) {
				String key = (String) entries.next().getKey();
				build(callStackId, pathToNode.isEmpty() ? key : pathToNode + '_' + key,
						aggregatedPathToNode.isEmpty() ? key : aggregatedPathToNode + '_' + key, jsonObject.get(key),
						paramIndex, isParentAnArray, numberOfParams, model);
			}
		} else {
			JsonPrimitive primitive = jsonElement.getAsJsonPrimitive();
			model.add("paths_" + callStackId + "_" + pathToNode);

			if (primitive.isString()) {
				String value = primitive.getAsString();
				getSplits(value, 1, callStackId, aggregatedPathToNode, model);
				getSplits(value, 3, callStackId, aggregatedPathToNode, model);
			} else if (primitive.isNumber()) {
				double value = primitive.getAsNumber().doubleValue();
				model.add("numbers_" + callStackId + '_' + aggregatedPathToNode + '_' + value);
			}
		}
	}

	private void getSplits(String input, int n, int callStackId, String aggregatedPathToNode, Vector<String> model) {
		HashMap<String, String> splitsSeen = new HashMap<String, String>();
		Splitter splitter = Splitter.fixedLength(n);

		for (int i = 0; i < n && i < input.length(); i++) {
			Iterable<String> splits = splitter.split(input.substring(i));

			for (String split : splits) {
				if (splitsSeen.containsKey(split))
					continue;

				model.add("splits_" + n + "_" + callStackId + "_" + aggregatedPathToNode + "_" + split);

				splitsSeen.put(split, "");
			}
		}
	}
}