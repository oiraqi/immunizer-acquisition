package org.immunizer.microservices.monitor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonParser;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.google.gson.JsonElement;
import com.google.common.base.Splitter;

import org.apache.ignite.Ignition;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Collections;
import java.util.HashMap;

public class ModelMapper implements FlatMapFunction<byte[], String> {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private Ignite ignite;
	private IgniteCache numbersStdevsCache, numbersMeansCache, stringLengthsStdevsCache, 
		stringLengthsMeansCache, wholeLengthsStdevsCache, wholeLengthsMeansCache, 
			callStacksCache, pathsCache, aggPathsCache, splits1Cache, splits3Cache;
	private long callStackOccurences;
	private long[] minPathOccurences, min1Occurences, min3Occurences;
	private String[] min1AggregatedPathToNode, min3AggregatedPathToNode;
	private double[] maxNumberVariations, maxStringLengthVariations, wholeLengthVariations;
	private JsonObject invocation;
	private int numberOfParams;
	HashMap<String, String> model = new HashMap<String, String>();
	HashMap<String, Double> record = new HashMap<String, Double>();

	/**
	 * Extracts features from invocation Uses build method to build features
	 * recursively for each parameter tree or returned value tree
	 * 
	 * @param invocation
	 * @return The Feature Record
	 */
	public Iterator<String> call(byte[] invocationBytes) {
		JsonParser parser = new JsonParser();
		invocation = parser.parse(new String(invocationBytes)).getAsJsonObject();
		JsonArray parameters = invocation.get("params").getAsJsonArray();
		JsonElement result = null;
		numberOfParams = parameters.size();

		if (numberOfParams == 0)
			return null;

		TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
		ipFinder.setAddresses(Collections.singletonList("ignite:47500..47509"));
		
		TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
		discoSpi.setIpFinder(ipFinder);
		
		IgniteConfiguration cfg = new IgniteConfiguration();
		cfg.setDiscoverySpi(discoSpi);
		
		cfg.setGridName("Monitor");
		
		ignite = Ignition.getOrStart(cfg);
		numbersStdevsCache = ignite.cache("numbersStdevs");
		numbersMeansCache = ignite.cache("numbersMeans");
		stringLengthsStdevsCache = ignite.cache("stringLengthsStdevs");
		stringLengthsMeansCache = ignite.cache("stringLengthsMeans");
		wholeLengthsStdevsCache = ignite.cache("wholeLengthsStdevs");
		wholeLengthsMeansCache = ignite.cache("wholeLengthsMeans");
		callStacksCache = ignite.cache("callStacks");
		pathsCache = ignite.cache("paths");
		aggPathsCache = ignite.cache("aggPaths");
		splits1Cache = ignite.cache("splits1");
		splits3Cache = ignite.cache("splits3");
		splits1MinFrequenciesCache = ignite.cache("splits1MinFrequencies");
		splits3MinFrequenciesCache = ignite.cache("splits3MinFrequencies");
		
		int callStackId = invocation.get("callStackId").getAsInt();
		model.put("callstacks_" + callStackId, "");
		int length;
		double splits1MinFrequenciesSum, splits3MinFrequenciesSum;

		callStackOccurences = callStacksCache.get("" + callStackId);
		minPathOccurences = new long[numberOfParams + 1];
		min1Occurences = new long[numberOfParams + 1];
		min3Occurences = new long[numberOfParams + 1];
		min1AggregatedPathToNode = new String[numberOfParams + 1];
		min3AggregatedPathToNode = new String[numberOfParams + 1];
		double wholeLengthMean, wholeLengthStdev;
		wholeLengthVariations = new double[numberOfParams + 1];
		maxNumberVariations = new double[numberOfParams + 1];
		maxStringLengthVariations = new double[numberOfParams + 1];
		
		for (int i = 0; i < numberOfParams; i++) {
			length = parameters.get(i).toString().length();
			model.put("whllens_" + callStackId + "_p" + i + "_" + length, "");
			wholeLengthMean = wholeLengthsMeansCache.get(callStackId + "_p" + i);
			wholeLengthStdev = wholeLengthsStdevsCache.get(callStackId + "_p" + i);
			wholeLengthVariations[i] = Math.abs(length - wholeLengthMean) / wholeLengthStdev;
			minPathOccurences[i] = min1Occurences[i] = min3Occurences[i] = callStackOccurences;
			maxNumberVariations[i] = maxStringLengthVariations[i] = 0.5;
			build(callStackId, "p" + i, "p" + i, parameters.get(i), i);
			buildRecordFromParam(pi, i);
			
			splits1MinFrequenciesSum = splits1MinFrequenciesCache.get("" + callStackId + "_p" + i);
			/* Frequency smoothing */
			if (min1Occurences[i] > splits1MinFrequenciesSum)	/* No need to divide both sides by callStackOccurences */
				min1Occurences[i] = splits1MinFrequenciesSum;
			splits1MinFrequenciesCache.put("" + callStackId + "_p" + i, splits1MinFrequenciesSum + (double)min1Occurences[i] / min1AggregatedPathToNode[i]);

			splits3MinFrequenciesSum = splits3MinFrequenciesCache.get("" + callStackId + "_p" + i);
			if (min3Occurences[i] > splits3MinFrequenciesSum)
				min3Occurences[i] = splits3MinFrequenciesSum;
			splits3MinFrequenciesCache.put("" + callStackId + "_p" + i, splits3MinFrequenciesSum + (double)min3Occurences[i] / min3AggregatedPathToNode[i]);
		}

		if (invocation.get("_returns").getAsBoolean()) {
			result = invocation.get("result");
			length = result.toString().length();
			model.put("whllens_" + callStackId + "_r_" + length, "");
			wholeLengthMean = wholeLengthsMeansCache.get(callStackId + "_r");
			wholeLengthStdev = wholeLengthsStdevsCache.get(callStackId + "_r");
			wholeLengthVariations[numberOfParams] = Math.abs(length - wholeLengthMean) / wholeLengthStdev;
			minPathOccurences[numberOfParams] = min3Occurences[numberOfParams] = min1Occurences[numberOfParams] = callStackOccurences;
			maxNumberVariations[numberOfParams] = maxStringLengthVariations[numberOfParams] = 0.5;
			build(callStackId, "r", "r", result, numberOfParams);
			buildRecordFromResult();

			splits1MinFrequenciesSum = splits1MinFrequenciesCache.get("" + callStackId + "_r");
			splits1MinFrequenciesCache.put("" + callStackId + "_r", splits1MinFrequenciesSum + (double)min1Occurences[numberOfParams] / min1AggregatedPathToNode[numberOfParams]);
			splits3MinFrequenciesSum = splits3MinFrequenciesCache.get("" + callStackId + "_r");
			splits3MinFrequenciesCache.put("" + callStackId + "_r", splits3MinFrequenciesSum + (double)min3Occurences[numberOfParams] / min3AggregatedPathToNode[numberOfParams]);
		}

		callStacksCache.put("" + callStackId, callStackOccurences + 1);
		
		FeatureRecord fr = new FeatureRecord(callStackId, invocation.get("threadTag").getAsString(),
				invocation.get("fullyQualifiedMethodName").getAsString(),
				invocation.get("version").getAsString(), record);
		FeatureRecordProducer frp = new FeatureRecordProducer();
		frp.send(fr);

		return model.keySet().iterator();
	}

	/**
	 * Builds features for each parameter or returned value by walking recursively
	 * through the parameter tree or returned value tree
	 * 
	 * @param callStackId
	 * @param pathToNode
	 * @param aggregatedPathToNode for sibling/relative grouping and comparision
	 * @param jsonElement
	 * @param model
	 */
	private void build(int callStackId, String pathToNode, String aggregatedPathToNode,
			JsonElement jsonElement, int paramIndex) {

		if (jsonElement.isJsonNull())
			return;

		if (jsonElement.isJsonArray()) {
			JsonArray jsonArray = jsonElement.getAsJsonArray();
			for (int i = 0; i < jsonArray.size(); i++)
				/**
				 * While we call build for each element of the array with a different pathToNode
				 * (second parameter: pathToNode + '_' + i), we keep track of the same
				 * aggregatedPathToNode for all of them (third parameter) to group and compare
				 * siblings and relatives
				 */
				build(callStackId, pathToNode + '_' + i, aggregatedPathToNode, jsonArray.get(i),
						paramIndex);
		} else if (jsonElement.isJsonObject()) {
			JsonObject jsonObject = jsonElement.getAsJsonObject();
			
			Iterator<Entry<String, JsonElement>> entries = jsonObject.entrySet().iterator();
			while (entries.hasNext()) {
				String key = (String) entries.next().getKey();
				build(callStackId, pathToNode.isEmpty() ? key : pathToNode + '_' + key,
						aggregatedPathToNode.isEmpty() ? key : aggregatedPathToNode + '_' + key,
						jsonObject.get(key), paramIndex);
			}
		} else {
			JsonPrimitive primitive = jsonElement.getAsJsonPrimitive();
			model.put("paths_" + callStackId + "_" + pathToNode, "");
			long pathOccurences = pathsCache.get(callStackId + "_" + pathToNode);
			if (pathOccurences < minPathOccurences[paramIndex])
				minPathOccurences[paramIndex] = pathOccurences;
			
			String key = "aggpaths_" + callStackId + "_" + aggregatedPathToNode;
			if (!model.containsKey(key))
				model.put(key, "");

			if (primitive.isString()) {
				String value = primitive.getAsString();
				model.put("strlens_" + callStackId + '_' + aggregatedPathToNode + '_' + value.length(), "");
				double stringLengthsMean = stringLengthsMeansCache.get(callStackId + '_' + aggregatedPathToNode);
				double stringLengthsStdev = stringLengthsStdevsCache.get(callStackId + '_' + aggregatedPathToNode);
				double stringLengthVariation = Math.abs(value - stringLengthsMean) / stringLengthsStdev;
				if (stringLengthVariation > 1 && stringLengthVariation > maxStringLengthVariations[paramIndex])
					maxStringLengthVariations[paramIndex] = stringLengthVariation;
				
				long minSplitOccurences;
				minSplitOccurences = getSplits(value, 1, callStackId, aggregatedPathToNode);
				if (minSplitOccurences < min1Occurences[paramIndex]) {
					min1Occurences[paramIndex] = minSplitOccurences;
					min1AggregatedPathToNode[paramIndex] = aggregatedPathToNode;
				}
				minSplitOccurences = getSplits(value, 3, callStackId, aggregatedPathToNode);
				if (minSplitOccurences < min3Occurences[paramIndex]) {
					min3Occurences[paramIndex] = minSplitOccurences;
					min3AggregatedPathToNode[paramIndex] = aggregatedPathToNode;
				}
			} else if (primitive.isNumber()) {
				double value = primitive.getAsNumber().doubleValue();
				model.put("numbers_" + callStackId + '_' + aggregatedPathToNode + '_' + value, "");
				double numbersMean = numbersMeansCache.get(callStackId + '_' + aggregatedPathToNode);
				double numbersStdev = numbersStdevsCache.get(callStackId + '_' + aggregatedPathToNode);
				double numberVariation = Math.abs(value - numbersMean) / numbersStdev;
				if (numberVariation > 1 && numberVariation > maxNumberVariations[paramIndex])
					maxNumberVariations[paramIndex] = numberVariation;
			}
		}
	}

	private long getSplits(String input, int n, int callStackId, String aggregatedPathToNode) {
		Splitter splitter = Splitter.fixedLength(n);
		long minSplitOccurences = -1;

		for (int i = 0; i < n && i < input.length(); i++) {
			Iterable<String> splits = splitter.split(input.substring(i));

			for (String split : splits) {
				String key = "splits_" + n + "_" + callStackId + "_" + aggregatedPathToNode + "_" + split;
				if (!model.containsKey(key))
					model.put(key, "");

				long splitOccurences;
				if (n == 1)
					splitOccurences = splits1Cache.get(callStackId + "_" + aggregatedPathToNode + "_" + split)
				else if (n == 3)
					splitOccurences = splits3Cache.get(callStackId + "_" + aggregatedPathToNode + "_" + split)
				
				if ((minSplitOccurences >= 0 && splitOccurences < minSplitOccurences) || minSplitOccurences == -1)
					minSplitOccurences = splitOccurences;
			}
		}
		return minSplitOccurences;
	}

	private void buildRecordFromParam(JsonElement pi, int i) {
		if (!pi.isJsonPrimitive()) {
			record.put("p" + i + "_length_variation", wholeLengthVariations[i]);
			record.put("p" + i + "_path_min_f", (double) minPathOccurences[i] / callStackOccurences);
			record.put("p" + i + "_min_if1", (double) min1Occurences[i] / min1AggregatedPathToNode[i]);
			record.put("p" + i + "_min_if3", (double) min3Occurences[i] / min3AggregatedPathToNode[i]);
			record.put("p" + i + "_max_string_length_variation", maxStringLengthVariations[i]);
			record.put("p" + i + "_max_number_variation", maxNumberVariations[i]);
		} else if (pi.getAsJsonPrimitive().isString()) {
			record.put("p" + i + "_min_if1", (double) min1Occurences[i] / min1AggregatedPathToNode[i]);
			record.put("p" + i + "_min_if3", (double) min3Occurences[i] / min3AggregatedPathToNode[i]);
			record.put("p" + i + "_length_variation", (double) maxStringLengthVariations[i]);
		} else if (pi.getAsJsonPrimitive().isNumber()) {
			record.put("p" + i + "_number_variation", (double) maxNumberVariations[i]);
		}
	}

	private void buildRecordFromResult() {
		if (invocation.get("_returnsString").getAsBoolean()) {
			record.put("r_min_if1", (double) min1Occurences[numberOfParams] / min1AggregatedPathToNode[numberOfParams]);
			record.put("r_min_if3", (double) min3Occurences[numberOfParams] / min3AggregatedPathToNode[numberOfParams]);
			record.put("r_length_variation", maxStringLengthVariations[numberOfParams]);
		} else if (result.isJsonPrimitive() && result.getAsJsonPrimitive().isNumber()) {
			record.put("r_number_variation", maxNumberVariations[numberOfParams]);
		} else {
			record.put("r_length_variation", wholeLengthVariations[numberOfParams]);
			record.put("r_path_min_f", (double) minPathOccurences[numberOfParams] / callStackOccurences);
			record.put("r_min_if1", (double) min1Occurences[numberOfParams] / min1AggregatedPathToNode[numberOfParams]);
			record.put("r_min_if3", (double) min3Occurences[numberOfParams] / min3AggregatedPathToNode[numberOfParams]);
			record.put("r_max_string_length_variation", maxStringLengthVariations[numberOfParams]);
			record.put("r_max_number_variation", maxNumberVariations[numberOfParams]);
		}
	}
}