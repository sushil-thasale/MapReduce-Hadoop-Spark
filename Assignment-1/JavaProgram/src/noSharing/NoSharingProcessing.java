package src.noSharing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import src.common.*;

/*
 * NoSharingProcessing : In this version the threads do not share a centralized data structure
 * 					 - each thread has its own HashMap
 * 					 - once all the threads complete their execution their HashMaps are merged 
 */
public class NoSharingProcessing {

	SharedDataStructure sharedData;
	int maxCores = 8; // lscpu -> # of CPUs
	final String fileName = "output/no-sharing.txt";
	long fileSize;
	long partitionSize;
	long startIndex, endIndex;
	long startTime, endTime;
	ArrayList<Long> executionTimes;
	ArrayList<NoSharingThread> threads;
	HashMap<String, StationData> dataMap;

	public NoSharingProcessing(ArrayList<String> fileData) {

		sharedData = new SharedDataStructure(fileData);
		fileSize = fileData.size();
		partitionSize = fileSize / maxCores;
		threads = new ArrayList<NoSharingThread>();
		executionTimes = new ArrayList<Long>();
		dataMap = new HashMap<String, StationData>();
		startIndex = 0;
		endIndex = -1;
		startTime = 0;
		endTime = 0;
	}

	/*
	 * spawn 8 threads, distributes input data amongst threads
	 * starts thread execution
	 * records execution time of threads
	 * repeats above process 10 times
	 */
	public void execute(boolean addDelay) {

		String programType = addDelay ? "N0-Sharing with delay"
				: "N0-Sharing without delay";

		for (int iteration = 0; iteration < 10; iteration++) {

			try {

				spawnThreads(addDelay);

				startTime = System.currentTimeMillis();

				// start all threads
				for (NoSharingThread t : threads)
					t.start();

				// wait for completion of each thread
				for (NoSharingThread t : threads)
					t.join();

				// the HashMaps of each thread
				mergeProcessedData();

				endTime = System.currentTimeMillis();
				executionTimes.add(endTime - startTime);

			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
		}

		DataPrinters dp = new DataPrinters();
		dp.writeAverageValues(dataMap, fileName);
		dp.writeSummary(executionTimes, programType);
		cleanup();
	}

	/*
	 * spawn 8 threads and distribute input data amongst them
	 */
	private void spawnThreads(boolean addDelay) {

		dataMap.clear();
		threads.clear();
		startIndex = 0;
		endIndex = -1;

		for (int i = 0; i < maxCores; i++) {

			startIndex = endIndex + 1;
			endIndex = startIndex + partitionSize;

			if (i == 7)
				endIndex = fileSize - 1;

			threads.add(new NoSharingThread(startIndex, endIndex, sharedData,
					addDelay));
		}
	}

	/*
	 * mergeProcessedData : gets the processed data from each thread and combines them
	 * 						to get the final result
	 */
	private void mergeProcessedData() {

		for (NoSharingThread t : threads) {
			
			HashMap<String, StationData> processedData = t.getProcessedData();
			
			for (Map.Entry<String, StationData> entry : processedData.entrySet()) {
				
				String stationID = (String) entry.getKey();
				double sum = entry.getValue().getSum();
				long count = entry.getValue().getCount();

				if (dataMap.containsKey(stationID)) {
					StationData s = dataMap.get(stationID);
					s.incrementSum(sum);
					s.incrementCount(count);
				} else {
					StationData s = new StationData();
					s.incrementSum(sum);
					s.incrementCount(count);
					dataMap.put(stationID, s);
				}
			}
		}
	}

	/*
	 * clean up the used data structures for subsequent execution
	 */
	private void cleanup() {
		dataMap.clear();
		threads.clear();
		executionTimes.clear();
	}
}
