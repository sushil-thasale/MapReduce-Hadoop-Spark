package src.coarseLock;

import java.util.ArrayList;
import src.common.*;

/*
 * CoarseLockProcessing : threads lock the entire HashMap while updating it
 */
public class CoarseLockProcessing {

	SharedDataStructure sharedData;
	int maxCores = 8; // lscpu -> # of CPUs
	final String fileName = "output/coarse-lock.txt";
	long fileSize;
	long partitionSize;
	long startIndex, endIndex;
	long startTime, endTime;
	ArrayList<Long> executionTimes;
	ArrayList<CoarseLockThread> threads;

	public CoarseLockProcessing(ArrayList<String> fileData) {

		sharedData = new SharedDataStructure(fileData);
		fileSize = fileData.size();
		partitionSize = fileSize / maxCores;
		threads = new ArrayList<CoarseLockThread>();
		executionTimes = new ArrayList<Long>();
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

		String programType = addDelay ? "Coarse-Lock with delay": "Coarse-Lock without delay";

		for (int iteration = 0; iteration < 10; iteration++) {

			try {

				spawnThreads(addDelay);

				startTime = System.currentTimeMillis();

				for (CoarseLockThread t : threads)
					t.start();

				for (CoarseLockThread t : threads)
					t.join();

				endTime = System.currentTimeMillis();
				executionTimes.add(endTime - startTime);

			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
		}

		DataPrinters dp = new DataPrinters();
		dp.writeAverageValues(sharedData.dataMap, fileName);
		dp.writeSummary(executionTimes, programType);
		cleanup();
	}

	/*
	 * spawn 8 threads and distribute input data amongst them
	 */
	private void spawnThreads(boolean addDelay) {

		sharedData.dataMap.clear();
		threads.clear();
		startIndex = 0;
		endIndex = -1;

		for (int i = 0; i < maxCores; i++) {

			startIndex = endIndex + 1;
			endIndex = startIndex + partitionSize;

			if (i == 7)
				endIndex = fileSize - 1;

			threads.add(new CoarseLockThread(startIndex, endIndex, sharedData,
					addDelay));
		}
	}

	/*
	 * clean up the used data structures for subsequent execution
	 */
	private void cleanup() {
		sharedData.dataMap.clear();
		threads.clear();
		executionTimes.clear();
	}
}
