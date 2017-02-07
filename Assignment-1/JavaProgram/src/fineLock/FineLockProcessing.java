package src.fineLock;

import java.util.ArrayList;
import src.common.DataPrinters;

/*
 * FineLockProcessing : Uses concurrent HasMap as a shared data structure between threads 
 */
public class FineLockProcessing {

	SharedDataStructure sharedData;
	int maxCores = 8; 
	final String fileName = "output/fine-lock.txt";
	long fileSize;
	long partitionSize;
	long startIndex, endIndex;
	long startTime, endTime;
	ArrayList<Long> executionTimes;
	ArrayList<FineLockThread> threads;

	public FineLockProcessing(ArrayList<String> fileData) {

		sharedData = new SharedDataStructure(fileData);
		fileSize = fileData.size();
		partitionSize = fileSize / maxCores;
		threads = new ArrayList<FineLockThread>();
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

		String programType = addDelay ? "Fine-Lock with delay": "Fine-Lock without delay";

		for (int iteration = 0; iteration < 10; iteration++) {

			try {

				spawnThreads(addDelay);

				startTime = System.currentTimeMillis();

				for (FineLockThread t : threads)
					t.start();

				for (FineLockThread t : threads)
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

			threads.add(new FineLockThread(startIndex, endIndex, sharedData,
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
