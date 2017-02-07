package src.coarseLock;

import src.common.*;

public class CoarseLockThread extends Thread {

	private long start, end;
	private SharedDataStructure sharedData;
	private boolean addDelay;

	public CoarseLockThread(long start, long end,
			SharedDataStructure sharedData, boolean addDelay) {
		this.start = start;
		this.end = end;
		this.sharedData = sharedData;
		this.addDelay = addDelay;
	}

	public void run() {
		processData();
	}

	/*
	 * reads input file and updates the HashMap accordingly
	 * notice that the entire HashMap is locked by a single thread
	 */
	private void processData() {

		for (long index = start; index <= end; index++) {

			String line = sharedData.fileData.get((int) index);

			if (line.contains("TMAX")) {
				String values[] = line.split(",");
				String stationID = values[0];
				double tmax = Double.parseDouble(values[3]);

				// get a lock on the HashMap object
				synchronized (sharedData.dataMap) {

					if (sharedData.dataMap.containsKey(stationID)) {
						StationData s = sharedData.dataMap.get(stationID);
						s.incrementCount(1);
						s.incrementSum(tmax);
					} else {
						StationData s = new StationData();
						s.incrementCount(1);
						s.incrementSum(tmax);
						sharedData.dataMap.put(stationID, s);
					}

					if (addDelay)
						Delay.addDelay();
				}
			}
		}
	}
}
