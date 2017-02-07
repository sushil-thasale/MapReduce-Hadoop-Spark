package src.noLock;

import src.common.*;

public class NoLockThread extends Thread {

	private long start, end;
	private SharedDataStructure sharedData;
	private boolean addDelay;

	public NoLockThread(long start, long end, SharedDataStructure sharedData,
			boolean addDelay) {
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
	 * notice that the thread doesn't lock the HashMap while updating
	 */
	private void processData() {

		for (long index = start; index <= end; index++) {

			String line = sharedData.fileData.get((int) index);

			if (line.contains("TMAX")) {
				String values[] = line.split(",");
				String stationID = values[0];
				double tmax = Double.parseDouble(values[3]);

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

				// adds delay (calls fibonacci function) if addDelay is true
				if (addDelay)
					Delay.addDelay();
			}
		}
	}
}
