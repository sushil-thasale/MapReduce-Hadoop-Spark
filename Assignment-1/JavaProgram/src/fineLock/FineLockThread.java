package src.fineLock;

import src.common.Delay;
import src.common.StationData;

public class FineLockThread extends Thread {

	private long start, end;
	private SharedDataStructure sharedData;
	private boolean addDelay;

	public FineLockThread(long start, long end, SharedDataStructure sharedData,
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
	 * reads input file and updates the concurrent HashMap accordingly
	 */
	private void processData() {

		for (long index = start; index <= end; index++) {

			String line = sharedData.fileData.get((int) index);

			if (line.contains("TMAX")) {
				String values[] = line.split(",");
				String stationID = values[0];
				double tmax = Double.parseDouble(values[3]);

				StationData s = new StationData();
				s.incrementCount(1);
				s.incrementSum(tmax);

				// putIFAbsent is atomic in nature (even replace)
				if (sharedData.dataMap.putIfAbsent(stationID, s) != null) {

					// method-1
					// StationData oldObject, newObject;
					// do {
					// oldObject = sharedData.dataMap.get(stationID);
					// newObject = oldObject.clone();
					// newObject.incrementCount(1);
					// newObject.incrementSum(tmax);
					//
					// } while (!sharedData.dataMap.replace(stationID,
					// oldObject, newObject));

					// method-2
					synchronized (sharedData.dataMap.get(stationID)) {
						StationData s1 = sharedData.dataMap.get(stationID);
						s1.incrementCount(1);
						s1.incrementSum(tmax);
					}
				}

				// adds delay (calls fibonacci function) if addDelay is true
				if (addDelay)
					Delay.addDelay();
			}
		}
	}
}
