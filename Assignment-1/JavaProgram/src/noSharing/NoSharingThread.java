package src.noSharing;

import java.util.HashMap;

import src.common.*;

/*
 * NoSharingThread : each thread reads data from shared data structures
 * 					 however, each thread has its own HasHMap to store data
 */
public class NoSharingThread extends Thread {

	private long start, end;
	private SharedDataStructure sharedData; // is used only to access csv file

	private HashMap<String, StationData> dataMap;
	private boolean addDelay;

	public NoSharingThread(long start, long end,
			SharedDataStructure sharedData, boolean addDelay) {
		this.start = start;
		this.end = end;
		this.sharedData = sharedData;
		this.addDelay = addDelay;
		this.dataMap = new HashMap<String, StationData>();
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

				if (dataMap.containsKey(stationID)) {
					StationData s = dataMap.get(stationID);
					s.incrementCount(1);
					s.incrementSum(tmax);
				} else {
					StationData s = new StationData();
					s.incrementCount(1);
					s.incrementSum(tmax);
					dataMap.put(stationID, s);
				}

				if (addDelay)
					Delay.addDelay();
			}
		}
	}

	public HashMap<String, StationData> getProcessedData() {
		return dataMap;
	}
}
