package src.sequential;

import java.util.ArrayList;
import java.util.HashMap;
import src.common.*;

/*
 * SequentialProcessing : reads th input file and sequentially process the data
 * 						  and stores the processed data accordingly
 */
public class SequentialProcessing {

	private HashMap<String, StationData> dataMap;
	private final String fileName = "output/sequential.txt";
	private ArrayList<String> fileData;
	ArrayList<Long> executionTimes;
	long startTime, endTime;

	public SequentialProcessing(ArrayList<String> fileData) {

		this.fileData = fileData;
		executionTimes = new ArrayList<Long>();
		dataMap = new HashMap<String, StationData>();
		startTime = 0;
		endTime = 0;
	}

	/*
	 * sequentially read and process the input data 10 times
	 * record the execution time for each run
	 */
	public void execute(boolean addDelay) {

		String programType = addDelay ? "Sequential with delay" : "Sequential without delay";

		for (int iteration = 0; iteration < 10; iteration++) {
			
			dataMap.clear();
			startTime = System.currentTimeMillis();
			processData(addDelay);
			endTime = System.currentTimeMillis();
			executionTimes.add(endTime - startTime);
		}

		DataPrinters dp = new DataPrinters();
		dp.writeAverageValues(dataMap, fileName);
		dp.writeSummary(executionTimes, programType);
		dataMap.clear();
		executionTimes.clear();
	}

	/*
	 * reads the input file and computes the average TMAX for each station
	 */
	private void processData(boolean addDelay) {

		for (String line : fileData) {
			
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

				// adds delay (calls fibonacci) is "addDelay" is true
				if (addDelay)
					Delay.addDelay();
			}
		}
	}
}
