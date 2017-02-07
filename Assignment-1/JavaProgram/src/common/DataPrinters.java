package src.common;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import src.common.StationData;

public class DataPrinters {

	/*
	 * writeAverageValues : dumps the given HashMap to given file
	 */
	public void writeAverageValues(HashMap<String, StationData> dataMap,
			String filePath) {
		PrintWriter pr = null;

		try {

			pr = new PrintWriter(filePath);

			for (Map.Entry<String, StationData> entry : dataMap.entrySet()) {
				String stationID = entry.getKey().toString();
				StationData s = (StationData) entry.getValue();
				double average = s.getAverage();
				pr.println(stationID + " : " + average);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			pr.close();
		}
	}

	/*
	 * writeAverageValues : dumps the ConcurrentHashMap to given file
	 */
	public void writeAverageValues(ConcurrentHashMap<String, StationData> dataMap,
			String filePath) {
		PrintWriter pr = null;

		try {

			pr = new PrintWriter(filePath);

			for (Map.Entry<String, StationData> entry : dataMap.entrySet()) {
				String stationID = entry.getKey().toString();
				StationData s = (StationData) entry.getValue();
				double average = s.getAverage();
				pr.println(stationID + " : " + average);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			pr.close();
		}
	}
	
	/*
	 * writeSummary : prints average, minimum and maximum execution time to console
	 */
	public void writeSummary(ArrayList<Long> executionTimes, String programType) {

		double sum = 0;
		for(long time:executionTimes)
			sum+=time;
		
		System.out.println(programType);
		System.out.println("Minimum Time:" + Collections.min(executionTimes));
		System.out.println("Maximum Time:" + Collections.max(executionTimes));
		System.out.println("Average Time:" + (double)sum/executionTimes.size()+"\n");		
	}
}
