package src.common;

import java.util.ArrayList;
import java.util.HashMap;

import src.common.StationData;

/*
 * SharedDataStructure: fileData - stores input file
 * 						dataMap - HashMap that is shared between threads
 */
public class SharedDataStructure {

	public ArrayList<String> fileData;
	public HashMap<String, StationData> dataMap;

	public SharedDataStructure(ArrayList<String> fileData) {
		this.fileData = fileData;
		dataMap = new HashMap<String, StationData>();
	}
}
