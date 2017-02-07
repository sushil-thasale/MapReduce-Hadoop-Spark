package src.fineLock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import src.common.StationData;

public class SharedDataStructure {

	public ArrayList<String> fileData;
	public ConcurrentHashMap<String, StationData> dataMap;

	public SharedDataStructure(ArrayList<String> fileData) {
		this.fileData = fileData;
		dataMap = new ConcurrentHashMap<String, StationData>(10000, 0.75f, 8);		
	}
}
