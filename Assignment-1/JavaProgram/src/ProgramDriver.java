package src;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

import src.coarseLock.CoarseLockProcessing;
import src.fineLock.FineLockProcessing;
import src.noLock.NoLockProcessing;
import src.noSharing.NoSharingProcessing;
import src.sequential.SequentialProcessing;

/*
 * ProgramDriver : - acts as a program handler.
 * 				   - loads file into memory and stores in an ArrayList
 *                 - executes each version of program  
 */
public class ProgramDriver {

	public static void main(String args[]) {
		if (args[0] == null) {
			System.out.println("provide csv file path");
			return;
		}

		String filePath = args[0];
		ArrayList<String> fileData = loadData(filePath);
		boolean addDelay = true;

		SequentialProcessing seq = new SequentialProcessing(fileData);
		seq.execute(!addDelay);		// sequential without delay
		seq.execute(addDelay);		// sequential with delay

		NoLockProcessing nolock = new NoLockProcessing(fileData);
		nolock.execute(!addDelay);	// no-lock without delay
		nolock.execute(addDelay);	// no-lock with delay

		CoarseLockProcessing coarse = new CoarseLockProcessing(fileData);
		coarse.execute(!addDelay);	// coarse-lock without delay
		coarse.execute(addDelay);	// coarse-lock with delay

		FineLockProcessing fine = new FineLockProcessing(fileData);
		fine.execute(!addDelay);	// file-lock without delay
		fine.execute(addDelay);		// file-lock with delay

		NoSharingProcessing noshare = new NoSharingProcessing(fileData);
		noshare.execute(!addDelay);	// no-sharing without delay
		noshare.execute(addDelay);	// no-sharing with delay
	}
	
/*
 * loadData : read file into ArrayList
 */
	public static ArrayList<String> loadData(String filePath) {
		
		ArrayList<String> fileData = new ArrayList<String>();
		String line = null;
		BufferedReader br = null;

		try {
			br = new BufferedReader(new FileReader(filePath));

			while ((line = br.readLine()) != null) {
				fileData.add(line);
			}
		} catch (Exception e) {

			e.printStackTrace();

		} finally {

			if (br != null) {

				try {
					br.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return fileData;
	}
}
