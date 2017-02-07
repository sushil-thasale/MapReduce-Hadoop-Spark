package src.common;

public class Delay {

	/*
	 * addDelay : simulates delay while updating a data structure
	 */
	public static void addDelay() {
		recursiveFibonacci(17);
	}

	/*
	 * recursiveFibonacci : computes fibonacci values for given number
	 * (recursive)
	 */
	private static int recursiveFibonacci(int n) {
		if (n == 0)
			return 0;
		else if (n == 1)
			return 1;

		return recursiveFibonacci(n - 1) + recursiveFibonacci(n - 2);
	}

	/*
	 * linearFibonacci : computes fibonacci values for given number (linear)
	 */
	private static void linearFibonacci(int n) {

		int x = 0;
		int y = 1;
		int z;
		for (int k = 2; k <= n; k++) {
			z = x + y;
			x = y;
			y = z;
		}
	}

}
