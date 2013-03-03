import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

/**
 * A parallel merge-sort implementation which uses the java.util.concurrent
 * Fork/Join framework.
 *
 * @author Joel Rausch
 */
public class ForkJoinDemo {
	
	/**
	 * Sort the array in parallel using merge-sort and the Fork/Join framework.
	 * 
	 * @param array Array to be sorted.
	 * @param c Comparator.
	 */
	public static <T> void parallelMergeSort(T[] array, Comparator<? super T> c) {

		@SuppressWarnings({ "serial", "hiding" })
		class MergeSortAction<T> extends RecursiveAction {

			private final T[] array;
			private final T[] mergeArray;
			private final int start;
			private final int end;
			private final Comparator<? super T> c;

			/**
			 * Called once by mergeSort.
			 * 
			 * @param array Array to be sorted.
			 * @param start Start index (inclusive).
			 * @param end End index (exclusive).
			 * @param c Comparator to be used.
			 */
			@SuppressWarnings("unchecked")
			public MergeSortAction(T[] array, int start, int end,
					Comparator<? super T> c) {
				this(array, (T[]) new Object[array.length], start, end, c);
			}

			/**
			 * Called internally. This allows the merge array to be reused
			 * by all instances working on the same problem, while still
			 * being reentrant.
			 * 
			 * @param array Array to be sorted.
			 * @param mergeArray Extra memory for merging; can use [start, end).
			 * @param start Start index (inclusive).
			 * @param end End index (exclusive).
			 * @param c Comparator to use.
			 */
			private MergeSortAction(T[] array, T[] mergeArray, int start,
					int end, Comparator<? super T> c) {
				this.array = array;
				this.mergeArray = mergeArray;
				this.start = start;
				this.end = end;
				this.c = c;
			}

			/**
			 * Divide-and-conquer this sorting problem, or solve directly if
			 * the problem size has been reduced sufficiently.
			 */
			@Override
			protected void compute() {

				// Solve directly if problem is small (arbitrary threshold)
				if ((end - start) < (4 * 1024)) {

					// Use the built-in sort
					Arrays.sort(array, start, end, c);

				// Otherwise use divide-and-conquer approach
				} else {

					// Create two sub-problems
					int mid = (start + end) / 2;
					MergeSortAction<T> lowerAction = new MergeSortAction<T>(
							array, mergeArray, start, mid, c);
					MergeSortAction<T> upperAction = new MergeSortAction<T>(
							array, mergeArray, mid, end, c);

					// Run both sub-problems and wait for them to complete
					invokeAll(lowerAction, upperAction);

					// Merge the results of the sub-problems
					merge(mid);
				}
			}

			/**
			 * Helper method for merging the two sub-arrays: [start, mid) and [mid, end).
			 */
			private void merge(int mid) {

				// Lower sorted array index
				int i = start;

				// Upper sorted array index
				int j = mid;

				// Merge array index
				int k = start;

				// Merge the next smallest element from either array
				while ((i < mid) && (j < end)) {
					T lowerObject = array[i];
					T upperObject = array[j];

					if (c.compare(lowerObject, upperObject) <= 0) {
						mergeArray[k] = lowerObject;
						i++;
					} else {
						mergeArray[k] = upperObject;
						j++;
					}

					k++;
				}

				// Finish merging the lower sorted array
				while (i < mid) {
					mergeArray[k] = array[i];
					k++;
					i++;
				}

				// Finish merging the upper sorted array
				while (j < end) {
					mergeArray[k] = array[j];
					k++;
					j++;
				}

				// Write the merged contents back to the array
				System.arraycopy(mergeArray, start, array, start, (end - start));
			}
		}

		if (array.length > 0) {

			// Create the fork-join worker thread pool
			ForkJoinPool pool = new ForkJoinPool();

			// Invoke the top-level problem
			pool.invoke(new MergeSortAction<T>(array, 0, array.length, c));
		}
	}

	public static void main(String[] args) {

		// Initialize an array with random values
		Random rand = new Random(System.currentTimeMillis());
		Integer[] input = new Integer[1024 * 1024 * 32];
		for (int i = 0; i < input.length; i++) {
			input[i] = new Integer(rand.nextInt());
		}

		// Roughly profile the time it takes to sort the array
		long startTime = System.currentTimeMillis();
		parallelMergeSort(input, new Comparator<Integer>() {

			@Override
			public int compare(Integer o1, Integer o2) {

				// Because "return o1 - o2;" will overflow
				if (o1 > o2) {
					return 1;
				} else if (o1 < o2) {
					return -1;
				} else {
					return 0;
				}
			}

		});

		// Un-comment this for comparison
		// Arrays.sort(input, null);

		long endTime = System.currentTimeMillis();
		System.out.println("Sorted " + input.length + " integers in "
				+ (endTime - startTime) + "ms.");

		// Verify the array is now sorted with an inductive test
		int last_int = Integer.MIN_VALUE;
		for (int i = 0; i < input.length; i++) {
			if (input[i] < last_int) {
				throw new RuntimeException("Out-of-order[" + (i - 1) + ", " + i
						+ "]: " + input[i - 1] + " > " + input[i]);
			} else {
				last_int = input[i];
			}
		}
	}
}
