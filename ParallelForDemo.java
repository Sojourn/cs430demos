import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Joel Rausch
 */
public class ParallelForDemo {

	/**
	 * Apply an operation for each of the parameters in parallel.
	 * 
	 * @param parameters
	 * @param operation
	 */
	public static <T> void forEach(Collection<T> parameters,
			final Operation<T> operation) {

		// Number of threads in executor is the number of processors
		int nThreads = Runtime.getRuntime().availableProcessors();
		ExecutorService exec = Executors.newFixedThreadPool(nThreads);

		// Used to block until all iterations have completed
		final CountDownLatch latch = new CountDownLatch(parameters.size());

		// Run iterations on the executor
		for (final T parameter : parameters) {
			exec.submit(new Runnable() {

				@Override
				public void run() {
					operation.apply(parameter);
					latch.countDown();
				}

			});
		}

		// Wait for the 'loop' to complete
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			exec.shutdown();
		}
	}

	/**
	 * A generic operation, taking a single parameter.
	 * 
	 * @param <T>
	 */
	public interface Operation<T> {
		void apply(final T parameter);
	}
	
	public static void main(String[] args) {

		// Gives a reference to an integer
		class TestClass {
			public Integer value;

			public TestClass(int value) {
				this.value = value;
			}
		}

		// Create a collection of parameters
		List<TestClass> parameters = new LinkedList<TestClass>();
		for (int i = 0; i < 1024 * 1024; i++) {
			parameters.add(new TestClass(i));
		}

		// Roughly profile the time it takes to run the loop in parallel
		long startTime = System.currentTimeMillis();
		forEach(parameters, new Operation<TestClass>() {

			@Override
			public void apply(TestClass parameter) {
				parameter.value++;
			}

		});

		long endTime = System.currentTimeMillis();
		System.out.println("Processed " + parameters.size() + " iterations in "
				+ (endTime - startTime) + "ms.");
		
		// Verify the results
		int i = 1;
		for (TestClass parameter : parameters) {
			if (parameter.value != i) {
				throw new RuntimeException("Illegal parameter value: " + i
						+ "!=" + parameter.value);
			}
			
			i++;
		}
	}
}
