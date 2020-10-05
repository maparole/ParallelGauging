package ch.completion;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ParallelGauging {

	static final int MAX_CALLS_INTERVAL = 100;

	static final int SERVICE_POOL_SIZE = 200;

	static final int SERVICE_MIN_EXECUTION_TIME = 50;

	static final int SERVICE_RANDOM_EXECUTION_TIME_VARIATION = 50;

	static final double SERVICE_EXECUTION_TIME_VARIATION_FOR_CURRENT_LOAD_DIVIDER = 2;

	static Service service = new Service();

	static final int FIRST_MAX_NB_THEADS = 1000; // TODO properties
	static final int SECOND_MAX_NB_THEADS = 1000; // TODO properties

	static OptimizedTaskPool.Config firstLevelConfig = new OptimizedTaskPool.Config(FIRST_MAX_NB_THEADS, "FIRST");
	static OptimizedTaskPool.Config secondLevelConfig = new OptimizedTaskPool.Config(SECOND_MAX_NB_THEADS, "SECOND");

	public static void main(String[] args) {

		System.out.println();

		System.out.println("Average nbCalls/s: " + (int) (1000 / (0.5 * MAX_CALLS_INTERVAL)));
		System.out.println();

		System.out.println(String.format("Underlying service pool-size: %s threads", SERVICE_POOL_SIZE));
		System.out.println(String.format("Underlying service min execution-time: %sms", SERVICE_MIN_EXECUTION_TIME));
		System.out.println(String.format("Underlying service max execution-time random-positive-variation: %sms", SERVICE_MIN_EXECUTION_TIME));
		System.out.println(String.format("Underlying service max execution-time variation under load: %sms",
				(int) (SERVICE_POOL_SIZE / SERVICE_EXECUTION_TIME_VARIATION_FOR_CURRENT_LOAD_DIVIDER)));
		System.out.println();

		System.out.println(String.format("ForkJoin pools optimisation tempo: %s calls", OptimizedTaskPool.Config.OPTIMISATION_TEMPO * 3));
		System.out.println(String.format("ForkJoin pools acceptable max nb-of-threads margin-coefficient before RunTimeException: %s",
				firstLevelConfig.getMaxNbThreadsMargin()));
		System.out.println(String.format("ForkJoin pools acceptable max last-execution-time before RunTimeException: %sms",
				firstLevelConfig.getMaxExecutionTime()));
		System.out.println();

		System.out.println(String.format("%s level pool max nb-of-threads: %s", secondLevelConfig.getTaskName(), secondLevelConfig.getMaxNbThreads()));
		System.out.println(String.format("%s level pool max nb-of-threads: %s", firstLevelConfig.getTaskName(), firstLevelConfig.getMaxNbThreads()));
		System.out.println();

		while (true) {
			try {
				Thread.sleep((long) (Math.random() * MAX_CALLS_INTERVAL));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			new Thread(() -> {
				firstLevels();
			}).start();
		}
	}

	static List<Integer> firstLevels() {

		List<Integer> testList = IntStream.range(0, (int) (Math.exp(Math.random() * Math.random() * Math.random() * 4) * 3)).boxed()
				.collect(Collectors.toList());

		///
		OptimizedTaskPool pool = new OptimizedTaskPool(testList.size(), firstLevelConfig);

		List<Integer> result = pool.submit(() -> testList.parallelStream().map(item -> {
			return secondLevel().stream().reduce(Integer::sum).get();
		}).collect(Collectors.toList())).join();

		///
		pool.shutdown();

		return result;
	}

	static List<Integer> secondLevel() {

		List<Integer> testList = IntStream.range(0, (int) (Math.exp(Math.random() * Math.random() * Math.random() * 4) * 3)).boxed()
				.collect(Collectors.toList());

		///
		OptimizedTaskPool pool = new OptimizedTaskPool(testList.size(), secondLevelConfig);

		List<Integer> result = pool.submit(() -> testList.parallelStream().map(item -> {
			return service.doThis(itemy -> itemy * 10, item);
		}).collect(Collectors.toList())).join();

		///
		pool.shutdown();

		return result;
	}

	static class Service {

		static final int connectionPoolSize = SERVICE_POOL_SIZE;

		final AtomicLong nbCurrent = new AtomicLong(0);

		<I, O> O doThis(Function<I, O> func, I input) {
			while (nbCurrent.get() >= connectionPoolSize) {
				try {
					Thread.sleep(2);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			nbCurrent.incrementAndGet();

			int sleepTime = SERVICE_MIN_EXECUTION_TIME + (int) (Math.random() * SERVICE_RANDOM_EXECUTION_TIME_VARIATION)
					+ (int) (nbCurrent.doubleValue() / SERVICE_EXECUTION_TIME_VARIATION_FOR_CURRENT_LOAD_DIVIDER);
			try {
				Thread.sleep(sleepTime);
				// System.out.println(sleepTime + "\ttask\t" + input.toString() + "\t" + Thread.currentThread().getId());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			nbCurrent.decrementAndGet();
			return func.apply(input);
		}
	}
}
