package ch.completion;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 
 * A Proxy pool for {@link ForkJoinPool}.<br>
 * <br>
 * It is optimized for the execution time of the tasks it's given, along with the other pools it shares its {@link Config} with.<br>
 * 
 * @author
 * 
 */
public class OptimizedTaskPool {

	/**
	 * 
	 * Config to be used to initialize and shared by {@link OptimizedTaskPool}s.<br>
	 * <br>
	 * It allows to specify their optimization settings.
	 * 
	 * @author
	 *
	 */
	public static class Config {

		/**
		 * The number of calls per options before optimization.
		 */
		public static final int OPTIMISATION_TEMPO = 50;

		/**
		 * The step between the current default parallelism level and the other optimization test option(s).
		 */
		private static final int OPTIMIZATION_STEP = 1;

		/**
		 * The max nb-of-threads to respect on a given task = sum of all thread pools' sizes on that task.
		 */
		private final int maxNbThreads;

		/**
		 * The acceptable {@link #maxNbThreads} max nb-of-threads margin-coefficient before {@link RunTimeException} if {@link #maxExecutionTime} exceeded.
		 */
		public final double maxNbThreadsMargin; // TODO properties

		/**
		 * The {@link ForkJoinPool}s' acceptable max last-execution-time before {@link RunTimeException} if acceptable {@link #maxNbThreads} exceeded.
		 */
		public final int maxExecutionTime; // TODO properties

		/**
		 * The task name for logging purposes.
		 */
		private final String taskName;

		private final Optimizer optimizer;

		/**
		 * 
		 * {@link Config} constructor.
		 * 
		 * @param maxNbThreads The max nb-of-threads to respect on a given task = sum of all thread pools' sizes on that task
		 * @param taskName The task name for logging purposes
		 */
		public Config(int maxNbThreads, String taskName) {
			this.maxNbThreads = maxNbThreads;
			this.taskName = taskName;
			this.maxNbThreadsMargin = 1.2; // TODO properties
			this.maxExecutionTime = 5000; // TODO properties
			this.optimizer = new Optimizer(this);
		}

		/**
		 * @return The max nb-of-threads to respect on a given task = sum of all thread pools' sizes on that task
		 */
		public int getMaxNbThreads() {
			return maxNbThreads;
		}

		/**
		 * @return The task name for logging purposes
		 */
		public String getTaskName() {
			return taskName;
		}

		/**
		 * @return The optimizer for the {@link OptimizedTaskPool}s sharing this {@link Config}
		 */
		public Optimizer getOptimizer() {
			return optimizer;
		}

		/**
		 * @return the acceptable {@link #maxNbThreads} max nb-of-threads margin-coefficient before {@link RunTimeException} if {@link #maxExecutionTime}
		 *         exceeded.
		 */
		public double getMaxNbThreadsMargin() {
			return maxNbThreadsMargin;
		}

		/**
		 * @return the {@link ForkJoinPool}s' acceptable max last-execution-time before {@link RunTimeException} if acceptable {@link #maxNbThreads}
		 *         exceeded.
		 */
		public int getMaxExecutionTime() {
			return maxExecutionTime;
		}
	}

	/**
	 * 
	 * Optimizer for {@link OptimizedTaskPool}s sharing the same {@link Config}.
	 * 
	 * @author
	 *
	 */
	protected static class Optimizer {

		/**
		 * The {@link Config} shared by all the {@link OptimizedTaskPool}s to be optimized by this Optimizer.
		 */
		private final Config config;

		/**
		 * The current default parallelism level.<br>
		 * <br>
		 * DO NOT CHANGE DEFAULT VALUE OF 1 (ONE)!! <br>
		 * Otherwise risks of {@link OutOfMemoryError} if {@link OptimizedTaskPool#shutdown} not properly called to optimize.
		 */
		private int parallelism = 1;

		/**
		 * Tracks max asked parallelism so far.
		 */
		private int maxAskedParallelism = 1;

		/**
		 * The last execution time for this task.
		 */
		private long lastExecutionTime = 0;

		/**
		 * The current nb-of-threads on this task.
		 */
		private final AtomicLong currentNbThreads = new AtomicLong(0);

		/**
		 * Map used for optimization.
		 */
		private final Map<Integer, List<Long>> parallelismToExecutionTimes = new ConcurrentHashMap<>();

		/**
		 * {@link Optimizer} constructor.
		 * 
		 * @param config The {@link Config} shared by all the {@link OptimizedTaskPool}s to be optimized by this Optimizer.
		 */
		public Optimizer(Config config) {
			this.config = config;
			if (parallelism != 1) {
				throw new IllegalArgumentException(
						String.format("%s OptimizedTaskPool.Optimizer.parallelism's default value MUST BE 1", config.getTaskName()));
			}
			reinitialize();
		}

		/**
		 * Re-initilize optimization for next test options. Its reset the optimization Map.
		 */
		private void reinitialize() {
			parallelismToExecutionTimes.clear();
			parallelismToExecutionTimes.put(parallelism + Config.OPTIMIZATION_STEP, new ArrayList<>());
			parallelismToExecutionTimes.put(parallelism, new ArrayList<>());
			if (parallelism > 1)
				parallelismToExecutionTimes.put(parallelism - Config.OPTIMIZATION_STEP, new ArrayList<>());
		}

		/**
		 * Calculates the acceptable pool-size for occurrence of this task given current circumstances.
		 * 
		 * @param askedSize Ideal pool-size if possible
		 * @return The acceptable pool-size
		 * @throws RuntimeException if {@link #maxNbThreadsMargin} * {@link #maxNbThreads} and {@link #maxExecutionTime} exceeded
		 */
		private int calculateSize(int askedSize) {

			long currentNb = currentNbThreads.get();

			if (thresholdsExcedded())
				throw new RuntimeException(String.format("%s: too many requests or/and maximum execution time exceeded: %s/%s", config.taskName,
						currentNb, config.getMaxNbThreads()));

			// update the max encountered asked-size so far if new bigger
			maxAskedParallelism = Math.max(askedSize, maxAskedParallelism);

			int size;

			// if asked-size more than double of current parallelism level and less than half of left available room, asked-size accepted
			// this allows to let exceptions pass through
			if ((askedSize / parallelism) > 2 && askedSize * 2 + currentNb < config.maxNbThreads) {
				size = askedSize;
			} else
			// if asked-size more than double of current parallelism level and less than of left available room left, half of asked-size accepted
			// this allows to let exceptions pass through in 2 "bulks"
			if ((askedSize / parallelism) > 2 && askedSize + currentNb < config.maxNbThreads) {
				size = askedSize / 2;
			} else {
				// lower parallelism level if max nb-of-threads threshold exceeded
				if (currentNb > config.maxNbThreads) {
					emergencyLowerParallelism();
				}
				// smallest between currently tested parallelism level and max encountered so far (this asked-size included)
				size = Math.min(parallelism, maxAskedParallelism);
				// never more than half of the left available room
				size = Math.min((int) ((double) (config.maxNbThreads - currentNb) / 2), size);
			}

			// never more than actually asked-size (list of 5 elements doesn't need pool of size > 5)
			size = Math.min(askedSize, size);
			// never 0 or less
			size = Math.max(1, size);

			// increment counter of nb-of-threads by the final calculated size
			currentNbThreads.addAndGet(size);

			return size;
		}

		/**
		 * @return true if max acceptable nb-of-threads and max execution-time exceeded
		 */
		private boolean thresholdsExcedded() {
			return currentNbThreads.get() > config.maxNbThreadsMargin && lastExecutionTime > config.maxExecutionTime
					|| currentNbThreads.get() > config.maxNbThreadsMargin * config.maxNbThreads;
		}

		/**
		 * Drastically lowers default parallelism level proportionally to ratio between {@link #maxNbThreads} and {@link #currentNbThreads} in case of
		 * exceeding {@link #maxNbThreadsMargin} * {@link #maxNbThreads}.
		 */
		private synchronized void emergencyLowerParallelism() {
			System.out.println(String.format("emergencyLowerParallelism %s", currentNbThreads.get()));
			parallelism = Math.max(1, (int) (parallelism * config.maxNbThreads / currentNbThreads.get()));
			reinitialize();
		}

		/**
		 * Processes the given metrics for optimization and updates current nb-of-threads on task.
		 * 
		 * @param time The execution time
		 * @param poolSize The pool-size
		 */
		private void processMetrics(long time, long poolSize) {
			lastExecutionTime = time;
			currentNbThreads.addAndGet(-poolSize);
			updateMapAndMaybeOptimizeNow(time);
		}

		/**
		 * 1. Records last execution time<br>
		 * 2. Selects other default parallelism level option if enough data recorded for current<br>
		 * 3. Does optimization to calculate next default parallelism level if all current options tested.<br>
		 * 
		 * @param time Execution to record for optimization
		 */
		private synchronized void updateMapAndMaybeOptimizeNow(Long time) {
			List<Long> list = parallelismToExecutionTimes.get(parallelism);
			if (list == null)
				return;
			list.add(time);
			if (list.size() >= OptimizedTaskPool.Config.OPTIMISATION_TEMPO) {
				Optional<Integer> optionalUntestedParallelism = parallelismToExecutionTimes.keySet().stream()
						.filter(key -> parallelismToExecutionTimes.get(key).size() == 0).findFirst();
				if (optionalUntestedParallelism.isPresent()) {
					parallelism = optionalUntestedParallelism.get();
				} else {
					optimize();
				}
			}
		}

		/**
		 * Executes optimization:<br>
		 * It finds the best parallelism level among the tested options according to their average execution times.
		 */
		private void optimize() {
			// calculate average execution time for each tested parallelism level
			Map<Integer, Double> parallelismToAverageExecutionTime = parallelismToExecutionTimes.entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().stream().mapToDouble(a -> a).average().getAsDouble()));

			// log results
			parallelismToAverageExecutionTime.entrySet().forEach(entry -> {
				System.out.println(String.format("%s -> %s", entry.getKey(), entry.getValue()));
			});

			// default parallelism level select at last optimization and used as base for this new one
			int previousParallelism = (int) parallelismToExecutionTimes.keySet().stream().mapToDouble(q -> q).average().getAsDouble();

			// best option for next optimization window
			Entry<Integer, Double> bestParallelismEntry = parallelismToAverageExecutionTime.entrySet().stream()
					.min((d1, d2) -> Double.compare(d1.getValue(), d2.getValue())).get();

			int bestParallelism = bestParallelismEntry.getKey();

			// scale proportionally to difference in average execution time to opportunistic maximization
			bestParallelism = previousParallelism
					+ (bestParallelism - previousParallelism) * Math.max(1, (int) ((double) parallelismToAverageExecutionTime.get(previousParallelism)
							/ parallelismToAverageExecutionTime.get(bestParallelism)));

			// don't unnecessarily exceed max encountered asked-size so far
			bestParallelism = Math.min(maxAskedParallelism, bestParallelism);
			// never 0 or less
			bestParallelism = Math.max(1, bestParallelism);
			// replace default parallelism level with new one
			parallelism = bestParallelism;

			// reset optimization context
			reinitialize();

			// log new default parallelism level
			System.out.println(
					String.format("%s\t(//)%s\t(nb)%s\t(ms)%s\n", config.taskName, parallelism, currentNbThreads, bestParallelismEntry.getValue()));
		}
	}

	/**
	 * Config shared with all {@link OptimizedTaskPool}s this {@link OptimizedTaskPool} should be optimized with.
	 */
	private final OptimizedTaskPool.Config config;

	/**
	 * The {@link ForkJoinPool} this proxy class encapsulates.
	 */
	private ForkJoinPool pool;

	/**
	 * The time at which this.
	 */
	private Long startTime;

	/**
	 * The definitive calculated optimized pool size.
	 */
	private final long poolSize;

	/**
	 * {@link OptimizedTaskPool} constructor.
	 * 
	 * @param askedSize The ideal pool-size if possible
	 * @param config Configuration for the task's pool
	 */
	public OptimizedTaskPool(int askedSize, OptimizedTaskPool.Config config) {
		this.poolSize = config.getOptimizer().calculateSize(askedSize);
		this.config = config;
	}

	/**
	 * Proxy method for {@link ForkJoinPool#submit(Callable)}. <br>
	 * <br>
	 * The {@link #shutdown} method MUST ABSOLUTELY BE CALLED after a submit(...), otherwise no optimization will be made and only single-threaded pools will
	 * be created.
	 * 
	 * @param <T> The Callable input type
	 * @param task The task to submit
	 * @return Future representing pending completion of the task
	 * @throws NullPointerException if the task is null
	 * @throws RejectedExecutionException if the task cannot be scheduled for execution
	 */
	public <T> ForkJoinTask<T> submit(Callable<T> task) {
		if (startTime != null) {
			throw new RuntimeException(
					String.format("%s: this OptimizedTaskPool has already been used. Please create/use another one.", config.getTaskName()));
		}
		this.pool = new ForkJoinPool((int) poolSize);
		this.startTime = System.currentTimeMillis();
		return pool.submit(task);
	}

	/**
	 * Proxy method for {@link ForkJoinPool#submit(ForkJoinTask)}. <br>
	 * <br>
	 * The {@link #shutdown} method MUST ABSOLUTELY BE CALLED after a submit(...), otherwise no optimization will be made and only single-threaded pools will
	 * be created.
	 * 
	 * @param task The task to submit
	 * @return Future representing pending completion of the task
	 * @throws NullPointerException if the task is null
	 * @throws RejectedExecutionException if the task cannot be scheduled for execution
	 */
	public <T> ForkJoinTask<T> submit(ForkJoinTask<T> task) {
		if (startTime != null) {
			throw new RuntimeException("This OptimizedTaskPool has already been used. Please create/use another one.");
		}
		this.pool = new ForkJoinPool((int) poolSize);
		this.startTime = System.currentTimeMillis();
		return pool.submit(task);
	}

	/**
	 * Proxy method for {@link ForkJoinPool#submit(Runnable)}. <br>
	 * <br>
	 * The {@link #shutdown} method MUST ABSOLUTELY BE CALLED after a submit(...), otherwise no optimization will be made and only single-threaded pools will
	 * be created.
	 * 
	 * @param task The task to submit
	 * @return Future representing pending completion of the task
	 * @throws NullPointerException if the task is null
	 * @throws RejectedExecutionException if the task cannot be scheduled for execution
	 */
	public ForkJoinTask<?> submit(Runnable task) {
		if (startTime != null) {
			throw new RuntimeException("This OptimizedTaskPool has already been used. Please create/use another one.");
		}
		this.pool = new ForkJoinPool((int) poolSize);
		this.startTime = System.currentTimeMillis();
		return pool.submit(task);
	}

	/**
	 * Proxy method for {@link ForkJoinPool#submit(Runnable)}. <br>
	 * <br>
	 * The {@link #shutdown} method MUST ABSOLUTELY BE CALLED after a submit(...), otherwise no optimization will be made and only single-threaded pools will
	 * be created.
	 * 
	 * @param task The task to submit
	 * @param result the result to return
	 * @return Future representing pending completion of the task
	 * @throws NullPointerException if the task is null
	 * @throws RejectedExecutionException if the task cannot be scheduled for execution
	 */
	public <T> ForkJoinTask<T> submit(Runnable task, T result) {
		if (startTime != null) {
			throw new RuntimeException("This OptimizedTaskPool has already been used. Please create/use another one.");
		}
		this.pool = new ForkJoinPool((int) poolSize);
		this.startTime = System.currentTimeMillis();
		return pool.submit(task, result);
	}

	/**
	 * Proxy method for {@link ForkJoinPool#shutdown}.<br>
	 * It allows to intercept the call to calculate metrics for optimization.
	 */
	public void shutdown() {
		if (startTime == null) {
			System.out.println("This OptimizedTaskPool has not been used. Shutting down anyway...");
		} else {
			Long time = System.currentTimeMillis() - startTime;
			config.getOptimizer().processMetrics(time, poolSize);
		}
		pool.shutdown();
	}
}