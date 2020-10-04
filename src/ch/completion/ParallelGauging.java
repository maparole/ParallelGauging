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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ParallelGauging {

	private static final int maxCallsInterval = 100;

	private static final int servicePoolSize = 200;

	private static final int serviceMinimumResponseTime = 50;

	private static final double serviceResponseTimeVariationForCurrentLoadQuotient = 2;

	static CustomPool.Config firstLevelConfig = new CustomPool.Config(1000, "FIRST");
	static CustomPool.Config secondLevelConfig = new CustomPool.Config(1000, "SECOND");

	static Service service = new Service();

	public static void main(String[] args) {

		System.out.println();
		System.out.println("average nbCalls/s: " + (1000.0 / maxCallsInterval));
		System.out.println();
		System.out.println("servicePoolSize: " + servicePoolSize);
		System.out.println("serviceMinimumResponseTime: " + serviceMinimumResponseTime);
		System.out.println("serviceResponseTimeVariationForCurrentLoadQuotient: " + serviceResponseTimeVariationForCurrentLoadQuotient);
		System.out.println();
		System.out.println(secondLevelConfig.taskName + " level's maxNbThreads: " + secondLevelConfig.maxNbThreads);
		System.out.println(firstLevelConfig.taskName + " level's maxNbThreads: " + firstLevelConfig.maxNbThreads);
		System.out.println();

		while (true) {
			try {
				Thread.sleep((long) (Math.random() * maxCallsInterval));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			new Thread(() -> {
				firstLevels();
			}).start();
		}
	}

	static List<Integer> secondLevel() {

		List<Integer> testList = IntStream.range(0, (int) (Math.exp(Math.random() * Math.random() * Math.random() * 4) * 3)).boxed()
				.collect(Collectors.toList());

		///
		CustomPool pool = new CustomPool(testList.size(), secondLevelConfig);

		List<Integer> result = pool.submit(() -> testList.parallelStream().map(item -> {
			return service.doThis(itemy -> itemy * 10, item);
		}).collect(Collectors.toList())).join();

		///
		pool.shutdown();

		return result;
	}

	static List<Integer> firstLevels() {

		List<Integer> testList = IntStream.range(0, (int) (Math.exp(Math.random() * Math.random() * Math.random() * 4) * 3)).boxed()
				.collect(Collectors.toList());

		///
		CustomPool pool = new CustomPool(testList.size(), firstLevelConfig);

		List<Integer> result = pool.submit(() -> testList.parallelStream().map(item -> {
			return secondLevel().stream().reduce(Integer::sum).get();
		}).collect(Collectors.toList())).join();

		///
		pool.shutdown();

		return result;
	}

	static class Service {

		static final int connectionPoolSize = servicePoolSize;

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

			int sleepTime = serviceMinimumResponseTime + (int) (Math.random() * 30)
					+ (int) (nbCurrent.doubleValue() / serviceResponseTimeVariationForCurrentLoadQuotient);
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

	public static class CustomPool {

		private static final int STEP = 1;

		private final CustomPool.Config config;
		private final ForkJoinPool pool;
		private final long start;
		private final long size;

		public static class Config {

			private static final int tempo = 50;

			private static final double margin = 1.2;

			private static final int maxLastResponseTime = 3000;

			private int parallelism = 10;
			private int maxNeededParallelism = 1;

			private long lastResponseTime = 0;

			private final int maxNbThreads;
			private final String taskName;
			private final AtomicLong currentNbThreads = new AtomicLong(0);
			private final Map<Integer, List<Long>> parallelismToResponseTimes = new ConcurrentHashMap<>();

			public Config(int maxNbThreads, String taskName) {
				this.maxNbThreads = maxNbThreads;
				this.taskName = taskName;
				this.resetMap();
			}

			private void resetMap() {
				parallelismToResponseTimes.clear();
				parallelismToResponseTimes.put(parallelism + STEP, new ArrayList<>());
				parallelismToResponseTimes.put(parallelism, new ArrayList<>());
				if (parallelism > 1)
					parallelismToResponseTimes.put(parallelism - STEP, new ArrayList<>());
			}

			synchronized void processMap(Long time) {
				List<Long> list = parallelismToResponseTimes.get(parallelism);
				if (list == null)
					return;
				list.add(time);
				if (list.size() >= CustomPool.Config.tempo) {
					Optional<Integer> optionalUntestedParallelism = parallelismToResponseTimes.keySet().stream()
							.filter(key -> parallelismToResponseTimes.get(key).size() == 0).findFirst();
					if (optionalUntestedParallelism.isPresent()) {
						parallelism = optionalUntestedParallelism.get();
					} else {
						Map<Integer, Double> parallelismToAverageResponseTime = parallelismToResponseTimes.entrySet().stream().collect(
								Collectors.toMap(Map.Entry::getKey, e -> e.getValue().stream().mapToDouble(a -> a).average().getAsDouble()));
						parallelismToAverageResponseTime.entrySet().forEach(entry -> {
							System.out.println(entry.getKey() + " -> " + entry.getValue());
						});
						int previousParallelism = (int) parallelismToResponseTimes.keySet().stream().mapToDouble(q -> q).average().getAsDouble();
						Entry<Integer, Double> bestParallelismEntry = parallelismToAverageResponseTime.entrySet().stream()
								.min((d1, d2) -> Double.compare(d1.getValue(), d2.getValue())).get();
						int bestParallelism = bestParallelismEntry.getKey();
						bestParallelism = previousParallelism + (bestParallelism - previousParallelism)
								* Math.max(1, (int) ((double) parallelismToAverageResponseTime.get(previousParallelism)
										/ parallelismToAverageResponseTime.get(bestParallelism)));
						bestParallelism = Math.min(maxNeededParallelism, bestParallelism);
						bestParallelism = Math.max(1, bestParallelism);
						parallelism = bestParallelism;
						resetMap();
						System.out.println(taskName + "\t(//)" + parallelism + "\t(nb)" + currentNbThreads + "\t(ms)"
								+ bestParallelismEntry.getValue() + "\n");
					}
				}
			}

			synchronized void emergencyLowerParallelism() {
				System.out.println("emergencyLowerParallelism " + currentNbThreads.get());
				parallelism = Math.max(1, (int) (parallelism * maxNbThreads / currentNbThreads.get()));
				resetMap();
			}

			int calculateSize(int neededSize) {
				if (currentNbThreads.get() > margin * maxNbThreads && lastResponseTime > maxLastResponseTime)
					throw new RuntimeException(taskName + "\t(//)" + "Too many requests");
				maxNeededParallelism = Math.max(neededSize, maxNeededParallelism);
				long currentNb = currentNbThreads.get();
				int size;
				if ((neededSize / parallelism) > 2 && neededSize * 2 + currentNb < maxNbThreads) {
					size = neededSize;
				} else if ((neededSize / parallelism) > 2 && neededSize + currentNb < maxNbThreads) {
					size = neededSize / 2;
				} else {
					if (currentNb > maxNbThreads) {
						emergencyLowerParallelism();
					}
					size = Math.min(parallelism, maxNeededParallelism);
					size = Math.min((int) ((double) (maxNbThreads - currentNb) / 2), size);
				}
				size = Math.min(neededSize, size);
				size = Math.max(1, size);
				currentNbThreads.addAndGet(size);
				return size;
			}

			void processMetrics(long time, long size) {
				lastResponseTime = time;
				currentNbThreads.addAndGet(-size);
				processMap(time);
				// System.out.println("time " + time);
			}
		}

		public CustomPool(int neededSize, CustomPool.Config config) {
			this.config = config;
			this.size = config.calculateSize(neededSize);
			this.pool = new ForkJoinPool((int) size);
			this.start = System.currentTimeMillis();
		}

		public <I, O> ForkJoinTask<O> submit(Callable<O> task) {
			return pool.submit(task);
		}

		public void shutdown() {
			Long time = System.currentTimeMillis() - start;
			config.processMetrics(time, size);
			pool.shutdown();
		}
	}
}
