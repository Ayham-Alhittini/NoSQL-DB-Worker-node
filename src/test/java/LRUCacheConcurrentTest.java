import com.atypon.decentraldbcluster.cache.datastructure.LRUCache;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class LRUCacheConcurrentTest {

    private final ExecutorService executor = Executors.newFixedThreadPool(10);

    @Test
    public void testConcurrency() throws InterruptedException, ExecutionException {
        final int numberOfIterations = 1000;
        final LRUCache<Integer, String> cache = new LRUCache<>(100); // Larger size to reduce the chance of collision

        List<Callable<Boolean>> tasks = new ArrayList<>();
        for (int i = 0; i < numberOfIterations; i++) {
            int finalI = i;
            tasks.add(() -> {
                // Put new items
                cache.put(finalI, "Value" + finalI);

                // Get items to ensure they return the correct value
                String value = cache.get(finalI);
                return ("Value" + finalI).equals(value);
            });
        }

        List<Future<Boolean>> results = executor.invokeAll(tasks);
        for (Future<Boolean> result : results) {
            assertTrue(result.get());
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        // Final size check to ensure no more than 100 items exist
        assertTrue(cache.size() <= 100);
    }
}
