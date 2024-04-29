import com.atypon.decentraldbcluster.index.Index;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class IndexTest {
    private Index index;
    private JsonNode jsonNode;

    @Before
    public void setUp() throws Exception {
        index = new Index();
        ObjectMapper mapper = new ObjectMapper();
        String jsonString = "{\"name\":\"test\"}";
        jsonNode = mapper.readTree(jsonString);
    }

    @Test
    public void testAddAndRetrievePointers() {
        String pointer = "pointer1";
        index.addPointer(jsonNode, pointer);

        assertTrue(index.getPointers(jsonNode).contains(pointer));
    }

    @Test
    public void testRemovePointer() {
        String pointer = "pointer2";
        index.addPointer(jsonNode, pointer);

        index.removePointer(jsonNode, pointer);

        assertFalse(index.getPointers(jsonNode).contains(pointer));
        assertTrue(index.getPointers(jsonNode).isEmpty());
    }

    @Test
    public void testConcurrency() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(10);
        Runnable addTask = () -> index.addPointer(jsonNode, "commonPointer");
        Runnable removeTask = () -> index.removePointer(jsonNode, "commonPointer");

        for (int i = 0; i < 100; i++) {
            executor.submit(addTask);
        }
        for (int i = 0; i < 100; i++) {
            executor.submit(removeTask);
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        assertTrue(index.getPointers(jsonNode).isEmpty());
    }
}
