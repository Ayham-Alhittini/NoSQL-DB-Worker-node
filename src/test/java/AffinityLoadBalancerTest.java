import com.atypon.decentraldbcluster.communication.affinity.balancer.RoundRobinLoadBalancer;
import com.atypon.decentraldbcluster.communication.config.NodeCommunicationConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AffinityLoadBalancerTest {

    @Mock
    private NodeCommunicationConfiguration config;

    private RoundRobinLoadBalancer loadBalancer;

    @Before
    public void setUp() {
        // Mock the static method to return a fixed cluster size of 3
        Mockito.mockStatic(NodeCommunicationConfiguration.class);
        when(NodeCommunicationConfiguration.getClusterNodeSize()).thenReturn(3);

        // Initialize the load balancer
        loadBalancer = new RoundRobinLoadBalancer();
    }

    @Test
    public void testGetNextNodeNumber_CyclesThroughNodesCorrectly() {
        assertEquals(1, loadBalancer.getNextAffinityNodeNumber());
        assertEquals(2, loadBalancer.getNextAffinityNodeNumber());
        assertEquals(3, loadBalancer.getNextAffinityNodeNumber());
        // Test wrapping around
        assertEquals(1, loadBalancer.getNextAffinityNodeNumber());
    }
}
