package com.atypon.decentraldbcluster;

import com.atypon.decentraldbcluster.exceptions.ResourceNotFoundException;
import org.junit.jupiter.api.Test;

public class WorkerNodeApplicationTests {

	@Test
	public void generalTest() {
		holder();
	}

	private void holder() {
		thrower();
	}

	private void thrower() throws ResourceNotFoundException {
		throw new ResourceNotFoundException();
	}


}

