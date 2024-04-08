package com.atypon.decentraldbcluster;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class WorkerNodeTests {

	@Test
	public void Main() {
		int maxSize = 2;
		AtomicInteger x = new AtomicInteger(0);


		System.out.println(x.getAndUpdate(i -> (i + 1) % maxSize) + 1);
		System.out.println(x.getAndUpdate(i -> (i + 1) % maxSize) + 1);
		System.out.println(x.getAndUpdate(i -> (i + 1) % maxSize) + 1);
		System.out.println(x.getAndUpdate(i -> (i + 1) % maxSize) + 1);

	}

}

