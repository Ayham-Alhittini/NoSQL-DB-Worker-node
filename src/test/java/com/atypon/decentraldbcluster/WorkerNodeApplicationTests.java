package com.atypon.decentraldbcluster;


import org.junit.Test;

import java.util.concurrent.ConcurrentSkipListMap;

public class WorkerNodeApplicationTests {

	@Test
	public void generalTest() {

		ConcurrentSkipListMap<String, String> index = new ConcurrentSkipListMap<>();

//		index.put("Ayham", "ayham");
		index.put("Mulham", "mulham");
		index.put("Menas", "menas");


		System.out.println(index.computeIfAbsent("Ayham", x -> "ayham"));
		System.out.println(index.computeIfAbsent("Ayham", x -> "ayham2"));
	}


}
