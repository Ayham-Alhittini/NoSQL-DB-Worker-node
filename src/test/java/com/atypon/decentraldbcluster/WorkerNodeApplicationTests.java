package com.atypon.decentraldbcluster;

import org.junit.jupiter.api.Test;

public class WorkerNodeApplicationTests {

	@Test
	public void generalTest() {
		String myName = "My name is ";
		myName = assignName(myName);
		System.out.println(myName);
	}

	private String assignName(String statement) {
		return statement + " Ayham";
	}




}

