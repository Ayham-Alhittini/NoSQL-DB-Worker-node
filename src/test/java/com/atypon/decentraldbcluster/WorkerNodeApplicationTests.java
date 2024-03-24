package com.atypon.decentraldbcluster;


import com.atypon.decentraldbcluster.entity.IndexKey;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class WorkerNodeApplicationTests {

	@Test
	public void generalTest() throws Exception {
		String indexPath = "storage/58c2dde5-246a-474f-b10d-bcc5d62d224b/test/users/indexes/name.ser";

		ConcurrentSkipListMap<IndexKey, ConcurrentSkipListSet<String>> index;

		try (FileInputStream fileIn = new FileInputStream(indexPath);

			 ObjectInputStream in = new ObjectInputStream(fileIn)) {
			index = (ConcurrentSkipListMap<IndexKey, ConcurrentSkipListSet<String>>) in.readObject();
		}

		for (var i: index.entrySet()) {
			System.out.println(i.getKey() + ", " + i.getValue());
		}

	}

}
