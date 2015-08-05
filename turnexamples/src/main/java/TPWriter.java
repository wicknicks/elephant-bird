import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import com.google.common.collect.Maps;
import com.twitter.elephantbird.examples.proto.Examples;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockWriter;

/**
 * Copyright (C) 2014 Turn Inc. All Rights Reserved.
 * Proprietary and confidential.
 */

public class TPWriter {

	public void writeAll() throws IOException {
		Random generator = new Random();
		String[] strs = new String[] {"arj", "tom", "pya", "slu"};

		HashMap<String, Integer> map = Maps.newHashMap();
		for (String s: strs) map.put(s, 0);

		FileOutputStream fos = new FileOutputStream("/home/asatish/Downloads/elephant-proto.pb");
		ProtobufBlockWriter<Examples.Age> pbWriter =
				new ProtobufBlockWriter<Examples.Age>(fos, Examples.Age.class);

		for (int i=0; i<1000; i++) {
			String name = strs[generator.nextInt(strs.length)];
			int a = generator.nextInt(10);
			Examples.Age age = Examples.Age.newBuilder()
					.setAge(a)
					.setName(name)
					.build();
			pbWriter.write(age);
			map.put(name, map.get(name) + a);
		}

		pbWriter.finish();
		pbWriter.close();

		System.out.println(map);
	}

	public static void main(String[] args) throws IOException {
		System.loadLibrary("gplcompression");
		TPWriter tpWriter = new TPWriter();
		tpWriter.writeAll();
	}
}
