package com.fpl.spark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class fileReadingUnicode {

	public static void main(String[] args) {
		try {
			File fileDir = new File("C:/Users/sxk0hg8/workspace/scala/sparkPOCs/src/main/java/com/fpl/spark/telugu.txt");

			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(fileDir), "UTF8"));
			//BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("")));
			String str;

			while ((str = in.readLine()) != null) {
				System.out.println(str);
			}

			in.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
