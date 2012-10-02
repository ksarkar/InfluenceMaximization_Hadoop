package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import edu.ame.rankloud.old.simulation.init.NeighborWritable;

public class Test {

	public static void main(String[] args) throws IOException {
	/*	
		// ArrayList test
		ArrayList<NeighborWritable> list = new ArrayList<NeighborWritable>();
		
		for (int i = 0; i < 5; i++) {
			list.add(new NeighborWritable(new Text("100" + i), new DoubleWritable((double)1/i)));
		}
		
		NeighborWritable n[] = list.toArray(new NeighborWritable[0]);
		System.out.println("Results:");
		for (int i =0; i < n.length; i++){
			System.out.println("NodeId: " + n[i].getNodeId().toString() + "\t\t" + "Weight: " + n[i].getWeight().get());
		}
*/
		// random access file test
/*		RandomAccessFile f = null;
		File file = new File("./data/test.txt");
		file.delete();
		
        try {
			f = new RandomAccessFile(file, "rw");
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        long pos = 0;
		try {
			
			for (int i = 0; i < 10; i++) {
	        	f.writeBytes("500" + i + "\n");
	        }
			pos = f.length();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (f != null) {
				try {
					f.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
        
        System.out.println("Length of file: " + pos);
        
        try {
			f = new RandomAccessFile("./data/test.txt", "rw");
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			f.seek(pos);
			f.writeBytes("5010\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				f.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try {
			f = new RandomAccessFile("./data/test.txt", "rw");
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			f.seek(pos);
			long new_pos = f.length();
			for (long i = pos; i < new_pos; i++) {
				f.writeByte(' ');
			}
			f.seek(pos);
			f.writeBytes("6\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				f.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}*/
		
		BufferedReader reader = new BufferedReader(new FileReader("./data/seed_list.txt"));
		int lines = 0;
		while (reader.readLine() != null) lines++;
		reader.close();
		System.out.println("number of seeds: " + lines);
		
	}
}
