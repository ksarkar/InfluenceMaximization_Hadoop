package edu.ame.rankloud.test.model.lt;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Before;
import org.junit.Test;

import edu.ame.rankloud.model.lt.InitModelReducer;
import edu.ame.rankloud.model.lt.UWLTNodeData;
import edu.ame.rankloud.util.NeighborArrayWritable;
import edu.ame.rankloud.util.NeighborWritable;


/**
 * @author ksarkar1
 *
 */

public class UWLTInitReducerTest {

	private Text key;
	private Iterator<NeighborWritable> values;
	private UWLTNodeData nodeData;
	private InitModelReducer reducer;

	/**
	 * Set up the states for calling the map function
	 */
	@Before
	public void setUp() throws Exception {
		key = new Text("1001");
		NeighborWritable[] neighbors = new NeighborWritable[4];
		for (int i = 0; i < 4; i++) {
			neighbors[i] = new NeighborWritable(new Text("300" + i), new DoubleWritable((double) 1 / (1 + i)));
		}
		
		values = Arrays.asList(neighbors).iterator();
		
		nodeData = new UWLTNodeData(key,
									new NeighborArrayWritable(neighbors),
									new DoubleWritable(0.0),
									new BooleanWritable(false));
		
		reducer = new InitModelReducer();

	}

	/**
	 * TestUWLT method for InitModelMapper#map - valid input
	 */
	@Test
	public void testMapValid() {
		
		// mock the output object
		OutputCollector<Text, UWLTNodeData> output = mock(OutputCollector.class);
		
		try {
			// call the API
			reducer.reduce(key, values, output, null);
			
			// in order (sequential) verification of the calls to output.collect()
			verify(output).collect(key, nodeData);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}

