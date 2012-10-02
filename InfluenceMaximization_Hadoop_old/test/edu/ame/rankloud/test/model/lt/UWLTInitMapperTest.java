package edu.ame.rankloud.test.model.lt;

import static org.mockito.Mockito.*;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import edu.ame.rankloud.model.lt.UniformWeightLTModel.InitModelMapper;
import edu.ame.rankloud.util.NeighborWritable;
import edu.ame.rankloud.util.TextArrayWritable;


/**
 * @author ksarkar1
 *
 */

public class UWLTInitMapperTest {

	private Text key;
	private TextArrayWritable value;
	private NeighborWritable me;
	private Text[] neighbors;
	private InitModelMapper mapper;

	/**
	 * Set up the states for calling the map function
	 */
	@Before
	public void setUp() throws Exception {
		key = new Text("1001");
		neighbors = new Text[4];
		for (int i = 0; i < 4; i++) {
			neighbors[i] = new Text("300" + i);
		}
		
		value = new TextArrayWritable(neighbors);
		
		me = new NeighborWritable(key, new DoubleWritable((double)1/4));
		
		mapper = new InitModelMapper();

	}

	/**
	 * TestUWLT method for InitModelMapper#map - valid input
	 */
	@Test
	public void testMapValid() {
		
		// mock the output object
		OutputCollector<Text, NeighborWritable> output = mock(OutputCollector.class);
		
		try {
			// call the API
			mapper.map(key, value, output, null);
			
			// in order (sequential) verification of the calls to output.collect()
			InOrder inOrder = inOrder(output);
			inOrder.verify(output).collect(neighbors[0], me);
			inOrder.verify(output).collect(neighbors[1], me);
			inOrder.verify(output).collect(neighbors[2], me);
			inOrder.verify(output).collect(neighbors[3], me);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}

