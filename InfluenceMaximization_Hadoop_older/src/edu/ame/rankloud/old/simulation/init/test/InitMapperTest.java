/**
 * 
 */
package edu.ame.rankloud.old.simulation.init.test;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import edu.ame.rankloud.old.simulation.init.InitMapOutWritable;
import edu.ame.rankloud.old.simulation.init.InitMapper;
import edu.ame.rankloud.old.simulation.init.NeighborWritable;
import edu.ame.rankloud.old.util.TextArrayWritable;

/**
 * @author ksarkar1
 *
 */
public class InitMapperTest {
	
	private Text key;
	private TextArrayWritable value;
	private InitMapOutWritable me;
	private Text[] neighbors;
	private InitMapOutWritable isActive;
	private InitMapper mapper;

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
		
		me = new InitMapOutWritable(new NeighborWritable(key, new DoubleWritable((double)1/4)));
		
		isActive = new InitMapOutWritable(new BooleanWritable(false));
		
		mapper = new InitMapper();
		
		// stub seed.contains(key.toString())
		HashSet<String> seed = mock(HashSet.class);
		when(seed.contains(key.toString())).thenReturn(false);
		mapper.setSeed(seed);
	}

	/**
	 * Test method for InitMapper#map - valid input
	 */
	@Test
	public void testMapValid() {
		
		// mock the output object
		OutputCollector<Text, InitMapOutWritable> output = mock(OutputCollector.class);
		
		try {
			// call the API
			mapper.map(key, value, output, null);
			
			// in order (sequential) verification of the calls to output.collect()
			InOrder inOrder = inOrder(output);
			inOrder.verify(output).collect(neighbors[0], me);
			inOrder.verify(output).collect(neighbors[1], me);
			inOrder.verify(output).collect(neighbors[2], me);
			inOrder.verify(output).collect(neighbors[3], me);
			inOrder.verify(output).collect(key, isActive);
			//verify(output).collect(neighbors[0], me);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
