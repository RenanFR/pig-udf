package pig.udf;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class BestLeftFootStriker extends FilterFunc{

	@Override
	public Boolean exec(Tuple tuple) throws IOException {
		if (tuple == null || tuple.size() == 0) {
			return false;
		}
		try {
			@SuppressWarnings("unused")
			String player = (String)tuple.get(0);
			String foot = (String)tuple.get(1);
			Integer overall = (Integer)tuple.get(2);
			String position = (String)tuple.get(3);
			if ((foot.equals("Left")) && (overall > 85) && (position.equals("ST"))) {
				return true;
			} else {
				return false;
			}
		} catch (Exception exception) {
			return false;
		}
	}

}
