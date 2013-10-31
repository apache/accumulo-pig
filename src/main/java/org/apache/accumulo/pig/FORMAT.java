package org.apache.accumulo.pig;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class FORMAT extends EvalFunc<String> {

  @Override
  public String exec(Tuple input) throws IOException {
    if (0 == input.size()) {
      return null;
    }
    
    final String format = input.get(0).toString();
    Object[] args = new Object[input.size() - 1];
    for (int i = 1; i < input.size(); i++) {
      args[i-1] = input.get(i);
    }
    
    return String.format(format, args);
  }
  
}
