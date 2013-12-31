/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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
