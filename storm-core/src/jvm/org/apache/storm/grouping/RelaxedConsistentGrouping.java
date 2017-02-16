/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.grouping;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.Fields;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class RelaxedConsistentGrouping implements LoadAwareCustomStreamGrouping, Serializable {
    private static final long serialVersionUID = -447379837314000353L;
    private List<Integer> targetTasks;
    private Fields fields = null;
    private Fields outFields = null;
    private long lastUpdate = 0;
    RelaxedConsistentHashing hash;
  
    public RelaxedConsistentGrouping() {
    	//Empty
    }

    public RelaxedConsistentGrouping(Fields fields) {
        this.fields = fields;
    }

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks = targetTasks;
        hash = new RelaxedConsistentHashing(targetTasks.size(), 100);
        if (this.fields != null) {
            this.outFields = context.getComponentOutputFields(stream);
        }
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values, LoadMapping load) {
        List<Integer> boltIds = new ArrayList<>(1);
        if (values.size() > 0) {
            byte[] raw;
            if (fields != null) {
                List<Object> selectedFields = outFields.select(fields, values);
                ByteBuffer out = ByteBuffer.allocate(selectedFields.size() * 4);
                for (Object o: selectedFields) {
                    if (o instanceof List) {
                        out.putInt(Arrays.deepHashCode(((List)o).toArray()));
                    } else if (o instanceof Object[]) {
                        out.putInt(Arrays.deepHashCode((Object[])o));
                    } else if (o instanceof byte[]) {
                        out.putInt(Arrays.hashCode((byte[]) o));
                    } else if (o instanceof short[]) {
                        out.putInt(Arrays.hashCode((short[]) o));
                    } else if (o instanceof int[]) {
                        out.putInt(Arrays.hashCode((int[]) o));
                    } else if (o instanceof long[]) {
                        out.putInt(Arrays.hashCode((long[]) o));
                    } else if (o instanceof char[]) {
                        out.putInt(Arrays.hashCode((char[]) o));
                    } else if (o instanceof float[]) {
                        out.putInt(Arrays.hashCode((float[]) o));
                    } else if (o instanceof double[]) {
                        out.putInt(Arrays.hashCode((double[]) o));
                    } else if (o instanceof boolean[]) {
                        out.putInt(Arrays.hashCode((boolean[]) o));
                    } else if (o != null) {
                        out.putInt(o.hashCode());
                    } else {
                      out.putInt(0);
                    }
                }
                raw = out.array();
            } else {
                raw = values.get(0).toString().getBytes(); // assume key is the first field
            }
            
            if ((lastUpdate + 1000) < System.currentTimeMillis()) {
            	//add increase load and decrease load logic
            	
                lastUpdate = System.currentTimeMillis();
            }
            int selected = hash.getServer(raw);
            boltIds.add(targetTasks.get(selected));
        }
        return boltIds;
    }

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		throw new RuntimeException("NOT IMPLEMENTED");
	}
}
