/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.external.library;

import edu.uci.ics.asterix.external.library.java.JObjects.JLong;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;

public class EchoDelayFunction implements IExternalScalarFunction {

	public static final String DELAY_RPOPERTY = "feeds.function.delay";
	private static final long DEAULT_DELAY = 10;

	private long sleepInterval;
	private JLong timestamp;

	@Override
	public void initialize(IFunctionHelper functionHelper) {
		String v = System.getProperty(DELAY_RPOPERTY);
		if (v != null) {
			sleepInterval = Integer.parseInt(v);
		} else {
			sleepInterval = DEAULT_DELAY;
		}
		timestamp = new JLong(0);
	}

	@Override
	public void deinitialize() {
	}

	@Override
	public void evaluate(IFunctionHelper functionHelper) throws Exception {
		JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
		Thread.sleep(sleepInterval);
		functionHelper.setResult(inputRecord);
	}
}
