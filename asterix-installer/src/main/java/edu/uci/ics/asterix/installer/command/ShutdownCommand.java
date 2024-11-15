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
package edu.uci.ics.asterix.installer.command;

import edu.uci.ics.asterix.event.service.AsterixEventService;
import edu.uci.ics.asterix.event.service.ILookupService;
import edu.uci.ics.asterix.event.service.ServiceProvider;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;

public class ShutdownCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig(false);
        ILookupService lookupService = ServiceProvider.INSTANCE.getLookupService();
        lookupService.stopService(AsterixEventService.getConfiguration());
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new ShutdownConfig();
    }

    @Override
    protected String getUsageDescription() {
        return "\nShuts down the installer's backgrouund processes";
    }

}

class ShutdownConfig extends CommandConfig {

}
