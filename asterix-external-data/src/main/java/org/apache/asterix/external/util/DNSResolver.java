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
package edu.uci.ics.asterix.external.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.Set;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.util.AsterixRuntimeUtil;

/**
 * Resolves a value (DNS/IP Address) to the id of a Node Controller running at the location.
 */
public class DNSResolver implements INodeResolver {

    private static Random random = new Random();

    @Override
    public String resolveNode(String value) throws AsterixException {
        try {
            InetAddress ipAddress = InetAddress.getByName(value);
            Set<String> nodeControllers = AsterixRuntimeUtil.getNodeControllersOnIP(ipAddress);
            if (nodeControllers == null || nodeControllers.isEmpty()) {
                throw new AsterixException(" No node controllers found at the address: " + value);
            }
            String chosenNCId = nodeControllers.toArray(new String[] {})[random.nextInt(nodeControllers.size())];
            return chosenNCId;
        }catch (UnknownHostException e){
            throw new AsterixException("Unable to resolve hostname '"+ value + "' to an IP address");
        } catch (AsterixException ae) {
            throw ae;
        } catch (Exception e) {
            throw new AsterixException(e);
        }
    }
}
