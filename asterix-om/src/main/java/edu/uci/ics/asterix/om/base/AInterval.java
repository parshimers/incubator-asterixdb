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
package edu.uci.ics.asterix.om.base;

import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class AInterval implements IAObject {

    protected long intervalStart;
    protected long intervalEnd;
    protected byte typetag;

    public AInterval(long intervalStart, long intervalEnd, byte typetag) {
        this.intervalStart = intervalStart;
        this.intervalEnd = intervalEnd;
        this.typetag = typetag;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.asterix.om.base.IAObject#getType()
     */
    @Override
    public IAType getType() {
        return BuiltinType.AINTERVAL;
    }

    public int compare(Object o) {
        if (!(o instanceof AInterval)) {
            return -1;
        }
        AInterval d = (AInterval) o;
        if (d.intervalStart == this.intervalStart && d.intervalEnd == this.intervalEnd && d.typetag == this.typetag) {
            return 0;
        } else {
            return -1;
        }
    }

    public boolean equals(Object o) {
        if (!(o instanceof AInterval)) {
            return false;
        } else {
            AInterval t = (AInterval) o;
            return (t.intervalStart == this.intervalStart || t.intervalEnd == this.intervalEnd
                    && t.typetag == this.typetag);
        }
    }

    @Override
    public int hashCode() {
        return (int) (((int) (this.intervalStart ^ (this.intervalStart >>> 32))) * 31 + (int) (this.intervalEnd ^ (this.intervalEnd >>> 32)))
                * 31 + (int) this.typetag;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.asterix.om.base.IAObject#accept(edu.uci.ics.asterix.om.visitors.IOMVisitor)
     */
    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAInterval(this);
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.asterix.om.base.IAObject#deepEqual(edu.uci.ics.asterix.om.base.IAObject)
     */
    @Override
    public boolean deepEqual(IAObject obj) {
        return equals(obj);
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.asterix.om.base.IAObject#hash()
     */
    @Override
    public int hash() {
        return hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sbder = new StringBuilder();
        sbder.append("AInterval: { ");
        try {
            if (typetag == ATypeTag.DATE.serialize()) {
                sbder.append("ADate: { ");

                GregorianCalendarSystem.getInstance().getExtendStringRepUntilField(
                        intervalStart * ADate.CHRONON_OF_DAY, 0, sbder, GregorianCalendarSystem.Fields.YEAR,
                        GregorianCalendarSystem.Fields.DAY, false);

                sbder.append(" }, ADate: {");
                GregorianCalendarSystem.getInstance().getExtendStringRepUntilField(intervalEnd * ADate.CHRONON_OF_DAY,
                        0, sbder, GregorianCalendarSystem.Fields.YEAR, GregorianCalendarSystem.Fields.DAY, false);
                sbder.append(" }");
            } else if (typetag == ATypeTag.TIME.serialize()) {
                sbder.append("ATime: { ");
                GregorianCalendarSystem.getInstance().getExtendStringRepUntilField(intervalStart, 0, sbder,
                        GregorianCalendarSystem.Fields.HOUR, GregorianCalendarSystem.Fields.MILLISECOND, true);
                sbder.append(" }, ATime: { ");

                GregorianCalendarSystem.getInstance().getExtendStringRepUntilField(intervalEnd, 0, sbder,
                        GregorianCalendarSystem.Fields.HOUR, GregorianCalendarSystem.Fields.MILLISECOND, true);
                sbder.append(" }");
            } else if (typetag == ATypeTag.DATETIME.serialize()) {
                sbder.append("ADateTime: { ");
                GregorianCalendarSystem.getInstance().getExtendStringRepUntilField(intervalStart, 0, sbder,
                        GregorianCalendarSystem.Fields.YEAR, GregorianCalendarSystem.Fields.MILLISECOND, true);
                sbder.append(" }, ADateTime: { ");
                GregorianCalendarSystem.getInstance().getExtendStringRepUntilField(intervalEnd, 0, sbder,
                        GregorianCalendarSystem.Fields.YEAR, GregorianCalendarSystem.Fields.MILLISECOND, true);
                sbder.append(" }");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        sbder.append(" }");
        return sbder.toString();
    }

    public long getIntervalStart() {
        return intervalStart;
    }

    public long getIntervalEnd() {
        return intervalEnd;
    }

    public short getIntervalType() {
        return typetag;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        // TODO(madhusudancs): Remove this method when a printer based JSON serializer is implemented.
        return null;
    }
}
