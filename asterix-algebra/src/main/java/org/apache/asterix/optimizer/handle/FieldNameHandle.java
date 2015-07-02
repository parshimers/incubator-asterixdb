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
package org.apache.asterix.optimizer.handle;

import org.apache.asterix.om.types.IAType;

public class FieldNameHandle implements IHandle {

    private String fieldName;
    private IAType fieldType;

    public FieldNameHandle(String fieldName) {
        this.fieldName = fieldName;
    }

    public IAType getFieldType() {
        return fieldType;
    }

    public void setFieldType(IAType fieldType) {
        this.fieldType = fieldType;
    }

    @Override
    public HandleType getHandleType() {
        return HandleType.FIELD_NAME;
    }

    public String getFieldName() {
        return fieldName;
    }
}
