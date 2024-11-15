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
package edu.uci.ics.asterix.common.annotations;

import java.io.File;

public class ListValFileDataGen implements IRecordFieldDataGen {

    private final File file;
    private final int min;
    private final int max;

    public ListValFileDataGen(File file, int min, int max) {
        this.file = file;
        this.min = min;
        this.max = max;
    }

    @Override
    public Kind getKind() {
        return Kind.LISTVALFILE;
    }

    public File getFile() {
        return file;
    }

    public int getMin() {
        return min;
    }

    public int getMax() {
        return max;
    }

}
