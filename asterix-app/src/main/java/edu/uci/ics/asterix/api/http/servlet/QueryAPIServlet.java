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
package edu.uci.ics.asterix.api.http.servlet;

import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.base.Statement.Kind;

public class QueryAPIServlet extends RESTAPIServlet {
    private static final long serialVersionUID = 1L;

    protected String getQueryParameter(HttpServletRequest request) {
        return request.getParameter("query");
    }

    protected List<Statement.Kind> getAllowedStatements() {
        Kind[] statementsArray = { Kind.DATAVERSE_DECL, Kind.FUNCTION_DECL, Kind.QUERY, Kind.SET, Kind.WRITE, Kind.RUN };
        return Arrays.asList(statementsArray);
    }

    protected String getErrorMessage() {
        return "Invalid statement: Non-query statement %s to the query API.";
    }
}
