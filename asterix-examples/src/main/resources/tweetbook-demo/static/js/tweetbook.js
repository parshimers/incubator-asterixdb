$(function() {

    // Initialize connection to AsterixDB. Just one connection is needed and contains
    // logic for connecting to each API endpoint. This object A is reused throughout the
    // code but does not store information about any individual API call.
    A = new AsterixDBConnection({

        // We will be using the twitter dataverse, which we can configure either like this
        // or by following our AsterixDBConnection with a dataverse call, like so:
        // A = new AsterixDBConnection().dataverse("twitter");
        "dataverse" : "twitter",

        // Due to the setup of this demo using the Bottle server, it is necessary to change the
        // default endpoint of API calls. The proxy server handles the call to http://localhost:19002
        // for us, and we reconfigure this connection to connect to the proxy server.
        "endpoint_root" : "/",

        // Finally, we want to make our error function nicer so that we show errors with a call to the
        // reportUserMessage function. Update the "error" property to do that.
        "error" : function(data) {
                    // For an error, data will look like this:
                    // {
                    //     "error-code" : [error-number, error-text]
                    //     "stacktrace" : ...stack trace...
                    //     "summary"    : ...summary of error...
                    // }
                    // We will report this as an Asterix REST API Error, an error code, and a reason message.
                    // Note the method signature: reportUserMessage(message, isPositiveMessage, target). We will provide
                    // an error message to display, a positivity value (false in this case, errors are bad), and a
                    // target html element in which to report the message.
                    var showErrorMessage = "Asterix Error #" + data["error-code"][0] + ": " + data["error-code"][1];
                    var isPositive = false;
                    var showReportAt = "report-message";

                    reportUserMessage(showErrorMessage, isPositive, showReportAt);
                  }
    });

    // This little bit of code manages period checks of the asynchronous query manager,
    // which holds onto handles asynchornously received. We can set the handle update
    // frequency using seconds, and it will let us know when it is ready.
    var intervalID = setInterval(
        function() {
            asynchronousQueryIntervalUpdate();
        },
        asynchronousQueryGetInterval()
    );

    // Query tracker
    last_query_submitted = {
        "time": null,
        "query": null
    };

    rainbow = new Rainbow();

    // Initialization of UI Tabs
    initDemoPrepareTabs();

    map = new GMaps({
        div: '#map',
        lat: 38.89,
        lng: -77.03,
        zoom: 4,
        draggable: false,
    });
    drawings = [];
    rainbow = new Rainbow();

    var input = document.getElementById('location-text-box');
    var autocomplete = new google.maps.places.Autocomplete(input);
    autocomplete.bindTo('bounds', map.map);

    google.maps.event.addListener(autocomplete, 'place_changed', function() {
        var place = autocomplete.getPlace();
        if (place.geometry.viewport) {
            map.fitBounds(place.geometry.viewport);
        } else {
            map.setCenter(place.geometry.location);
            map.setZoom(17);  // Why 17? Because it looks good.
        }
        var address = '';
        if (place.address_components) {
            address = [(place.address_components[0] && place.address_components[0].short_name || ''),
              (place.address_components[1] && place.address_components[1].short_name || ''),
              (place.address_components[2] && place.address_components[2].short_name || '') ].join(' ');
        }
    });

    // Drawing Manager for selecting map regions. See documentation here:
    // https://developers.google.com/maps/documentation/javascript/reference#DrawingManager
    rectangleManager = new google.maps.drawing.DrawingManager({
        drawingMode : google.maps.drawing.OverlayType.RECTANGLE,
        drawingControl : false,
        rectangleOptions : {
            strokeWeight: 1,
            clickable: false,
            editable: true,
            strokeColor: "#2b3f8c",
            fillColor: "#2b3f8c",
            zIndex: 1
        }
    });
    rectangleManager.setMap(map.map);
    selectionRectangle = null;

    // Drawing Manager: Just one editable rectangle!
    google.maps.event.addListener(rectangleManager, 'rectanglecomplete', function(rectangle) {
        selectionRectangle = rectangle;
        rectangleManager.setDrawingMode(null);
    });

    // Initialize data structures
    APIqueryTracker = {};
    asyncQueryManager = {};
    review_mode_tweetbooks = [];

    getAllDataverseTweetbooks();
    initDemoUIButtonControls();
});

function initDemoUIButtonControls() {

    // Explore Mode - Update Sliders
    var updateSliderDisplay = function(event, ui) {
        if (event.target.id == "grid-lat-slider") {
            $("#gridlat").text(""+ui.value);
        } else {
          $("#gridlng").text(""+ui.value);
        }
    };
    sliderOptions = {
        max: 10,
        min: 2.0,
        step: .1,
        value: 3.0,
        slidechange: updateSliderDisplay,
        slide: updateSliderDisplay,
        start: updateSliderDisplay,
        stop: updateSliderDisplay
    };
    $("#gridlat").text(""+sliderOptions.value);
    $("#gridlng").text(""+sliderOptions.value);
    $(".grid-slider").slider(sliderOptions);

    // Explore Mode - Query Builder Date Pickers
    var dateOptions = {
        dateFormat: "yy-mm-dd",
        defaultDate: "2012-01-02",
        navigationAsDateFormat: true,
        constrainInput: true
    };
    var start_dp = $("#start-date").datepicker(dateOptions);
    start_dp.val(dateOptions.defaultDate);
    dateOptions['defaultDate'] = "2012-12-31";
    var end_dp= $("#end-date").datepicker(dateOptions);
    end_dp.val(dateOptions.defaultDate);

    // Explore Mode: Toggle Selection/Location Search
    $('#selection-button').on('click', function (e) {
        $("#location-text-box").attr("disabled", "disabled");
        rectangleManager.setMap(map.map);
        if (selectionRectangle) {
            selectionRectangle.setMap(null);
            selectionRectangle = null;
        } else {
            rectangleManager.setDrawingMode(google.maps.drawing.OverlayType.RECTANGLE);
        }
    });
    $('#location-button').on('change', function (e) {
        $("#location-text-box").removeAttr("disabled");
        if (selectionRectangle) {
            selectionRectangle.setMap(null);
        }        
        rectangleManager.setMap(null);
        rectangleManager.setDrawingMode(google.maps.drawing.OverlayType.RECTANGLE);
    });

    // Review Mode: New Tweetbook
    $('#new-tweetbook-button').on('click', function (e) {
        onCreateNewTweetBook($('#new-tweetbook-entry').val());

        $('#new-tweetbook-entry').val("");
        $('#new-tweetbook-entry').attr("placeholder", "Name a new tweetbook");
    });

    // Explore Mode - Show Query Button
    $("#show-query-button").click(function() {
        var show_query_content, query_text, timestamp;

        query_text = last_query_submitted.query;
        timestamp = last_query_submitted.time;

        $("#show-query-blob").remove();

        if (last_query_submitted.query) {
            show_query_content = "Last query submitted at " + timestamp + ":<br/>" + query_text;
        } else {
            show_query_content = "No recent queries to show."
        }

        $('<div/>')
            .attr("class", "alert alert-success")
            .attr("id", "show-query-blob")   
            .html('<button type="button" class="close" data-dismiss="alert">&times;</button>' + show_query_content)
            .appendTo('#explore-well');
    });

    // Explore Mode - Clear Button
    $("#clear-button").click(resetMap);

    // Explore Mode: Query Submission
    $("#submit-button").on("click", function () {

        $("#report-message").html('');
        $("#submit-button").attr("disabled", true);
        rectangleManager.setDrawingMode(null);

        var kwterm = $("#keyword-textbox").val();
        var startdp = $("#start-date").datepicker("getDate");
        var enddp = $("#end-date").datepicker("getDate");
        var startdt = $.datepicker.formatDate("yy-mm-dd", startdp)+"T00:00:00Z";
        var enddt = $.datepicker.formatDate("yy-mm-dd", enddp)+"T23:59:59Z";

        var formData = {
            "keyword": kwterm,
            "startdt": startdt,
            "enddt": enddt,
            "gridlat": $("#grid-lat-slider").slider("value"),
            "gridlng": $("#grid-lng-slider").slider("value")
        };

        // Get Map Bounds
        var bounds;
        if ($('#selection-label').hasClass("active") && selectionRectangle) {
            bounds = selectionRectangle.getBounds();
        } else {
            bounds = map.getBounds();
        }

        var swLat = Math.abs(bounds.getSouthWest().lat());
        var swLng = Math.abs(bounds.getSouthWest().lng());
        var neLat = Math.abs(bounds.getNorthEast().lat());
        var neLng = Math.abs(bounds.getNorthEast().lng());

        formData["swLat"] = Math.min(swLat, neLat);
        formData["swLng"] = Math.max(swLng, neLng);
        formData["neLat"] = Math.max(swLat, neLat);
        formData["neLng"] = Math.min(swLng, neLng);

        var build_tweetbook_mode = "synchronous";
        if ($('#asbox').is(":checked")) {
            build_tweetbook_mode = "asynchronous";
        }

        var f = buildAQLQueryFromForm(formData);

        APIqueryTracker = {
            "query" : "use dataverse twitter;\n" + f.val(),
            "data" : formData
        };

        last_query_submitted = {
            "query": APIqueryTracker.query,
            "time": new Date()
        };

        if (build_tweetbook_mode == "synchronous") {
            A.query(f.val(), tweetbookQuerySyncCallback, build_tweetbook_mode);
        } else {
            A.query(f.val(), tweetbookQueryAsyncCallback, build_tweetbook_mode);
        }

        // Clears selection rectangle on query execution, rather than waiting for another clear call.
        if (selectionRectangle) {
            selectionRectangle.setMap(null);
            selectionRectangle = null;
        }
    });
}

/**
* Builds AsterixDB REST Query from explore mode form.
*/
function buildAQLQueryFromForm(parameters) {

    var bounds = {
        "ne" : { "lat" : parameters["neLat"], "lng" : -1*parameters["neLng"]},
        "sw" : { "lat" : parameters["swLat"], "lng" : -1*parameters["swLng"]}
    };

    var rectangle =
        new FunctionExpression("create-rectangle",
            new FunctionExpression("create-point", bounds["sw"]["lat"], bounds["sw"]["lng"]),
            new FunctionExpression("create-point", bounds["ne"]["lat"], bounds["ne"]["lng"]));

    // You can chain these all together, but let's take them one at a time.
    // Let's start with a ForClause. Here we go through each tweet $t in the
    // dataset TweetMessageShifted.
    var aql = new FLWOGRExpression()
        .ForClause("$t", new AExpression("dataset TweetMessagesShifted"));

    // We know we have bounds for our region, so we can add that LetClause next.
    aql = aql.LetClause("$region", rectangle);

    // Now, let's change it up. The keyword term doesn't always show up, so it might be blank.
    // We'll attach a new let clause for it, and then a WhereClause.
    if (parameters["keyword"].length > 0) {
        aql = aql
                .LetClause("$keyword", new AExpression('"' + parameters["keyword"] + '"'))
                .WhereClause().and(
                    new FunctionExpression("spatial-intersect", "$t.sender-location", "$region"),
                    new AExpression('$t.send-time > datetime("' + parameters["startdt"] + '")'),
                    new AExpression('$t.send-time < datetime("' + parameters["enddt"] + '")'),
                    new FunctionExpression("contains", "$t.message-text", "$keyword")
                );
    } else {
        aql = aql
                .WhereClause().and(
                    new FunctionExpression("spatial-intersect", "$t.sender-location", "$region"),
                    new AExpression('$t.send-time > datetime("' + parameters["startdt"] + '")'),
                    new AExpression('$t.send-time < datetime("' + parameters["enddt"] + '")')
                );
    }

    // Finally, we'll group our results into spatial cells.
    aql = aql.GroupClause(
                "$c",
                new FunctionExpression("spatial-cell", "$t.sender-location",
                new FunctionExpression("create-point", "24.5", "-125.5"),
                parameters["gridlat"].toFixed(1), parameters["gridlng"].toFixed(1)),
                "with",
                "$t"
            );

    // ...and return a resulting cell and a count of results in that cell.
    aql = aql.ReturnClause({ "cell" : "$c", "count" : "count($t)" });

    return aql;
}

/**
* getAllDataverseTweetbooks
*
* Returns all datasets of type TweetbookEntry, populates review_mode_tweetbooks
*/
function getAllDataverseTweetbooks(fn_tweetbooks) {

    // This creates a query to the Metadata for datasets of type
    // TweetBookEntry. Note that if we throw in a WhereClause (commented out below)
    // there is an odd error. This is being fixed and will be removed from this demo.
    var getTweetbooksQuery = new FLWOGRExpression()
        .ForClause("$ds", new AExpression("dataset Metadata.Dataset"))
        //.WhereClause(new AExpression('$ds.DataTypeName = "TweetbookEntry"'))
        .ReturnClause({
            "DataTypeName" : "$ds.DataTypeName",
            "DatasetName" : "$ds.DatasetName"
        });

    // Now create a function that will be called when tweetbooks succeed.
    // In this case, we want to parse out the results object from the Asterix
    // REST API response.
    var tweetbooksSuccess = function(r) {
        // Parse tweetbook metadata results        
        $.each(r.results, function(i, dataset) {
            if (dataset.DataTypeName == "TweetbookEntry") {
                review_mode_tweetbooks.push(dataset.DatasetName);
            }
        });

        // Now, if any tweetbooks already exist, opulate review screen.
        $('#review-tweetbook-titles').html('');
        $.each(review_mode_tweetbooks, function(i, tweetbook) {
            addTweetBookDropdownItem(tweetbook);
        });
    };

    // Now, we are ready to run a query.
    A.meta(getTweetbooksQuery.val(), tweetbooksSuccess);
}

/**
* Checks through each asynchronous query to see if they are ready yet
*/
function asynchronousQueryIntervalUpdate() {
    for (var handle_key in asyncQueryManager) {
        if (!asyncQueryManager[handle_key].hasOwnProperty("ready")) {
            asynchronousQueryGetAPIQueryStatus( asyncQueryManager[handle_key]["handle"], handle_key );
        }
    }
}

/**
* Returns current time interval to check for asynchronous query readiness
* @returns  {number}    milliseconds between asychronous query checks
*/
function asynchronousQueryGetInterval() {
    var seconds = 10;
    return seconds * 1000;
}

/**
* Retrieves status of an asynchronous query, using an opaque result handle from API
* @param    {Object}    handle, an object previously returned from an async call
* @param    {number}    handle_id, the integer ID parsed from the handle object
*/
function asynchronousQueryGetAPIQueryStatus (handle, handle_id) {


    // TODO
    // last_query_submitted = {
    //      "query": APIqueryTracker.query,
    //      "time": new Date()
    // };    
    A.query_status(
        {
            "handle" : JSON.stringify(handle)
        },
        function (res) {
            if (res["status"] == "SUCCESS") {
                // We don't need to check if this one is ready again, it's not going anywhere...
                // Unless the life cycle of handles has changed drastically
                asyncQueryManager[handle_id]["ready"] = true;

                // Indicate success.
                $('#handle_' + handle_id).removeClass("btn-disabled").prop('disabled', false).addClass("btn-success");
            }
        }
     );
}

/**
* On-success callback after async API query
* @param    {object}    res, a result object containing an opaque result handle to Asterix
*/
function tweetbookQueryAsyncCallback(res) {

    // Parse handle, handle id and query from async call result
    var handle_query = APIqueryTracker["query"];
    var handle = res;
    var handle_id = res["handle"].toString().split(',')[0];

    // Add to stored map of existing handles
    asyncQueryManager[handle_id] = {
        "handle" : handle,
        "query" : handle_query, // This will show up when query control button is clicked.
        "data" : APIqueryTracker["data"]
    };

    // Create a container for this async query handle
    $('<div/>')
        .css("margin-left", "1em")
        .css("margin-bottom", "1em")
        .css("display", "block")
        .attr({
            "class" : "btn-group",
            "id" : "async_container_" + handle_id
        })
        .appendTo("#async-handle-controls");

    // Adds the main button for this async handle
    var handle_action_button = '<button class="btn btn-disabled" id="handle_' + handle_id + '">Handle ' + handle_id + '</button>';
    $('#async_container_' + handle_id).append(handle_action_button);
    $('#handle_' + handle_id).prop('disabled', true);
    $('#handle_' + handle_id).on('click', function (e) {

        // make sure query is ready to be run
        if (asyncQueryManager[handle_id]["ready"]) {

            APIqueryTracker = {
                "query" : asyncQueryManager[handle_id]["query"],
                "data"  : asyncQueryManager[handle_id]["data"]
            };

            if (!asyncQueryManager[handle_id].hasOwnProperty("result")) {
                
                // TODO
                // last_query_submitted = {
                //      "query": APIqueryTracker.query,
                //      "time": new Date()
                // };
                A.query_result(
                    { "handle" : JSON.stringify(asyncQueryManager[handle_id]["handle"]) },
                    function(res) {
                        asyncQueryManager[handle_id]["result"] = res;

                        var resultTransform = {
                            "results" : res.results
                        };

                        tweetbookQuerySyncCallback(resultTransform);
                    }
                );
            } else {

                var resultTransform = {
                    "results" : asyncQueryManager[handle_id]["result"].results
                };

                tweetbookQuerySyncCallback(resultTransform);
            }
        }
    });

    // Adds a removal button for this async handle
    var asyncDeleteButton = addDeleteButton(
        "trashhandle_" + handle_id,
        "async_container_" + handle_id,
        function (e) {
            $('#async_container_' + handle_id).remove();
            delete asyncQueryManager[handle_id];
        }
    );

    $('#async_container_' + handle_id).append('<br/>');

    $("#submit-button").attr("disabled", false);
}

/**
* A spatial data cleaning and mapping call
* @param    {Object}    res, a result object from a tweetbook geospatial query
*/
function tweetbookQuerySyncCallback(res) {
    // First, we check if any results came back in.
    // If they didn't, return. TODO check for empty list here too...
    if (!res.hasOwnProperty("results")) {
        reportUserMessage("Oops, no results found for those parameters.", false, "report-message");
        return;
    }

    // Initialize coordinates and weights, to store
    // coordinates of map cells and their weights
    var rectangles = [];
    var maxCount = 0;
    var minCount = Number.MAX_VALUE;

    // Parse resulting JSON objects. Here is an example record:
    // {
    //   "cell": {
    //     "rectangle": [
    //       {"point": [21.5, -98.5]},
    //       {"point": [24.5, -95.5]}
    //     ]
    //   },
    //   "count": {
    //     "int64": 11
    //   }
    // }
    $.each(res.results, function(i, data) {
        var rectangle = {
            "bottom_left" : data.cell.rectangle[0].point,
            "top_right" : data.cell.rectangle[1].point,
            "count": data.count.int64,

            // Lat/Lng, for ease of use later...
            "latSW" : data.cell.rectangle[0].point[0],
            "lngSW" : data.cell.rectangle[0].point[1],
            "latNE" : data.cell.rectangle[1].point[0],
            "lngNE" : data.cell.rectangle[1].point[1]
        };
        
        rectangles.push(rectangle);

        // We track the minimum and maximum weight for the legend.
        maxCount = Math.max(rectangle["count"], maxCount);
        minCount = Math.min(rectangle["count"], minCount);
    });
    
    triggerUIUpdate(rectangles, maxCount, minCount);
}

/**
* Triggers a map update based on a set of spatial query result cells
* @param    [Array]     mapPlotData, an array of coordinate and weight objects
* @param    [Array]     plotWeights, a list of weights of the spatial cells - e.g., number of tweets
*/
function triggerUIUpdate(mapPlotData, maxWeight, minWeight) {
    resetMap();
    rectangleManager.setMap(null);

    // mapPlotData is a list of rectangle objects, such as this:
    // {
    //   "bottom_left": [21.5, -98.5],
    //   "top_right": [24.5, -95.5],
    //   "count": 11
    // }
    //
    // In longitude/latitude terms, this translates as follows:
    // {
    //   "bottom_left": [latitude SW, longitude SW],
    //   "top_right": [latitude NE, longitude NE],
    //   "count": 11
    // }
    $.each(mapPlotData, function (m, rectangle) {
        var drawing = map.drawCircle({
            lat: (rectangle.latSW + rectangle.latNE)/2.0,
            lng: (rectangle.lngSW + rectangle.lngNE)/2.0,
            click: function(e) {
                onMapPointDrillDown(this.get('val'));
            },
            mouseover: function(e) {
                this.getMap().getDiv().setAttribute('title',this.get('val').count + ' tweets');
            },
            mouseout: function(e) {
                this.getMap().getDiv().removeAttribute('title');
            },

            // Extra options for demo
            fillOpacity: 0.85,
            fillColor: "#" + rainbow.colourAt(Math.ceil(100 * (rectangle.count / maxWeight))),
            radius: computeCircleMarkerRadius(rectangle, maxWeight),
            val: rectangle
        });
        drawings.push(drawing);
    });

    // Show legend
    $("#legend-min").html(minWeight);
    $("#legend-max").html(maxWeight);
    $("#rainbow-legend-container").show();

    $("#submit-button").attr("disabled", false);
}

/**
* Computes radius for a given data point from a spatial cell
* @param    {Object}    keys => ["bottom_left", "top_right", "count", "latSW", "latNE", "lngSW", "lngNE"]
* @param    {Number}    tweetLimit
* @returns  {number}    radius between 2 points in metres
*/
function computeCircleMarkerRadius(rectangle, tweetLimit) {
    // Define Boundary Points
    var point_center = new google.maps.LatLng((rectangle.latSW + rectangle.latNE)/2.0, (rectangle.lngSW + rectangle.lngNE)/2.0);
    var point_left = new google.maps.LatLng((rectangle.latSW + rectangle.latNE)/2.0, rectangle.lngSW);
    var point_top = new google.maps.LatLng(rectangle.latNE, (rectangle.lngSW + rectangle.lngNE)/2.0);

    // Circle scale modifier =
    var scale = 425 + 425*(rectangle.count / tweetLimit);

    // Calculates the distance between two latlng locations in km, using Google Geometry API.
    var distanceBetweenPoints = function(p1, p2) {
        return 0.001 * google.maps.geometry.spherical.computeDistanceBetween(p1, p2);
    };

    // Return proportionate value so that circles mostly line up.
    return scale * Math.min(distanceBetweenPoints(point_center, point_left), distanceBetweenPoints(point_center, point_top));
}

/**
* prepares an Asterix API query to drill down in a rectangular spatial zone
*
* @params {object} marker_borders a set of bounds for a region from a previous api result
*/
function onMapPointDrillDown(tweet_borders) {
    resetMapDrawings();

    // Zoom in on boundaries of rectangle containing tweets
    var drilldown_bounds = [];
    drilldown_bounds.push(new google.maps.LatLng(tweet_borders.latSW, tweet_borders.lngSW));
    drilldown_bounds.push(new google.maps.LatLng(tweet_borders.latNE, tweet_borders.lngNE));
    map.fitLatLngBounds(drilldown_bounds)

    var zoneData = APIqueryTracker["data"];
    var df = getDrillDownQuery(zoneData, tweet_borders);
    
    APIqueryTracker = {
        "query_string" : "use dataverse twitter;\n" + df.val(),
        "marker_path" : "static/img/mobile2.png"
    };

    last_query_submitted = {
        "query": APIqueryTracker.query_string,
        "time": new Date()
    };
    A.query(df.val(), onTweetbookQuerySuccessPlot);
}

/**
* Generates an aql query for zooming on a spatial cell and obtaining tweets contained therein.
* @param parameters, the original query parameters
* @param bounds, the bounds of the zone to zoom in on.
*/
function getDrillDownQuery(parameters, bounds) {
    var zoomRectangle = new FunctionExpression("create-rectangle",
        new FunctionExpression("create-point", bounds.latSW, bounds.lngSW),
        new FunctionExpression("create-point", bounds.latNE, bounds.lngNE));

    var drillDown = new FLWOGRExpression()
        .ForClause("$t", new AExpression("dataset TweetMessagesShifted"))
        .LetClause("$region", zoomRectangle);

    if (parameters["keyword"].length == 0) {
        drillDown = drillDown
                        .WhereClause().and(
                            new FunctionExpression('spatial-intersect', '$t.sender-location', '$region'),
                            new AExpression().set('$t.send-time > datetime("' + parameters["startdt"] + '")'),
                            new AExpression().set('$t.send-time < datetime("' + parameters["enddt"] + '")')
                        );
    } else {
        drillDown = drillDown
                        .LetClause("$keyword", new AExpression('"' + parameters["keyword"] + '"'))
                        .WhereClause().and(
                            new FunctionExpression('spatial-intersect', '$t.sender-location', '$region'),
                            new AExpression().set('$t.send-time > datetime("' + parameters["startdt"] + '")'),
                            new AExpression().set('$t.send-time < datetime("' + parameters["enddt"] + '")'),
                            new FunctionExpression('contains', '$t.message-text', '$keyword')
                        );
    }

    drillDown = drillDown
                    .ReturnClause({
                        "tweetId" : "$t.tweetid",
                        "tweetText" : "$t.message-text",
                        "tweetLoc" : "$t.sender-location"
                    });

    return drillDown;
}

/**
* Given a location where a tweet exists, opens a modal to examine or update a tweet's content.
* @param t0, a tweetobject that has a location, text, id, and optionally a comment.
*/
function onDrillDownAtLocation(tO) {

    var tweetId = tO["tweetEntryId"];
    var tweetText = tO["tweetText"];

    // First, set tweet in drilldown modal to be this tweet's text
    $('#modal-body-tweet').html('Tweet #' + tweetId + ": " + tweetText);

    // Next, empty any leftover tweetbook comments or error/success messages
    $("#modal-body-add-to").val('');
    $("#modal-body-add-note").val('');
    $("#modal-body-message-holder").html("");

    // Next, if there is an existing tweetcomment reported, show it.
    if (tO.hasOwnProperty("tweetComment")) {

        // Show correct panel
        $("#modal-existing-note").show();
        $("#modal-save-tweet-panel").hide();

        // Fill in existing tweet comment
        $("#modal-body-tweet-note").val(tO["tweetComment"]);

        // Change Tweetbook Badge
        $("#modal-current-tweetbook").val(APIqueryTracker["active_tweetbook"]);

        // Add deletion functionality
        $("#modal-body-trash-icon").on('click', function () {
            // Send comment deletion to asterix
            var deleteTweetCommentOnId = '"' + tweetId + '"';
            var toDelete = new DeleteStatement(
                "$mt",
                APIqueryTracker["active_tweetbook"],
                new AExpression("$mt.tweetid = " + deleteTweetCommentOnId.toString())
            );
            
            // TODO
            // last_query_submitted = {
            //      "query": ???,
            //      "time": new Date()
            // };
            A.update(
                toDelete.val()
            );

            // Hide comment from map
            $('#drilldown_modal').modal('hide');

            // Replot tweetbook
            onPlotTweetbook(APIqueryTracker["active_tweetbook"]);
        });

    } else {
        // Show correct panel
        $("#modal-existing-note").hide();
        $("#modal-save-tweet-panel").show();

        // Now, when adding a comment on an available tweet to a tweetbook
        $('#save-comment-tweetbook-modal').unbind('click');
        $("#save-comment-tweetbook-modal").on('click', function(e) {

            // Stuff to save about new comment
            var save_metacomment_target_tweetbook = $("#modal-body-add-to").val();
            var save_metacomment_target_comment = '"' + $("#modal-body-add-note").val() + '"';
            var save_metacomment_target_tweet = '"' + tweetId + '"';

            // Make sure content is entered, and then save this comment.
            if ($("#modal-body-add-note").val() == "") {

                reportUserMessage("Please enter a comment about the tweet", false, "report-message");

            } else if ($("#modal-body-add-to").val() == "") {

                reportUserMessage("Please enter a tweetbook.", false, "report-message");

            } else {

                // Check if tweetbook exists. If not, create it.
                if (!(existsTweetbook(save_metacomment_target_tweetbook))) {
                    onCreateNewTweetBook(save_metacomment_target_tweetbook);
                }

                var toInsert = new InsertStatement(
                    save_metacomment_target_tweetbook,
                    {
                        "tweetid" : save_metacomment_target_tweet.toString(),
                        "comment-text" : save_metacomment_target_comment
                    }
                );

                // TODO
                // last_query_submitted = {
                //      "query": ???,
                //      "time": new Date()
                // };
                A.update(toInsert.val(), function () {
                    var successMessage = "Saved comment on <b>Tweet #" + tweetId +
                        "</b> in dataset <b>" + save_metacomment_target_tweetbook + "</b>.";
                    reportUserMessage(successMessage, true, "report-message");

                    $("#modal-body-add-to").val('');
                    $("#modal-body-add-note").val('');
                    $('#save-comment-tweetbook-modal').unbind('click');

                    // Close modal
                    $('#drilldown_modal').modal('hide');
                });
            }
        });
    }
}

/**
* Adds a new tweetbook entry to the menu and creates a dataset of type TweetbookEntry.
*/
function onCreateNewTweetBook(tweetbook_title) {

    var tweetbook_title = tweetbook_title.split(' ').join('_');

    // TODO
    // last_query_submitted = {
    //      "query": ???,
    //      "time": new Date()
    // };
    A.ddl(
        "create dataset " + tweetbook_title + "(TweetbookEntry) primary key tweetid;",
        function () {}
    );

    if (!(existsTweetbook(tweetbook_title))) {
        review_mode_tweetbooks.push(tweetbook_title);
        addTweetBookDropdownItem(tweetbook_title);
    }
}

/**
* Removes a tweetbook from both demo and from
* dataverse metadata.
*/
function onDropTweetBook(tweetbook_title) {

    // TODO
    // last_query_submitted = {
    //      "query": ???,
    //      "time": new Date()
    // };
    A.ddl(
        "drop dataset " + tweetbook_title + " if exists;",
        function () {}
    );

    // Removes tweetbook from review_mode_tweetbooks
    var remove_position = $.inArray(tweetbook_title, review_mode_tweetbooks);
    if (remove_position >= 0) review_mode_tweetbooks.splice(remove_position, 1);

    // Clear UI with review tweetbook titles
    $('#review-tweetbook-titles').html('');
    for (r in review_mode_tweetbooks) {
        addTweetBookDropdownItem(review_mode_tweetbooks[r]);
    }
}

/**
* Adds a tweetbook action button to the dropdown box in review mode.
* @param tweetbook, a string representing a tweetbook
*/
function addTweetBookDropdownItem(tweetbook) {
    // Add placeholder for this tweetbook
    $('<div/>')
        .attr({
            "class" : "btn-group",
            "id" : "rm_holder_" + tweetbook
        }).appendTo("#review-tweetbook-titles");

    // Add plotting button for this tweetbook
    var plot_button = '<button class="btn btn-default" id="rm_plotbook_' + tweetbook + '">' + tweetbook + '</button>';
    $("#rm_holder_" + tweetbook).append(plot_button);
    $("#rm_plotbook_" + tweetbook).width("200px");
    $("#rm_plotbook_" + tweetbook).on('click', function(e) {
        onPlotTweetbook(tweetbook);
    });

    // Add trash button for this tweetbook
    var onTrashTweetbookButton = addDeleteButton(
        "rm_trashbook_" + tweetbook,
        "rm_holder_" + tweetbook,
        function(e) {
            onDropTweetBook(tweetbook);
        }
    );
}

/**
* Generates AsterixDB query to plot existing tweetbook commnets
* @param tweetbook, a string representing a tweetbook
*/
function onPlotTweetbook(tweetbook) {

    // Clear map for this one
    map.removeMarkers();

    var plotTweetQuery = new FLWOGRExpression()
        .ForClause("$t", new AExpression("dataset TweetMessagesShifted"))
        .ForClause("$m", new AExpression("dataset " + tweetbook))
        .WhereClause(new AExpression("$m.tweetid = $t.tweetid"))
        .ReturnClause({
            "tweetId" : "$m.tweetid",
            "tweetText" : "$t.message-text",
            "tweetCom" : "$m.comment-text",
            "tweetLoc" : "$t.sender-location"
        });

    APIqueryTracker = {
        "query_string" : "use dataverse twitter;\n" + plotTweetQuery.val(),
        "marker_path" : "static/img/mobile_green2.png",
        "active_tweetbook" : tweetbook
    };

    last_query_submitted = {
        "query": APIqueryTracker.query_string,
        "time": new Date()
    };
    A.query(plotTweetQuery.val(), onTweetbookQuerySuccessPlot);
}

/**
* Given an output response set of tweet data,
* prepares markers on map to represent individual tweets.
* @param res, a JSON Object
*/
function onTweetbookQuerySuccessPlot (res) {

    // First, we check if any results came back in.
    // If they didn't, return.
    if (!res.hasOwnProperty("results")) {
        reportUserMessage("Oops, no data matches this query.", false, "report-message");
        return;
    }

    // Parse out tweet Ids, texts, and locations
    var tweets = [];
    $.each(res.results, function(i, tweet) {
        // Construct a tweet object
        var tweetData = {
            "tweetEntryId" : parseInt(tweet.tweetId),
            "tweetText" : tweet.tweetText,
            "lat" : tweet.tweetLoc.point[0],
            "lng" : tweet.tweetLoc.point[1],
        };    

        // If we are parsing out tweetbook data with comments, we need to check
        // for those here as well.
        if (tweet.hasOwnProperty("tweetCom")) {
            tweetData["tweetComment"] = tweet.tweetCom;
        }

        tweets.push(tweetData);
    });

    // Create a marker for each tweet
    $.each(tweets, function(i, tweet) {
        // Create a phone marker at tweet's position
        var tweet_marker = map.addMarker({
            lat: tweet.lat,
            lng: tweet.lng,
            icon: APIqueryTracker["marker_path"],
            val: tweet,
            click: function(e) {
                onClickTweetbookMapMarker(this.get('val'));
            },
        });
        drawings.push(tweet_marker);
    });
}

/**
* Checks if a tweetbook exists
* @param tweetbook, a String
*/
function existsTweetbook(tweetbook) {
    if (parseInt($.inArray(tweetbook, review_mode_tweetbooks)) == -1) {
        return false;
    } else {
        return true;
    }
}

/**
* When a marker is clicked on in the tweetbook, it will launch a modal
* view to examine or edit the appropriate tweet
*/
function onClickTweetbookMapMarker(t) {
    onDrillDownAtLocation(t)
    $('#drilldown_modal').modal();
}

/**
* Explore mode: Initial map creation and screen alignment
*/
function onOpenExploreMap () {
    var explore_column_height = $('#explore-well').height();
    var right_column_width = $('#right-col').width();
    $('#map').height(explore_column_height + "px");
    $('#map').width(right_column_width + "px");

    $('#review-well').height(explore_column_height + "px");
    $('#review-well').css('max-height', explore_column_height + "px");
    $('#right-col').height(explore_column_height + "px");
}

/**
* initializes demo - adds some extra events when review/explore
* mode are clicked, initializes tabs, aligns map box, moves
* active tab to about tab
*/
function initDemoPrepareTabs() {

    // Tab behavior for About, Explore, and Demo
    $('#mode-tabs a').click(function (e) {
        e.preventDefault()
        $(this).tab('show')
    })

    // Explore mode should show explore-mode query-builder UI
    $('#explore-mode').click(function(e) {
        $('#review-well').hide();
        $('#explore-well').show();
        rectangleManager.setMap(map.map);
        rectangleManager.setDrawingMode(google.maps.drawing.OverlayType.RECTANGLE);
        resetMap();
    });

    // Review mode should show review well and hide explore well
    $('#review-mode').click(function(e) {
        $('#explore-well').hide();
        $('#review-well').show();
        resetMap();
        rectangleManager.setMap(null);
        rectangleManager.setDrawingMode(null);
    });

    // Does some alignment necessary for the map canvas
    onOpenExploreMap();
}

/**
* Creates a delete icon button using default trash icon
* @param    {String}    id, id for this element
* @param    {String}    attachTo, id string of an element to which I can attach this button.
* @param    {Function}  onClick, a function to fire when this icon is clicked
*/
function addDeleteButton(iconId, attachTo, onClick) {

    var trashIcon = '<button class="btn btn-default" id="' + iconId + '"><span class="glyphicon glyphicon-trash"></span></button>';
    $('#' + attachTo).append(trashIcon);

    // When this trash button is clicked, the function is called.
    $('#' + iconId).on('click', onClick);
}

/**
* Creates a message and attaches it to data management area.
* @param    {String}    message, a message to post
* @param    {Boolean}   isPositiveMessage, whether or not this is a positive message.
* @param    {String}    target, the target div to attach this message.
*/
function reportUserMessage(message, isPositiveMessage, target) {
    // Clear out any existing messages
    $('#' + target).html('');

    // Select appropriate alert-type
    var alertType = "alert-success";
    if (!isPositiveMessage) {
        alertType = "alert-danger";
    }

    // Append the appropriate message
    $('<div/>')
        .attr("class", "alert " + alertType)
        .html('<button type="button" class="close" data-dismiss="alert">&times;</button>' + message)
        .appendTo('#' + target);
}

/**
* resetMap
*
* Clears ALL map elements - plotted items, overlays, then resets position
*/
function resetMap() {
    // Remove markers and legend    
    resetMapDrawings();
    $("#rainbow-legend-container").hide();
    $("#submit-button").attr("disabled", false);
 
    // Reset map center and zoom
    map.setCenter(38.89, -77.03, function () {});
    map.setZoom(4);

    // Reset rectangle drawing
    if (selectionRectangle) {
        selectionRectangle.setMap(null);
        selectionRectangle = null;
    }
    rectangleManager.setMap(map.map);
    rectangleManager.setDrawingMode(google.maps.drawing.OverlayType.RECTANGLE);
}

function resetMapDrawings() {
  for (var i = 0; i < drawings.length; i++) {
    drawings[i].setMap(null);
  }
}

/**
* buildLegend
*
* Generates gradient, button action for legend bar
*/
function buildLegend() {
    // Create a rainbow from a pretty color scheme.
    // http://www.colourlovers.com/palette/292482/Terra
    rainbow.setSpectrum("#E8DDCB", "#CDB380", "#036564", "#033649", "#031634");

    // Add blocks to gradient because I'm useless at fancy css
    for (i = 0; i<100; i++) {
        $("#legend-gradient").append('<div style="display:inline-block; max-width:2px; background-color:#' + rainbow.colourAt(i) +';">&nbsp;</div>');
    }
}
