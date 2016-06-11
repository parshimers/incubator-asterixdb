CodeMirror.defineMode("aql", function() {

  var KEYWORD_MATCH = ["return","select","for","from","at","in","let","with","where","order","by","asc","desc","group",
                        "keeping","limit","offset","distinct"];
  var VAR_MATCH = /[$][a-zA-Z]+(\d*)/;
  var DOUBLE_QUOTE_MATCH = /["].*["]/g;
  var SINGLE_QUOTE_MATCH = /['].*[']/g;
  return {
    startState: function() {return {inString: false};},
    token: function(stream, state) {

          //match variable reference
          if (stream.match(VAR_MATCH)) {
            return "variable-2";
          }

          //string variable match
          if (stream.match(DOUBLE_QUOTE_MATCH)) {
            return "string";
          }
          if (stream.match(SINGLE_QUOTE_MATCH)) {
            return "string";
          }

          //keyword match
          for (i in KEYWORD_MATCH){
            if (stream.match(KEYWORD_MATCH[i])){
                return "keyword"
             }
          }

          stream.next();

      }
  };
});