var SERVER_HOST = "http://"+location.hostname+":19002";
var DATAVERSE_QUERY = "for $x in dataset Metadata.Dataverse return $x;"
var DATE_TIME_REGEX = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z)$/;
var DATE_REGEX = /^(\d{4}-\d{2}-\d{2})$/;

var app = angular.module('queryui', ['jsonFormatter','ui.codemirror']);

app.service('recordFunctions', function(){
  this.ObjectKeys = function(obj){
    var res = [];
    for (var key in obj) {
      if (obj.hasOwnProperty(key) && !(key === "$$hashKey")) {
        res.push(key)
      }
    }
    return res;
  }
  this.isObject = function(obj){
    var typeStr = Object.prototype.toString.call(obj);
    if (typeStr === "[object Object]"){
      return true;
    }else{
      return false;
    }
  }
  this.isArray = function(obj){
    var typeStr = Object.prototype.toString.call(obj);
    if ((typeStr === "[object Array]" )){
      return true;
    }else{
      return false;
    }
  }
  this.isNested = function(obj){
    return  this.isObject(obj) || this.isArray(obj);
  }
  this.ObjectValue = function(obj){
    var typeStr = Object.prototype.toString.call(obj);
    var dateType = new Date(obj);
    if (typeStr === "[object Array]"){
      return "Record +";
    }else if (typeStr === "[object Object]"){
      return "Record +";
    }else if (typeStr == "[object Null]"){
      return "NULL";
    }else if(DATE_REGEX.exec(obj) != null){
      var dat = new Date(obj);
      return dat.getUTCFullYear()+"/"+dat.getUTCMonth()+"/"+dat.getUTCDay();
    }else if(DATE_TIME_REGEX.exec(obj) != null){
      var dat = new Date(obj);
      return dat.getUTCFullYear()+"/"+dat.getUTCMonth()+"/"+dat.getUTCDay()
        +" "+dat.getUTCHours()+":"+dat.getUTCMinutes()+":"+dat.getUTCSeconds();
    }else{
      return obj;
    }
  }
});

app.controller('queryCtrl', function($rootScope, $scope, $http, recordFunctions) {

  $scope.recordFunctions = recordFunctions;
  $scope.current_tab = 0;
  $scope.current_preview_tab = 0;
  $scope.current_list = 0;
  $scope.maximized = false;
  $scope.results = [];
  $scope.history = [];
  $scope.dataverses = [];
  $scope.selectedItem = null;
  $scope.selected_dataverse = "";
  $scope.errorText = null;
  $scope.statusText = "Web UI Ready";
  $scope.query_input =  "";

  $scope.queryCmOptions ={
      lineNumbers: true,
      indentWithTabs: true,
      lineWrapping: true,
      mode: 'aql'
  }

  $scope.queryPreviewOptions ={
      indentWithTabs: true,
      lineWrapping: true,
      mode: 'javascript',
      readOnly : true
  }

  $scope.init = function(){
    $http.get(SERVER_HOST+"/query?query="+encodeURI(DATAVERSE_QUERY)).then(function(response){
      for (i in response.data){
        $scope.dataverses.push(response.data[i].DataverseName);
        $scope.selected_dataverse = $scope.dataverses[0];
      }
    },
    function(response){
      $scope.statusText = "Error Occured Executing Query";
      $scope.errorText = response.data.summary;
      $scope.maximized = false;
    });
    $scope.load();
  }

  $scope.query = function(){
    var timer = new Date().getTime();
    $scope.save($scope.query_input,$scope.selected_dataverse);
    $http.get(SERVER_HOST+"/query?query="+encodeURI("use dataverse "+$scope.selected_dataverse+";"+$scope.query_input)).then(function(response){
      $scope.results = response.data;
      console.log(response);
      timer = new Date().getTime() - timer;
      $scope.statusText = "Query returned " + $scope.results.length + " record in " + timer + "ms";
      $scope.errorText = null;
      $scope.maximized = false;
    },
    function(response){
      $scope.statusText = "Error Occured Executing Query";
      $scope.errorText = response.data.summary;
      $scope.maximized = false;
      $scope.results = [];
    });
  }

  $scope.isNested = function(obj){
    for (var key in obj) {
      if (obj.hasOwnProperty(key)) {
        var typeStr = Object.prototype.toString.call(obj[key]);
        if (typeStr === "[object Array]" || typeStr === "[object Object]") return true;
      }
    }
    return false;
  }

  $scope.isRecordPlus = function(obj){
      return $scope.recordFunctions.isNested(obj) ? "asterix-nested" : "";
  }

  $scope.viewRecord = function(obj){
    $scope.selectedItem = obj;
    $scope.current_preview_tab = 0;
    $("#recordModel").modal();
  }

  $scope.previewJSON = function(obj){
    return JSON.stringify(obj,null,4);
  }

  $scope.save = function(query, database){
    var toSave = [query, database];
    if($scope.history.push(toSave) == 11){
      $scope.history.shift();
    }
    localStorage.setItem("history",JSON.stringify($scope.history));
  }

  $scope.load = function(){
      var history = JSON.parse(localStorage.getItem("history"));
      if (history != null) $scope.history = history;
  }

  $scope.previewHistory = function(entry){
      $scope.current_tab = 0;
      $scope.query_input = entry[0];
      $scope.selected_dataverse = entry[1];
  }

});
