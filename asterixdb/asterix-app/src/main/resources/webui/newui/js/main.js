var SERVER_HOST = "http://localhost:19002";
var DATAVERSE_QUERY = "for $x in dataset Metadata.Dataverse return $x;"
var DATE_TIME_REGEX = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z)$/;
var DATE_REGEX = /^(\d{4}-\d{2}-\d{2})$/;

var app = angular.module('queryui', []);

app.controller('queryCtrl', function($rootScope, $scope, $http) {

  $scope.current_tab = 0;
  $scope.current_list = 0;
  $scope.maximized = false;
  $scope.results = [];
  $scope.history = [];
  $scope.dataverses = [];
  $scope.selected_dataverse = "";
  $scope.errorText = null;
  $scope.statusText = "Web UI Ready";
  $scope.query_input =  "";

  $scope.init = function(){
    $http.get(SERVER_HOST+"/query?query="+encodeURI(DATAVERSE_QUERY)).then(function(response){
      for (i in response.data){
        $scope.dataverses.push(response.data[i].DataverseName);
      }
    },
    function(response){
      $scope.statusText = "Error Occured Executing Query";
      $scope.errorText = response.data.summary;
      $scope.maximized = false;
    });
  }

  $scope.query = function(){
    var timer = new Date().getTime();
    $scope.history.push($scope.query_input);
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

  $scope.ObjectKeys = function(obj){
    var res = [];
    for (var key in obj) {
      if (obj.hasOwnProperty(key) && !(key === "$$hashKey")) {
        res.push(key)
      }
    }
    return res;
  }

  $scope.ObjectValue = function(obj){
    var typeStr = Object.prototype.toString.call(obj);
    var dateType = new Date(obj);
    if (typeStr === "[object Array]"){
      return "Record +";
    }else if (typeStr === "[object object]"){
      return "Record +";
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

  $scope.isRecordPlus = function(obj){
    var typeStr = Object.prototype.toString.call(obj);
    if ((typeStr === "[object Array]" ) || (typeStr === "[object object]")){
      return "asterix-nested";
    }else{
      return "";
    }
  }

});
