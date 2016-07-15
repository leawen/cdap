/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

function LogViewerController ($scope, LogViewerStore, myLogsApi, LOGVIEWERSTORE_ACTIONS) {
  'ngInject';

  this.data = {};
  this.errorCount = 0;
  this.warningCount = 0;
  this.loading = true;
  this.loadingMoreLogs = true;

  this.configOptions = {
    time: true,
    level: true,
    source: true,
    message: true
  };

  this.hiddenColumns = {
    time: false,
    level: false,
    source: false,
    message: false
  };

  //viewLimit and cacheDecrement should match
  this.viewLimit = 50;
  this.cacheDecrement = 50;
  this.cacheSize = 0;

  //Collapsing LogViewer Table Columns
  var theColumns = [];
  var cols = this.configOptions;

  if(cols['source']){
    theColumns.push('source');
  }
  if(cols['level']){
    theColumns.push('level');
  }
  if(cols['time']){
    theColumns.push('time');
  }

  var collapseCount = 0;
  this.collapseColumns = () => {
    if(this.isMessageExpanded){
      this.isMessageExpanded = !this.isMessageExpanded;
    }
    if(collapseCount < theColumns.length){
      this.hiddenColumns[theColumns[collapseCount++]] = true;
      if(collapseCount === theColumns.length){
        this.isMessageExpanded = true;
      }
    } else {
      collapseCount = 0;
      for(var key in this.hiddenColumns){
        if(this.hiddenColumns.hasOwnProperty(key)){
          this.hiddenColumns[key] = false;
        }
      }
    }
  };

  this.updateScrollPositionInStore = function(val) {
    console.log('updating scroll position in store');
    LogViewerStore.dispatch({
      type: LOGVIEWERSTORE_ACTIONS.SCROLL_POSITION,
      payload: {
        scrollPosition: val
      }
    });
  };

  LogViewerStore.subscribe(() => {
    this.logStartTime = LogViewerStore.getState().startTime;
    //Convert start time to seconds
    this.startTimeSec = Math.floor(this.logStartTime.getTime()/1000);
    requestWithStartTime();
  });

  myLogsApi.nextLogsJson({
    'namespace' : this.namespaceId,
    'appId' : this.appId,
    'programType' : this.programType,
    'programId' : this.programId,
    'runId' : this.runId
  }).$promise.then(
    (res) => {
      this.totalCount = res.length;

      angular.forEach(res, (element, index) => {
        if(res[index].log.logLevel === 'WARN'){
          this.warningCount++;
        } else if(res[index].log.logLevel === 'ERROR'){
          this.errorCount++;
        }
        let formattedDate = new Date(res[index].log.timestamp);
        res[index].log.timestamp = formattedDate;
        res[index].log.displayTime = ((formattedDate.getMonth() + 1) + '/' + formattedDate.getDate() + '/' + formattedDate.getFullYear() + ' ' + formattedDate.getHours() + ':' + formattedDate.getMinutes() + ':' + formattedDate.getSeconds());
      });
      this.data = res;
      this.loading = false;
      this.fromOffset = res[res.length-1].offset;
      this.cacheSize = res.length - this.cacheDecrement;
    },
    (err) => {
      console.log('ERROR: ', err);
    });

  const requestWithOffset = () => {
    myLogsApi.nextLogsJsonOffset({
      'namespace' : this.namespaceId,
      'appId' : this.appId,
      'programType' : this.programType,
      'programId' : this.programId,
      'runId' : this.runId,
      'fromOffset' : this.fromOffset
    }).$promise.then(
      (res) => {

        if(res.length === 0){
          this.loadingMoreLogs = false;
          return;
        }

        this.fromOffset = res[res.length-1].offset;
        this.totalCount += res.length;

        angular.forEach(res, (element, index) => {
          if(res[index].log.logLevel === 'WARN'){
            this.warningCount++;
          } else if(res[index].log.logLevel === 'ERROR'){
            this.errorCount++;
          }

          //Format dates properly for rendering and computing
          let formattedDate = new Date(res[index].log.timestamp);
          res[index].log.timestamp = formattedDate;
          res[index].log.displayTime = ((formattedDate.getMonth() + 1) + '/' + formattedDate.getDate() + '/' + formattedDate.getFullYear() + ' ' + formattedDate.getHours() + ':' + ((formattedDate.getMinutes()<10) ? '0'+formattedDate.getMinutes() : formattedDate.getMinutes()) + ':' + formattedDate.getSeconds());
        });

        this.data = this.data.concat(res);
        this.cacheSize = res.length - this.cacheDecrement;
      },
      (err) => {
        console.log('ERROR: ', err);
      });
  };

  // const computePinPosition = () => {
  //   let scrollTime;

  //   //Condition: waiting for more data ; poll for more
  //   if(this.data.length < this.viewLimit){
  //     scrollTime = this.data[this.data.length - 1].log.timestamp;
  //     //Poll for data and update table when it comes available
  //   } else {
  //     //If polling, stop
  //     scrollTime = this.data[this.viewLimit - 1].log.timestamp;
  //   }

  //   //If returns date in readable format
  //   if(typeof scrollTime !== 'number'){
  //     scrollTime = new Date(scrollTime).getTime();
  //   }

  //   this.updateScrollPositionInStore(scrollTime);
  // };

  const requestWithStartTime = () => {
   myLogsApi.getLogsStart({
      'namespace' : this.namespaceId,
      'appId' : this.appId,
      'programType' : this.programType,
      'programId' : this.programId,
      'runId' : this.runId,
      'start' : this.startTimeSec
    }).$promise.then(
      (res) => {

        this.viewLimit = 50;
        this.cacheDecrement = 50;
        this.cacheSize = 0;
        this.loadingMoreLogs = true;

        console.log('Returned with new start time: ', res);
        console.log('Start time: ', this.startTimeSec);

        //There are no more logs to be returned
        if(res.length === 0){
          this.loadingMoreLogs = false;
          return;
        }
        //clear the current array
        this.data.length = 0;

        // console.log('this is the new response:')
        this.fromOffset = res[res.length-1].offset;
        this.totalCount = res.length;
        this.warningCount = 0;
        this.errorCount = 0;
        angular.forEach(res, (element, index) => {
          if(res[index].log.logLevel === 'WARN'){
            this.warningCount++;
          } else if(res[index].log.logLevel === 'ERROR'){
            this.errorCount++;
          }
          let formattedDate = new Date(res[index].log.timestamp);
          res[index].log.timestamp = formattedDate;
          res[index].log.displayTime = ((formattedDate.getMonth() + 1) + '/' + formattedDate.getDate() + '/' + formattedDate.getFullYear() + ' ' + formattedDate.getHours() + ':' + formattedDate.getMinutes() + ':' + formattedDate.getSeconds());
        });
        this.data = res;
        this.cacheSize = res.length - this.cacheDecrement;
      },
      (err) => {
        console.log('ERROR: ', err);
      });
  };

  angular.forEach($scope.displayOptions, (value, key) => {
    this.configOptions[key] = value;
  });

  this.logEvents = ['ERROR', 'WARN', 'INFO', 'DEBUG', 'TRACE'];

  let included = {
    'ERROR' : false,
    'WARN' : false,
    'INFO' : false,
    'DEBUG' : false,
    'TRACE' : false
  };

  let numEvents = 0;
  this.toggleExpandAll = false;

  this.toggleLogExpansion = function() {
    this.toggleExpandAll = !this.toggleExpandAll;
    angular.forEach(this.data, (data) => {
      if(data.log.stackTrace.length > 0){
        data.isStackTraceExpanded = this.toggleExpandAll;
      }
    });
  };

  this.includeEvent = function(eventType){
    if(included[eventType]){
      numEvents--;
    } else{
      numEvents++;
    }
    included[eventType] = !included[eventType];
  };

  this.eventFilter = function(entry){
    if(numEvents === 0 || included[entry.log.logLevel]){
      return entry;
    }
    return;
  };

  this.scrollFn = _.debounce(function(){
    this.loadingMoreLogs = true;
    this.cacheSize -= this.cacheDecrement;
    if(this.cacheSize <= 0){
      requestWithOffset();
    }
    // computePinPosition();
    this.viewLimit += this.cacheDecrement;
  }, 1000);

  this.filterByStartDate = (entry) => {
    if(this.logStartTime > entry.log.timestamp) {
      return;
    }
    return entry;
  };
}

angular.module(PKG.name + '.commons')
  .directive('myLogViewer', function () {
    return {
      templateUrl: 'log-viewer/log-viewer.html',
      controller: LogViewerController,
      scope: {
        displayOptions: '=?',
        namespaceId: '@',
        appId: '@',
        programType: '@',
        programId: '@',
        runId: '@'
      },
      bindToController: true,
      controllerAs: 'LogViewer'
    };
  });
