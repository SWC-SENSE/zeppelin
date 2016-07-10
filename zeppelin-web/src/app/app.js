/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';
(function() {
    var zeppelinWebApp = angular.module('zeppelinWebApp', [
            'ngCookies',
            'ngAnimate',
            'ngRoute',
            'ngSanitize',
            'angular-websocket',
            'ui.ace',
            'ui.bootstrap',
            'as.sortable',
            'ngTouch',
            'ngDragDrop',
            'angular.filter',
            'monospaced.elastic',
            'puElasticInput',
            'xeditable',
            'ngToast',
            'focus-if',
            'ngResource',
            'leaflet-directive'
        ])
        .filter('breakFilter', function() {
            return function (text) {
                if (!!text) {
                    return text.replace(/\n/g, '<br />');
                }
            };
        })
        .config(function ($httpProvider, $routeProvider, ngToastProvider) {
            // withCredentials when running locally via grunt
            $httpProvider.defaults.withCredentials = true;

            $routeProvider
                .when('/', {
                    templateUrl: 'app/home/home.html'
                })
                .when('/notebook/:noteId', {
                    templateUrl: 'app/notebook/notebook.html',
                    controller: 'NotebookCtrl'
                })
                .when('/notebook/:noteId/paragraph?=:paragraphId', {
                    templateUrl: 'app/notebook/notebook.html',
                    controller: 'NotebookCtrl'
                })
                .when('/notebook/:noteId/paragraph/:paragraphId?', {
                    templateUrl: 'app/notebook/notebook.html',
                    controller: 'NotebookCtrl'
                })
                .when('/interpreter', {
                    templateUrl: 'app/interpreter/interpreter.html',
                    controller: 'InterpreterCtrl'
                })
                .when('/credential', {
                    templateUrl: 'app/credential/credential.html',
                    controller: 'CredentialCtrl'
                })
                .when('/configuration', {
                  templateUrl: 'app/configuration/configuration.html',
                  controller: 'ConfigurationCtrl'
                })
                .when('/search/:searchTerm', {
                    templateUrl: 'app/search/result-list.html',
                    controller: 'SearchResultCtrl'
                })
                .otherwise({
                    redirectTo: '/'
                });

            ngToastProvider.configure({
                dismissButton: true,
                dismissOnClick: false,
                timeout: 6000
            });
        });


  // After the AngularJS has been bootstrapped, you can no longer
  // use the normal module methods (ex, app.controller) to add
  // components to the dependency-injection container. Instead,
  // you have to use the relevant providers. Since those are only
  // available during the config() method at initialization time,
  // we have to keep a reference to them.
  // --
  // NOTE: This general idea is based on excellent article by
  // Ifeanyi Isitor: http://ify.io/lazy-loading-in-angularjs/

  zeppelinWebApp.config(
    function( $controllerProvider, $provide, $compileProvider ) {
      // Since the "shorthand" methods for component
      // definitions are no longer valid, we can just
      // override them to use the providers for post-
      // bootstrap loading.
      console.log( 'Config method executed.' );
      // Let's keep the older references.
      zeppelinWebApp._controller = zeppelinWebApp.controller;
      zeppelinWebApp._service = zeppelinWebApp.service;
      zeppelinWebApp._factory = zeppelinWebApp.factory;
      zeppelinWebApp._value = zeppelinWebApp.value;
      zeppelinWebApp._directive = zeppelinWebApp.directive;
      // Provider-based controller.
      zeppelinWebApp.controller = function( name, constructor ) {
        $controllerProvider.register( name, constructor );
        return( this );
      };
      // Provider-based service.
      zeppelinWebApp.service = function( name, constructor ) {
        $provide.service( name, constructor );
        return( this );
      };
      // Provider-based factory.
      zeppelinWebApp.factory = function( name, factory ) {
        $provide.factory( name, factory );
        return( this );
      };
      // Provider-based value.
      zeppelinWebApp.value = function( name, value ) {
        $provide.value( name, value );
        return( this );
      };
      // Provider-based directive.
      zeppelinWebApp.directive = function( name, factory ) {
        $compileProvider.directive( name, factory );
        return( this );
      };
      // NOTE: You can do the same thing with the "filter"
      // and the "$filterProvider"; but, I don't really use
      // custom filters.
    }
  );



  window.zeppelin = {
    components: {},
    addComponent : function(name,template,scopeFunction){
      var components = window.zeppelin.components;
      var exists = (components[name]!==undefined);
      if (!exists) {
        components[name] = {};
        angular.module('zeppelinWebApp').directive(name, ['$compile',function ($compile) {
          return {
            restrict: 'E',
            template: '' ,
            replace: true,
            link: function (scope, element) {
              element.html(components[name].template);
              $compile(element.contents())(scope);
              components[name].scopeFunction(scope,element);
            }
          };
        }]);
      }

      components[name].template = template;
      components[name].scopeFunction = scopeFunction;
      this.reloadComponent(name)
    },

    toTagName:function(directiveName){
      var result = "";
      for(var i = 0; i<directiveName.length;i++){
        var char = directiveName[i];
        if(isNaN(char) && char == char.toUpperCase()){
          result = result + "-";
        }
        result = result + char.toLowerCase();
      }
      return result;
    },
    reloadComponent: function (name) {
      var tagName = this.toTagName(name);
      var elements = $(tagName);
      elements.each(function (index,el) {
        var aElem = angular.element(el);
        aElem.injector().invoke(['$compile',function($compile) {
   		var scope = aElem.scope();
		scope.$emit("clean");
	       $compile(aElem)(scope)
        }]);
      });
    }
  };


    function auth() {
        var $http = angular.injector(['ng']).get('$http');
        var baseUrlSrv = angular.injector(['zeppelinWebApp']).get('baseUrlSrv');
        // withCredentials when running locally via grunt
        $http.defaults.withCredentials = true;

        return $http.get(baseUrlSrv.getRestApiBase()+'/security/ticket').then(function(response) {
            zeppelinWebApp.run(function($rootScope) {
                $rootScope.ticket = angular.fromJson(response.data).body;
            });
        }, function(errorResponse) {
            // Handle error case
        });
    }

    function bootstrapApplication() {
        angular.bootstrap(document, ['zeppelinWebApp']);
    }


    angular.element(document).ready(function() {
        auth().then(bootstrapApplication);
    });

}());

