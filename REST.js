const $ = require('jquery');
var request = require("request");

/*Get document
* id = collectionName/_key
* [optional] database = databasename [default is _system]
*/
module.exports.getDocument = function(id, database = '_system', endpoint, authToken){

  var options = 
  { 
    method: 'GET',
    url: endpoint + "_db/" +  database + "/_api/document/" + id,
    headers: {
      "authorization": authToken,
      "content-type": "application/json",
      "cache-control": "no-cache",
    }
  };

  //console.log(options);
  
  return new Promise((resolve, reject) => {
    request(options, function(error, response, body){

      if(error){
        console.log(error);
        throw new Error(error);
      }

      body = JSON.parse(body);
      error = body.error;

      console.log(error);

      if (error){
        console.log(error);
        reject(error);
      }
      
      //console.log(body);
      resolve(body);
    });
  });
};

/*Insert document
* body = json object to be inserted
* [optional] database = databasename [default is _system]
*/
module.exports.insertDocument = function (body, collection, database = '_system', endpoint, authToken) {
  
  
  var options = 
  { 
    method: 'POST',
    url: endpoint + "_db/" +  database + "/_api/document/" + collection + "/",
    headers: {
      "authorization": authToken,
      "content-type": "application/json",
      "cache-control": "no-cache",
    },
    body: body
  };

  //console.log(options);
  
  return new Promise((resolve, reject) => {
    request(options, function(error, response, body){

      if(error){
        console.log(error);
        throw new Error(error);
      }

      body = JSON.parse(body);
      error = body.error;

      console.log(error);

      if (error){
        console.log("INSERT ERROR!");
        console.log(error);
        reject(error);
      }
      
      //console.log(body);
      resolve(body);
    });
  });
};

/* Make grapgh
*  body = json object containing at least
* 
*/
module.exports.graph = function (graphName, edgeCollection, collectionArray, database = '_system', endpoint, authToken){
  
    var options = 
    { 
      method: 'POST',
      url: endpoint + "_db" + database + "/_api/cursor/",
      headers: {
        "authorization": authToken,
        "content-type": "application/json",
        "cache-control": "no-cache",
      },
      "data": 
      {
        "name": graphName,
        "edgeDefinitions": [
          {
            "collection": edgeCollection,
            "from": collectionArray,
            "to": collectionArray,
          }
        ]
      }
    };
  
    return new Promise((resolve, reject) => {
      
      request(options, function (error, response, body) {
  
        console.log(response.statusCode);
  
        if(error){
          console.log(error);
          throw new Error(error);
        }
  
        console.log(body);
        body = JSON.parse(body);
        error = body.error;
        
        if (error) {
          console.log(body);
          reject(body.errorMessage);
          //throw new Error(error);
        }
        console.log(typeof body);
        console.log(body.error);
        console.log(body.code);
        resolve(body);
      })
    });
  };

/* query the database
*  body = json object containing at least
*  the key 'query' with a text of AQL query
*  
*  including the key 'batchSize' in the body object and setting it
*  to an number will limit the number of results returned.
*
*  An indicator in the result 'hasMore' is a true/false value
*  telling if there's more to get.
*  To get the next batch of results use tje 'id' together with
*  the function getNextBatch
*
*  [optional] database = databasename [default is _system]
* 
*/
module.exports.query = function (body, database = '_system', endpoint, authToken){

  var options = 
  { 
    method: 'POST',
    url: endpoint + "_db" + database + "/_api/cursor/",
    headers: {
      "authorization": authToken,
      "content-type": "application/json",
      "cache-control": "no-cache",
    },
    "data": body
  };

  return new Promise((resolve, reject) => {
    
    request(options, function (error, response, body) {

      console.log(response.statusCode);

      if(error){
        console.log(error);
        throw new Error(error);
      }

      console.log(body);
      body = JSON.parse(body);
      error = body.error;
      
      if (error) {
        console.log(body);
        reject(body.errorMessage);
        //throw new Error(error);
      }
      console.log(typeof body);
      console.log(body.error);
      console.log(body.code);
      resolve(body);
    })
  });
};

/*Get next batch
* id = the ID returned from a resultset of a query
*
* [optional] database = databasename [default is _system]
*
*/
module.exports.getNext = function (id, database = '_system', endpoint, authToken){

  var options = 
  { 
    method: 'PUT',
    url: endpoint + "_db/" +  database + "_api/cursor/" + id,
    headers: {
      "authorization": authToken,
      "content-type": "application/json",
      "cache-control": "no-cache",
    }
  };

  //console.log(options);
  
  return new Promise((resolve, reject) => {
    request(options, function(error, response, body){

      if(error){
        console.log(error);
        throw new Error(error);
      }

      body = JSON.parse(body);
      error = body.error;

      console.log(error);

      if (error){
        console.log(error);
        reject(error);
      }
      
      //console.log(body);
      resolve(body);
    });
  });
};

/* Explain a query
*  body = json object containing 
*  the key 'query' with a text of AQL query
*
* [optional] database = databasename [default is _system]
*/
module.exports.explain = function(body, database = '_system', endpoint, authToken){
  var options = 
  { 
    method: 'POST',
    url: endpoint + "_db/" +  database + "_api/explain/",
    headers: {
      "authorization": authToken,
      "content-type": "application/json",
      "cache-control": "no-cache",
    },
    "data": body
  };

  //console.log(options);
  
  return new Promise((resolve, reject) => {
    request(options, function(error, response, body){

      if(error){
        console.log(error);
        throw new Error(error);
      }

      body = JSON.parse(body);
      error = body.error;

      console.log(error);

      if (error){
        console.log(error);
        reject(error);
      }
      
      //console.log(body);
      resolve(body);
    });
  });
  
};

/*Create collection
* body = a JSON object with minimum a key 'name' that's the collection name.
* add the key 'type' and set the value to '3' to create an edge collection
*
* [optional] database = databasename [default is _system]
*/
module.exports.createCollection = function (collection, type = '',database = '_system', endpoint, authToken){

  console.log("create collection");

  var options =
  {
    method: 'POST',
    url: endpoint + "_db/" + database + "/_api/collection/",
    headers: {
      "authorization": authToken,
      "content-type": "application/json",
      "cache-control": "no-cache",
    },
    body: JSON.stringify({"name": collection, "type": type})
  };

  return new Promise((resolve, reject) => {
    request(options, function(error, response, body){
      body = JSON.parse(body);
      console.log(body);
      error = body.error;

      if (error){
        console.log(body);
        reject(body.errorMessage);
      }
      
      resolve(body);
    });
  });
};

/*List collections
* [optional] database = databasename [default is _system]
*/
module.exports.listCollections = function (collection = '', database = '_system', endpoint, authToken) {

  var options = 
  { 
    method: 'GET',
    url: endpoint + "_db/" + database + "/_api/collection/" + collection,
    headers: {
      "authorization": authToken,
      "content-type": "application/json",
      "cache-control": "no-cache",
    },
    body: ""
  };

  console.log(options);

  var error = null;
  var code = 200;
  
  return new Promise((resolve, reject) => {
    
    request(options, function (error, response, body) {
      if(error){
        console.log(error);
        throw new Error(error);
      }

      if(response.statusCode > 400 && response.statusCode != 404){
        console.log(response.statusCode + " - " + response.statusMessage);
        throw new Error(response.statusCode);
      }

      console.log(body);
      body = JSON.parse(body);
      error = body.error;
      
      if (error) {
        console.log(body);
        reject(body.errorMessage);
        //throw new Error(error);
      }
      console.log(typeof body);
      console.log(body.error);
      console.log(body.code);

      error = body.error;
      code = body.code;

      resolve(body);
    })
  });
};

/*List database*/
module.exports.listDatabases = function(endpoint, authToken){
  var options = 
  { 
    method: 'GET',
    url: endpoint + "/_api/database/",
    headers: {
      "authorization": authToken,
      "content-type": "application/json",
      "cache-control": "no-cache",
    }
  };
  
  return new Promise((resolve, reject) => {
    
    request(options, function (error, response, body) {

      console.log(response.statusCode);

      if(error){
        console.log(error);
        throw new Error(error);
      }

      console.log(body);
      body = JSON.parse(body);
      error = body.error;
      
      if (error) {
        console.log(body);
        reject(body.errorMessage);
        //throw new Error(error);
      }
      console.log(typeof body);
      console.log(body.error);
      console.log(body.code);
      resolve(body);
    })
  });

  
};

/*Create database
* body = a JSON object with at least the key 'name' with the name of the DB to create
*/
module.exports.createDatabase = function(body, endpoint, authToken){
  var options = 
  { 
    method: 'POST',
    url: endpoint + "/_api/database/",
    headers: {
      "authorization": authToken,
      "content-type": "application/json",
      "cache-control": "no-cache",
    },
    "data": body
  };

  return new Promise((resolve, reject) => {
    
    request(options, function (error, response, body) {

      console.log(response.statusCode);

      if(error){
        console.log(error);
        throw new Error(error);
      }

      console.log(body);
      body = JSON.parse(body);
      error = body.error;
      
      if (error) {
        console.log(body);
        reject(body.errorMessage);
        //throw new Error(error);
      }
      console.log(typeof body);
      console.log(body.error);
      console.log(body.code);
      resolve(body);
    })
  });
};


/*delete database
* db = Name of the db to delete
*/
module.exports.deleteDatabase = function (db, endpoint, authToken){

  var options = 
  { 
    method: 'DELETE',
    url: endpoint + "/_api/database/" + db,
    headers: {
      "authorization": authToken,
      "content-type": "application/json",
      "cache-control": "no-cache",
    },
    "data": body
  };

  return new Promise((resolve, reject) => {
    
    request(options, function (error, response, body) {

      console.log(response.statusCode);

      if(error){
        console.log(error);
        throw new Error(error);
      }

      console.log(body);
      body = JSON.parse(body);
      error = body.error;
      
      if (error) {
        console.log(body);
        reject(body.errorMessage);
        //throw new Error(error);
      }
      console.log(typeof body);
      console.log(body.error);
      console.log(body.code);
      resolve(body);
    })
  });
};