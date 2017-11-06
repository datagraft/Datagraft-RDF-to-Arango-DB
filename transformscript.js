/* load the rdf mapping and vocab into the script */
//.load rdfMapping.js
//.load rdfVocab.js
const bodyParser = require('body-parser');
const jsonParser = bodyParser.json();
const logging = require('./logging');
const multer  = require('multer');
const pa = require('./papaparse.min.js');
const fs = require('fs');
const rl = require('readline');
const rest = require('./REST.js');

const request = require('request');


const storage =   multer.diskStorage({
  destination: function (req, file, callback) {
     callback(null, './uploads/');
  },
  filename: function (req, file, callback) {
    callback(null, file.fieldname + '-' + Date.now());
  }
});

const upload = multer({ storage: storage });

const cpUpload = upload.fields([{ name: 'csv', maxCount: 1 }, { name: 'mapping', maxCount: 1 }, { name: 'vocabulary', maxCount: 1 }]);

module.exports = (app) => {

    app.post('/', cpUpload, (req, res) => {
        console.log(req.body);
      
        const showAndLogError = (res, status, message, data) => {
          // If the headers are already sent, it probably means the server has started to
          // provide a message and it's better to just keep the same message instead of
          // crashing trying to send already sent headers
          if (!res.headersSent) {
            res.status(status).json({
              error: message,
              data
            });
          }
      
          logging.error(message, data);
        };
    
        const mapping =   JSON.parse(fs.readFileSync(req.files.mapping[0].path).toString());
        const vocabulary = JSON.parse(fs.readFileSync(req.files.vocabulary[0].path).toString()); 
        const csv = req.files.csv[0];
        const path = req.files.csv[0].path;

        //Params for REST calls
        const useRest = req.body.REST;
        const endpoint = req.body.endpoint;
        const db = req.body.db;
        const name = req.body.name;
        const split = req.body.split;
        const authToken = req.body.authToken;
        
        if (!mapping) {
          showAndLogError(res, 400, 'The RDF mapping is missing');
          return;
        }
    
        if (!vocabulary) {
          showAndLogError(res, 400, 'The RDF vocabulary is missing');
          return;
        }
    
        if (!csv) {
          showAndLogError(res, 400, 'The source csv is missing');
          return;
        }

        if(useRest) {
            

            if(!endpoint) {
                showAndLogError(res, 400, 'Missing endpoint path');
                return;
            }

            if(!db) {
                showAndLogError(res, 400, 'Missing db name');
                return;
            }

            if(!name) {
                showAndLogError(res, 400, 'Missing a name for the dataset / transformation');
                return;
            }

            rest.listCollections(name, db, endpoint, authToken)
                .then(result => {
                    console.log(result)
                    run();
                })
                .catch(reason => {
                    //We didn't have the collection try to create them
                    console.log(reason);

                    var creColl = false;
                    var creEdge = false;

                    //create the collection
                    rest.createCollection(name, db, endpoint, authToken)
                        .then(result => {
                            console.log(result)
                            creColl = true;
                            if(creColl && creEdge){
                                creColl = false;
                                creEdge = false;
                                run();
                            }
                        })
                        .catch(result => {showAndLogError(res, 500, 'could not create collection'); console.log("could not make normal collection")}); 

                    //create the edge collection
                    rest.createCollection(name+"_edge", "3", db, endpoint, authToken)
                        .then(result => {
                            console.log(result)
                            creEdge = true;
                            if(creColl && creEdge){
                                creColl = false;
                                creEdge = false;
                                run();
                            }
                        })
                        .catch(result => {showAndLogError(res, 500, 'could not create collection'); console.log("could not make edge collection")}); 
                    });
        }else{
            run();
        }

        function run(){
    
        var vocab = [];
        var headings = {};
        
        /*Make vocabs for dataset*/
        for(var i = 0; i < vocabulary.length; i++){
            if(i == 0){vocab = [];}
            var key = vocabulary[i].name;
            vocab[key] = vocabulary[i];
        }
        /*Hash function  - used to hash namespaces for keys*/
        String.prototype.hashCode = function () {
            var hash = 0;
            if (this.length == 0) return hash;
            for (i = 0; i < this.length; i++) {
                char = this.charCodeAt(i);
                hash = ((hash << 5) - hash) + char;
                hash = hash & hash; // Convert to 32bit integer
            }
            return (hash + 2147483647) + 1; 
        };

        function write_array(arangoValue, arangoEdge){
            
            //var stamp = new Date().toISOString().replace('T', ' ').replace('.', '')
            var stamp = Date.now();
            fs.writeFile("results/" + stamp + "_arango_value.json", JSON.stringify(arangoValue), function(err){
                if(err) {
                    return console.log(err);
                }
                console.log("The file was saved!");
            });
            
            fs.writeFile("results/" + stamp + "_arango_edge.json", JSON.stringify(arangoEdge), function(err) {
                
                if(err) {
                    return console.log(err);
                }
                console.log("The file was saved!");
            });
        }
        
        function write_object(arangoValue, arangoEdge){
            //var stamp = new Date().toISOString().replace('T', ' ').replace('.', '')
            var stamp = Date.now();
            //write nodes
            console.log("writing node values to file...");
            for(var i = 0; i < arangoValue.length; i++){
                fs.appendFileSync("results/" + stamp + "_arango_value.json", JSON.stringify(arangoValue[i]) + '\n', function(err) {
                    if(err) {
                        return console.log(err);
                    }
                    //console.log("The file was started...");
                }); 
            }
            
            console.log("The file \""+stamp+"_arango_value.json\" was saved!");
            console.log("writing edge values to file...");
            
            //Write edges
            for(var i = 0; i < arangoEdge.length; i++){
                fs.appendFileSync("results/" + stamp + "_arango_edge.json", JSON.stringify(arangoEdge[i])+'\n', function(err) {
                    if(err) {
                        return console.log(err);
                    }
                    //console.log("The file was started...");
                }); 
            }
            
            console.log("The file \""+stamp+"_arango_edge.json\" was saved!");
        }

        function map(data, line){            
            var obj = {}
        
            if(data !== null){
                obj = {"_key":count.toString()};
                obj['namespace+id'] = "";
                //console.log(obj._key);
                count ++;
            }
    
            for(key in data){
                
                if(key == "$$hashKey")
                {
                    //console.log(key);
                }
                
                if(key === "prefix"){
                /*  console.log("------------------")
                    console.log("get prefix!")
                    console.log(data[key])
                    console.log(vocab[data[key]])
                    console.log(data)
                    console.log("------------------")
                    */
                    obj['namespace'] = vocab[data[key]].namespace;
                                    
                    var nameHash = obj.namespace.hashCode().toString();
                    var isThere = false;
                    
                    if(arango_value2.length !== 0){
                        for(key2 in arango_value2){
                            if(key2 == nameHash){
                                isThere = true;
                            }
                        }
                    }
                    
                    if(!isThere && nameHash !== undefined){
                        arango_value2[nameHash] = {"_key": nameHash, "label": obj.namespace};
                        arango_edge.push({"_from": 0, "_to": nameHash});
                        //console.log(arango_value2);
                    }
                    
                    obj['namespace+id'] = vocab[data[key]].namespace;                
                }
                
                if(key === 'constant' || key === "propertyName"){
                    obj['namespace+id'] += data[key];                
                }
                
                if(key === "column" || key === "literalValue"){
                    var test = data[key].value;
                    var field = headings[test];
                    var val = line[field];

                    obj[key] = data[key].value;
                    obj['value'] = line[field];

                    if(!isNaN(obj.value)){
                        obj.value = +obj.value;
                    }   

                }else if(Array.isArray(data[key])){
                    data[key].forEach(function(entry) {
                        var ArrToObj = {"_from": obj._key, "_to":map(entry, line)};
                        if(typeof ArrToObj._to != 'undefined'){
                            arango_edge.push(ArrToObj);
                        }
                    });
                }else if(typeof data[key] === 'object'){
                    var ObjToObj = {"_from": obj._key, "_to":map(data[key], line)};
                    if(typeof ObjToObj._to != 'undefined'){
                        arango_edge.push(ObjToObj);
                    }
                }else{
                    if(key != "$$hashKey"){
                        obj[key] = data[key];
                    }
                }
            }
            
            if(data !== null){
                obj['label'] = "";
                
                if(typeof obj.prefix != 'undefined'){
                    obj.label = obj.label + obj.prefix + " ";
                }
                
                if(typeof obj.value != 'undefined'){
                    obj.label = obj.label + obj.value;
                }
                
                if(typeof obj.propertyName != 'undefined'){
                    obj.label = obj.label + ": " + obj.propertyName;
                }    
                
                //handle prefix mapping of keys here.....
                
                if(obj.__type !== "Property"){
                    
                    if(obj.value !== undefined){
                        obj['namespace+id'] += obj.value.toString();
                        obj['old_key'] = obj._key;
                        obj._key = obj["namespace+id"].hashCode().toString();
                        
                        for(var i = 0; i < arango_edge.length; i++){
                            if(arango_edge[i]._from === obj.old_key){
                                arango_edge[i]._from = obj._key;
                            }else if(arango_edge[i]._to === obj.old_key){
                                arango_edge[i]._to = obj._key;
                            }
                            
                        }
                    }
                    
                    if(obj.namespace !== undefined){
                        arango_edge.push({"_from":obj.namespace.hashCode().toString(), "_to":obj._key});
                    }
                }
                
                //End prefix handling of keys
                var existsInSet = false;
                
                for (key in arango_value){
                    if(arango_value[key]._key === obj._key){
                        existsInSet = true;
                        break;
                    }
                }
                
                if(!existsInSet){
                    arango_value.push(obj);
                }
            }
            
            return obj._key;
        };

        var count = 0;
        var arango_value = [];  //main value array
        var arango_value2 = {}; //array for namepsace nodes
        var arango_edge = [];   //array for all edges

        var createdHeadings = false;
        headings = []; //reset headings
        
        var lineReader = require('readline').createInterface({
            input: fs.createReadStream(path)
        });
        
        var lineCounter = 0;
        lineReader.on('line', function (line) {
            line = pa.parse(line);
    
            console.log("Line: " + ++lineCounter);
    
            if(!createdHeadings){
                createdHeadings = true;
                for (var i = 0; i < line.data[0].length; i++) { //for all elements i colums array from datalib parse
                    var value = line.data[0][i];
                    headings[value] = i; //add an entry with value as key and arrayposition as value
                }
            }else{
                var rootNode = {"_key":count.toString(), "label": "Start node", "value": "Start node"};
                count ++;
                arango_edge.push({"_from": 0, "_to":(count).toString()});
                obKey = map(mapping, line.data[0]);
    
                arango_value.push(rootNode);
            }
        });
    
        lineReader.on('close', function(){
            console.log("Done!");
    
            /*Add all namespaces to node collection*/
            for(key in arango_value2){
                arango_value.push(arango_value2[key]);
            }
    
            //Save as array with objects
            //write_array(arango_value, arango_edge);
        
            //Save one object per line
            write_object(arango_value, arango_edge);

            //rest.listCollections();
            
/*            for(elem in arango_value){
                console.log(JSON.stringify(arango_value[elem]));
                rest.insertDocument(JSON.stringify(arango_value[elem]), "infrarisk");
            }
*/
            //console.log(JSON.stringify(arango_value))
            if(useRest){
                rest.insertDocument(JSON.stringify(arango_value), name, db, endpoint, authToken)
                    .then(console.log("Inserted nodes to collection: " + name))
                    .catch(res => console.log(res));
                rest.insertDocument(JSON.stringify(arango_edge), name+"_edge", db, endpoint, authToken)
                    .then(res => console.log('inserted edges to edge collection: '+name+"_edge"))
                    .catch(res => console.log(res));
            }
            res.send('ok');
        });
    }});
};