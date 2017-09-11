/* load the rdf mapping and vocab into the script */
.load rdfMapping.js
.load rdfVocab.js

const pa = require('./papaparse.min.js');
const dl = require('datalib');
const fs = require('fs');

/* Containers for transformation data */
var vocab = [];
var headings = {};
var buffer;

var count = 0;
var arango_value_done = [];  //main value array
var arango_value2 = {}; //array for namepsace nodes
var arango_edge_done = [];   //array for all edges

var arango_value = [];  //main value array
var arango_edge = [];   //array for all edges

/*Make vocabs for dataset*/
for(var i = 0; i < data_vocab.length; i++){
    if(i == 0){vocab = [];}
    var key = data_vocab[i].name;
    vocab[key] = data_vocab[i];
}

/*Read function of the csv file*/
function read(input) {

    var createdHeadings = false;
    headings = []; //reset headings
    
    var lineReader = require('readline').createInterface({
        input: fs.createReadStream(input)
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
            obKey = map(data, line.data[0]);

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
    })
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
}


/*Build array for headings, to lookup colum positions*/
function build() {
    headings = []; //reset headings
    for (var i = 0; i < buffer.columns.length; i++) { //for all elements i first buffer row
        var value = buffer.columns[i];
        headings[value] = i; //add an entry with value as key and arrayposition as value
    }
};

function map(data, line){            
    var obj = {}

    if(data !== null){
        obj = {"_key":count.toString()};
        obj['namespace+id'] = "";
        //console.log(obj._key);
        count ++;
    }
    
    for(key in data){
        
        /*if(key == "$$hashKey")
        {
            console.log(key);
        }*/
        
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
    
function sort(){
    /*Sort the info so it's easier to read*/
   arango_value.sort(function(a, b) {
        return a._key - b._key;
    });
    
    arango_edge.sort(function(a, b) {
        return a._from - b._from;
    });
};

function write_array(arangoValue, arangoEdge){
    console.log("array!");
    var stamp = new Date().toISOString().replace('T', ' ').replace('.', '')
    
    fs.writeFile("./results/" + stamp + "_arango_value.json", JSON.stringify(arangoValue), function(err) {
        if(err) {
            return console.log(err);
        }
        console.log("The file was saved!");
    });
    
    fs.writeFile("./results/" + stamp + "_arango_edge.json", JSON.stringify(arangoEdge), function(err) {
        if(err) {
            return console.log(err);
        }
        console.log("The file was saved!");
    });
}

function write_object(arangoValue, arangoEdge){
    console.log("object!");
    var stamp = new Date().toISOString().replace('T', ' ').replace('.', '');
    
    //write nodes
    console.log("writing node values to file...");
    for(var i = 0; i < arangoValue.length; i++){
        fs.appendFileSync("./results/" + stamp + "_arango_value.json", JSON.stringify(arangoValue[i]) + '\n', function(err) {
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
            fs.appendFileSync("./results/" + stamp + "_arango_edge.json", JSON.stringify(arangoEdge[i])+'\n', function(err) {
                if(err) {
                    return console.log(err);
                }
                //console.log("The file was started...");
            }); 
    }
    
    console.log("The file \""+stamp+"_arango_edge.json\" was saved!");
};

read("download2.csv")