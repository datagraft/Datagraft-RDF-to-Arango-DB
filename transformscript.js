/* jshint esversion: 6 */
const pa = require('./papaparse.min.js');
const dl = require('datalib');
const fs = require('fs');

/* Containers for transformation data */
var vocab = [],
    headings = {},
    buffer,
    count = 0,
    arango_value2 = {},
    args = require('minimist')(process.argv.slice(2)),
    transformation_vocabs = [],
    graph_mapping = {},
    lineCounter = 0;

/* Hash function - used to hash namespaces for keys */
String.prototype.hashCode = function () {
  var hash = 0;
  if (this.length === 0) return hash;
  for (i = 0; i < this.length; i++) {
    char = this.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32bit integer
  }
  return (hash + 2147483647) + 1; 
};

/* Read function of the csv file */


function mapAsync(data, line, arango_value, arango_edge){            
  var obj = {};

  if(data !== null){
    obj = {"_key":count.toString()};
    obj['namespace+id'] = "";
    //console.log(obj._key);
    count ++;
  }

  for(key in data){
    if(key === "prefix"){
      obj.namespace = vocab[data[key]].namespace;

      var nameHash = obj.namespace.hashCode().toString();
      var isThere = false;

      if(arango_value2.length !== 0){
        for(var key2 in arango_value2){
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
      obj['value'] = data[key];                
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
        var ArrToObj = {"_from": obj._key, "_to":mapAsync(entry, line, arango_value, arango_edge)};
        if(typeof ArrToObj._to != 'undefined'){
          arango_edge.push(ArrToObj);
        }
      });
    }else if(typeof data[key] === 'object'){
      var ObjToObj = {"_from": obj._key, "_to":mapAsync(data[key], line, arango_value, arango_edge)};
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
    obj.label = "";

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
        obj.old_key = obj._key;
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
}

// output file name without the file extension
var stamp = (args.file || args.f).substring(0, (args.file || args.f).lastIndexOf('.'));

var arangoValuesFilePath = "./results/" + stamp + "_arango_value.json";
var arangoEdgeFilePath = "./results/" + stamp + "_arango_edge.json";

// Open write streams to the files for the value and edge collections
var wsValues = fs.createWriteStream(arangoValuesFilePath);
var wsEdges = fs.createWriteStream(arangoEdgeFilePath);


// empty the output files if they exist
if(fs.existsSync(arangoValuesFilePath)){
  fs.truncateSync(arangoValuesFilePath, 0);
}

if(fs.existsSync(arangoEdgeFilePath)){
  fs.truncateSync(arangoEdgeFilePath, 0);
}

// Initialise files
fs.appendFileSync(arangoValuesFilePath, '');
// Edge collection must be an array so we add the opening bracket before we start adding the individual edges
fs.appendFileSync(arangoEdgeFilePath, '');



function read(input) {
  var createdHeadings = false;
  // reset headings
  headings = []; 

  var lineReader = require('readline').createInterface({
    terminal: false,
    input: fs.createReadStream(input)
  });

  lineReader.on('line', function (line) {
    //console.log(arango_value2);    
    var arango_value = [],
        arango_edge = [];
    var lineBak = line;
    line = pa.parse(line);
    ++lineCounter;
    //    console.log("Line: " + lineCounter);

    if(!createdHeadings) {
      createdHeadings = true;
      for (var i = 0; i < line.data[0].length; i++) { //for all elements i colums array from datalib parse
        var value = line.data[0][i];
        headings[value] = i; //add an entry with value as key and arrayposition as value
      }
    } else {
      var rootNode = {"_key":count.toString(), "label": "Start node", "value": "Start node"};
      count ++;

      if(!line.data[0]){
        // Apparently sometimes there are random empty lines in the input files...
        console.log("Empty or corrupted line found - ignoring ...");
        console.log(line);
        console.log("Line text: ");
        console.log(lineBak);
      } else {
        arango_edge.push({"_from": 0, "_to":(count).toString()});
        obKey = mapAsync(graph_mapping, line.data[0], arango_value, arango_edge);
        arango_value.push(rootNode);
        write_object(arango_value, arango_edge);
      }
    }
  });

  lineReader.on('close', function(){
    console.log("Done!");
    console.timeEnd("transformData");
    console.log("Lines: " + lineCounter);

    // Edge collection must be an array so we add the closing bracket after we are done reading input
    //wsEdges.write(']');

    write_object(arango_value2, []);

    // Close streams
    wsValues.end();
    wsEdges.end();
    //    fs.appendFileSync("./results/" + stamp + "_arango_edge.json", '{}]');
  });
}

function write_object(arangoValue, arangoEdge){
  // Write nodes
  for(var i = 0; i < arangoValue.length; i++){
    wsValues.write(JSON.stringify(arangoValue[i]) + '\n');
    //    fs.appendFileSync("./results/" + stamp + "_arango_value.json", JSON.stringify(arangoValue[i]) + '\n'); 
  }

  // Write edges
  for(i = 0; i < arangoEdge.length; i++){
    wsEdges.write(JSON.stringify(arangoEdge[i]) + '\n');
    //    fs.appendFileSync("./results/" + stamp + "_arango_edge.json", JSON.stringify(arangoEdge[i]) + ',\n'); 
  }
}

try {
  if(args.transformation || args.t) {
    var transformation = JSON.parse(fs.readFileSync(args.transformation || args.t));
    transformation_vocabs = transformation.extra.rdfVocabs;
    graph_mapping = transformation.extra.graphs[0];
    /* Make vocabs for dataset */
    for(var i = 0; i < transformation_vocabs.length; i++) {
      if(i === 0){ vocab = []; }
      var key = transformation_vocabs[i].name;
      vocab[key] = transformation_vocabs[i];
    }
  } else {
    throw ("Transformation argument not found.")
  }
} catch(e) {
  console.log("Error reading transformation!");
  console.log(e);
}

try {
  if(args.file || args.f) {
    console.time("transformData");
    read(args.file || args.f);
  } else {
    throw("Input file argument not found!");
  }
} catch (e) {
  console.log("Error reading input file!");
  console.log(e);
}
