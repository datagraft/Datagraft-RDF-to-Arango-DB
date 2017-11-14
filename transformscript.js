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
    lineCounter = 0,
    constants ={};


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
    obj['rdf'] = "";
    count ++;
  }

  //for all elements inside the dataobject
  for(key in data){
    if(key === "prefix"){
      obj.namespace = vocab[data[key]].namespace;

      var nameHash = obj.namespace.hashCode().toString();
      var setContainsPrefix = false;

      if(arango_value2.length !== 0){
        for(var key2 in arango_value2){
          if(key2 === nameHash){
            setContainsPrefix = true;
          }
        }
      }

      if(!setContainsPrefix && nameHash !== undefined){
        arango_value2[nameHash] = {"_key": nameHash, "rdf": obj.namespace};
        arango_edge.push({"_from": 0, "_to": nameHash});
      }

      obj['rdf'] = vocab[data[key]].namespace;              
    }

    if(key === 'constant' || key === "propertyName"){
      obj['rdf'] += data[key];
      obj['value'] = data[key];                
    }
    
    if(key === "column" || key === "literalValue"){
      var field = headings[data[key].value];
      //obj[key] = data[key].value; //This is the line for adding column and litteralValue to the node
      obj['value'] = line[field];
      
      !isNaN(obj.value) ? obj.value = +obj.value : '';
      
      obj.value !== undefined ? obj.rdf += obj.value.toString():'';

    }else if(Array.isArray(data[key])){
      data[key].forEach(function(entry) {
        //Ignore Condition nodes
        if(entry.__type !== "Condition"){
          var ArrToObj = {"_from": obj._key, "_to":mapAsync(entry, line, arango_value, arango_edge)};
          if(typeof ArrToObj._to != 'undefined'){
            arango_edge.push(ArrToObj);
          }
        }
      });
    }else if(typeof data[key] === 'object'){
      var ObjToObj = {"_from": obj._key, "_to":mapAsync(data[key], line, arango_value, arango_edge)};
      if(typeof ObjToObj._to != 'undefined'){
        arango_edge.push(ObjToObj);
      }
    }else{
      /*
        Things / keys to ignore and not include in the finished transformation.
      */
      if(key != "$$hashKey" && key != "constant" && key != "propertyName" && key != "langTag" && key != "datatypeURI"){
        obj[key] = data[key];
      }
    }
  } 

  if(data !== null){
    if(obj.__type !== "Property"){
      if(obj.value !== undefined){
        //keep the old key so we can update it
        var old_key = obj._key;
    
        //make a new key based on the hash of the RDF URI
        obj._key = obj['rdf'].hashCode().toString();

        // For all elements reffering to the old key, swap with the new key
        for(var i = 0; i < arango_edge.length; i++){
          if(arango_edge[i]._from === old_key){
            arango_edge[i]._from = obj._key;
          }else if(arango_edge[i]._to === old_key){
            arango_edge[i]._to = obj._key;
          }

        }
      }

      //if we have the namespace, add a connection from the node to the namespace itself
      obj.namespace !== undefined ? arango_edge.push({"_from":obj.namespace.hashCode().toString(), "_to":obj._key}) : '';
    }
    
    //Check if this node already exists
    var existsInSet = false;
    for (key in arango_value){
      if(arango_value[key]._key === obj._key){
        existsInSet = true;
        break;
      }
    }
    
    //Keep track of constants so that we don't add them more often than we need.
    if(obj.__type === 'ConstantURI' || obj.__type === 'ConstantLiteral'){
      if(constants[obj._key] !== undefined){
        existsInSet = true;
      }else{
        constants[obj._key] = obj;
      }
    }

    //If it doesen't exist add it to the set.
    !existsInSet ? arango_value.push(obj) : '';
  }
  
  //Remove values we don't want to have
  delete obj.__type;
  delete obj.namespace;
  
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

  console.log("starting transformation");

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
    console.log(arango_value2);
    fs.appendFileSync(arangoValuesFilePath, arango_value2);    
    console.log("Done!");
    console.timeEnd("transformData");
    console.log("Lines: " + lineCounter);
    // Edge collection must be an array so we add the closing bracket after we are done reading input
    //wsEdges.write(']');

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
