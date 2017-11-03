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
    
        const csv = req.files.csv[0];
        const path = req.files.csv[0].path;

        //Params for REST calls
        const useRest = req.body.REST;
        const endpoint = req.body.endpoint;
        const db = req.body.db;
        const name = req.body.name;
        const split = req.body.split;
        const authToken = req.body.authToken;
        
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

            rest.listCollections(name,db, endpoint, authToken)
                .then(result => {
                    console.log(result)
                    run();
                })
                .catch(reason => {
                    //We didn't have the collection try to create them
                    console.log(reason);
                    //create the collection
                    rest.createCollection(name,1 , db, endpoint, authToken)
                        .then(result => {
                            //console.log(result)
                            run();})
                        .catch(result => showAndLogError(res, 500, 'could not create collection'));    
                });
        }else{
            run();
        }
        
        //writer function. Writes on arango object per line, and
        //outputs the file to the sub directory "results"
        function write_object(arangoValue){
            //var stamp = new Date().toISOString().replace('T', ' ').replace('.', '')
            var stamp = Date.now();
            //write nodes
            console.log("writing node values to file...");
            for(var i = 0; i < arangoValue.length; i++){
                fs.appendFileSync("result/" + stamp + "_arango_value.json", JSON.stringify(arangoValue[i]) + '\n', function(err) {
                    if(err) {
                        return console.log(err);
                    }
                    //console.log("The file was started...");
                }); 
            }
            
            console.log("The file \""+stamp+"_arango_value.json\" was saved!");
        }

        function run(){
            var headings =[];
            var arango_value = [];  //main value array
            var createdHeadings = false;
            
            //Start new line reader
            var lineReader = require('readline').createInterface({
                input: fs.createReadStream(path)
            });
            
            //On each line map corresponding heading to value
            //in a new JSON object
            lineReader.on('line', function (line) {
                line = pa.parse(line, {
                    dynamicTyping: true
                });
        
                if(!createdHeadings){
                    createdHeadings = true;
                    headings = line.data[0];
                }else{
                    var newObj = {};
                    for(var i = 0; i < line.data[0].length; i++){
                        newObj[headings[i]] = line.data[0][i];
                    }
                    arango_value.push(newObj);
                }
            });
        
            //When the file is closed (we have read the last line)
            //Write to file or insert using REST API
            lineReader.on('close', function(){
                /*  If the option to use REST to insert is specified
                *   Insert to arango DB using the provided arango information
                *   Else write the result to file.
                */
                if(useRest){
                    rest.insertDocument(JSON.stringify(arango_value), name, db, endpoint, authToken)
                    .then(console.log("Inserted nodes to collection: " + name))
                    .catch(res => console.log(res));
                }else{
                    write_object(arango_value);
                }
                res.send('ok');
            });
    }});
};