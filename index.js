var pg = require('pg');
var genericPool = require('generic-pool');
var PooledPg = require('./PooledPg.js');
var nodeScripts = require('./node_scripts/nodeScripts.js');
var splitCommand = '$NODE_COMMAND$';
var nodeCommand = 'NODE';
var parallelCommand = 'PARALLEL';
var parallelSplit = '$PARALLEL$';
var async = require('async');
var debug = require('debug')('pg-batch:')
var debugPool = require('debug')('pg-batch:pool')
var fs = require('fs')
var PGBatch = function(pgConfig){
    var self = this;
    self.pgConfig = {
        poolIdleTimeout : pgConfig.pgPoolIdleTimeout,
        pgPoolSize : pgConfig.pgPoolSize || 5,
        user : pgConfig.username,
        password : pgConfig.password,
        host : pgConfig.host,
        port : pgConfig.port,
        database : pgConfig.database
    }
    this.pool = genericPool.Pool({
        name : 'postgres',
        create : function (callback) {
            var Client = pg.Client;
            var c = new Client(self.pgConfig);
            c.on('notice',function(text){
                console.log( text)
            });
            c.on('info',function(text){
                console.log(text)
            });
            c.connect();
            callback(null, c);
        },
        destroy : function (client) {client.end(); },
        max : self.pgConfig.pgPoolSize,
        idleTimeoutMillis : self.pgConfig.pgPoolIdleTimeout,
        log : function (text, level) {
            if (level !== 'verbose') {
                debugPool(text, 'PoolSize:', self.pool.getPoolSize());
            }
        }
    });
    this.pooledPg = new PooledPg(this.pool);
}

//Run a simple sql
PGBatch.prototype.runPostgresCommand = function(command, callback) {
    this.pooledPg.query(command, {}, function(err, result){
        if(err){
            console.error('error with:', command.substring(0,20),err);
            console.error('error at: ', command.substring(err.position - 10, err.position  ))
            return callback(err);
        }
        console.log('We managed to: ',command.substring(0,20))
        return callback();

    })
}
PGBatch.prototype.runFile = function(instructions, start, callback) {
    var self = this;
    async.forEachOfSeries(instructions.split(splitCommand).slice(start), function(item,index,callback){
        console.log('Starting ', start + index);
        if(item.substring(0,4) === nodeCommand){
            eval('var myCommand = ' + item.substring(4));
            self.runNodeCommand(myCommand, function(err) { if(err) { return callback(err + 'Error was at' + (start + index))} return callback()});
        } else if (item.substring(0,8) === parallelCommand){
            async.eachLimit(item.substring(8).split(parallelSplit), self.pgConfig.pgPoolSize,function(new_item, callback){
                self.runPostgresCommand(new_item, callback);
            }, function(err){if (err){return callback(err + 'Error was at start' + index )} return callback()} )
        } else {
            self.runPostgresCommand(item,function(err) { if(err) { return callback(err + 'Error was at start' + (start + index))} return callback()});
        }
    },function(err, results){
        if(err){
            console.error(err);
            return callback(err);
        }
        callback();
    });
}
PGBatch.prototype.runNodeCommand = function(instructions, callback) {
    var self = this;
    if(instructions.type === 'upload_csv'){
        var csv = instructions.csv;
        var terminal = require('child_process').spawn('bash');
        terminal.stdout.on('data', function(data){
            console.log('loading csv, sdtdout: ', data.toString('utf-8'));
        });
        terminal.on('exit', function(code){
            console.log('We finished processing with code: ', code, ' the file:  ', csv.name);
            if(code === 1){
                return callback('We finished processing with code: ' + code + ' the file:  ' + csv.name);
            }
            callback();
        });
        terminal.stderr.on('data', function(data){
            //psql prints notices to stderr
            console.log('loading csv, stderr: ', data.toString('utf-8'));
        });
        var csvPath = path.resolve(csvFolder, csv.name + '.csv');
        var firstCommand = 'psql -U ' + self.pgConfig.username + ' -p ' + self.pgConfig.port + ' -h ' + self.pgConfig.host + ' -d ' + self.pgConfig.database + ' -c \'SET CLIENT_ENCODING = "' + csv.encoding + '";\'\n';
        var command = 'psql -U ' + self.pgConfig.username + ' -p ' + self.pgConfig.port +  ' -h ' + self.pgConfig.host + ' -d ' + self.pgConfig.database + ' -c "\\copy ' + csv.table + ' from ' + csvPath + ' DELIMITER \'' + csv.delimiter + '\' ' + (csv.quote !== undefined ? ' QUOTE ' + csv.quote : ' ') + (csv.headers === true ? ' HEADER' : ' ')+ ' CSV"';
        terminal.stdin.write(firstCommand);
        console.log(command);
        terminal.stdin.write(command);
        terminal.stdin.end();
    } else if (instructions.type === 'load_sql'){
        var sql = instructions.sql;
        var sqlCommand = fs.readFileSync(path.resolve(__dirname,sql.relative_path)).toString();
        self.runPostgresCommand(sqlCommand, callback);
    } else if(instructions.type === 'node_script'){
        var node = instructions.node;
        var myFunction = nodeScripts[node.relative_path];
        //Script is not defined
        if(myFunction === undefined){
            return callback('Method not defined', {})
        }
        myFunction(self, node.params, callback)
    } else if(instructions.type === 'restore_backup'){
        var backup = instructions.backup;
        var filename = backup.name + ".backup";
        console.log(filename);
        var terminal = require('child_process').spawn('bash');
        terminal.stdout.on('data', function(data){
            console.log('loading ', filename, data.toString('utf-8'));
        });
        terminal.on('exit', function(code){
            console.log('We finished processing with code: ', code);
            return callback();
            terminal.stdin.end();
        });
        terminal.stderr.on('data', function(data){
            //psql prints notices to stderr
            console.log('loading', filename, data.toString('utf-8'));
        });
        setTimeout(
            function(){
                console.log("Writing code");
                terminal.stdin.write("echo 'lets start the restoring'\n");
                //TODO use template strings
                console.log("pg_restore -U " + self.pgConfig.username + " -d " + self.pgConfig.database + ' -h ' + self.pgConfig.host +  " -p " + self.pgConfig.port + " -v " + path.resolve(csvFolder,filename) + "\n")
                terminal.stdin.write("pg_restore -U " + self.pgConfig.username + " -d " + self.pgConfig.database +  ' -h ' + self.pgConfig.host + " -p " + self.pgConfig.port + " -v " + path.resolve(csvFolder,filename) + "\n");
                terminal.stdin.end()
            },100)
    } else if(instructions.type === 'close_connections'){
        //TODO implement this
        return callback();
    } else if(instructions.type === 'skip'){
        console.log('command skipped: ', command);
        return callback();
    }   else{
        console.error('Unkown command', instructions);
        return callback();
    }

}

module.exports = PGBatch;