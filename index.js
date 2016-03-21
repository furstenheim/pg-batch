var pg = require('pg');
var pgSpice = require('pg-spice');
var genericPool = require('generic-pool');
var PooledPg = require('./PooledPg.js');
var nodeScripts = require('./node_scripts/nodeScripts.js')
pgSpice.patch(pg);
var async = require('async');
var debug = require('debug')('pg-batch:')
var fs = require('fs')
var PGBatch = function(config){
    var self = this;
    this.config = config;
    var pgConfig = {
        poolIdleTimeout : config.postgres.pgPoolIdleTimeout,
        poolSize : config.postgres.pgPoolSize,
        user : config.postgres.username,
        password : config.postgres.password,
        host : config.postgres.host,
        port : config.postgres.port,
        database : config.postgres.database
    }
    this.pool = genericPool.Pool({
        name : 'postgres',
        create : function (callback) {
            var Client = pg.Client;
            var c = new Client(self.config);
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
        max : config.postgres.pgPoolSize,
        idleTimeoutMillis : config.postgres.pgPoolIdleTimeout,
        log : function (text, level) {
            if (level !== 'verbose') {
                debugPool(text, 'PoolSize:', postgresPool.getPoolSize());
            }
        }
    });
    this.pooledPg = new PooledPg(this.pool);
}

//Run a simple sql
PGBatch.prototype.runPostgresCommand = function(command, callback) {
    var self = this;
    this.pooledPg.query(command, function(err, result){
        if(err){
            console.error('error with:', command.substring(0,20),err);
            console.error('error at: ', command.substring(err.position - 10, err.position  ))
            return callback(err);
        }
        console.log('We managed to: ',command.substring(0,20))
        return callback();

    })
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
        var firstCommand = 'psql -U ' + self.pgConfig.postgres.username + ' -p ' + self.pgConfig.postgres.port + ' -h ' + self.pgConfig.postgres.host + ' -d ' + self.pgConfig.postgres.database + ' -c \'SET CLIENT_ENCODING = "' + csv.encoding + '";\'\n';
        var command = 'psql -U ' + self.pgConfig.postgres.username + ' -p ' + self.pgConfig.postgres.port +  ' -h ' + self.pgConfig.postgres.host + ' -d ' + self.pgConfig.postgres.database + ' -c "\\copy ' + csv.table + ' from ' + csvPath + ' DELIMITER \'' + csv.delimiter + '\' ' + (csv.quote !== undefined ? ' QUOTE ' + csv.quote : ' ') + (csv.headers === true ? ' HEADER' : ' ')+ ' CSV"';
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
        var myFunction = nodeScripts[relative_path];
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
                console.log("pg_restore -U " + config.postgres.username + " -d " + config.postgres.database + ' -h ' + config.postgres.host +  " -p " + config.postgres.port + " -v " + path.resolve(csvFolder,filename) + "\n")
                terminal.stdin.write("pg_restore -U " + config.postgres.username + " -d " + config.postgres.database +  ' -h ' + config.postgres.host + " -p " + config.postgres.port + " -v " + path.resolve(csvFolder,filename) + "\n");
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