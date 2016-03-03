var pg = require('pg');
var pgSpice = require('pg-spice');
pgSpice.patch(pg);
var async = require('async');
var debug = require('debug')('pg-batch:')
var fs = require('fs')
var PGBatch = function(config){
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
}

//Run a simple sql
PGBatch.prototype.runPostgresCommand = function(command, callback) {
    var self = this;
    pg.connect(self.pgConfig, function(err, client, done){
        //TODO Fix this with generic pool
        client.removeAllListeners('info', function(){});
        client.removeAllListeners('notice',function(){})
        client.on('notice',function(text){
            console.log( text)
        });
        client.on('info',function(text){
            console.log(text)
        });
        console.log('Starting to: ',command.substring(0,20))
        client.query(command, function(err, result){
            if(err){
                console.error('error with:', command.substring(0,20),err);
                console.error('error at: ', command.substring(err.position - 10, err.position  ))
                return callback(err);
            }
            console.log('We managed to: ',command.substring(0,20))
            done();
            return callback();
        })
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
    }

}