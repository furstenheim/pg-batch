var async = require('async');

var getIterators = function(pgBatch, prepareQuery, callback){
    pgBatch.pooledPg.query(prepareQuery, {}, function(err, result){
        if(err){
            return callback(err, {})
        }
        callback(null, result.rows)
    })
}
//There are two parameters, prepareQuery and query
module.exports = function(pgBatch, params, callback) {
    var prepareQuery = params.prepareQuery;
    var query = params.query;
    getIterators(pgBatch, prepareQuery, function(err, result){
        if(err){
            return callback(err);
        }
            console.log(pgBatch.pgConfig.pgPoolSize, result)
        async.eachLimit(result, pgBatch.pgConfig.pgPoolSize, function iterator(item, callback){
            "use strict";
            console.log(item);
            var my_query = query;
            for(var variable in item){
                my_query = my_query.replace(new RegExp(':' + variable,'g'), item[variable])
            }
            pgBatch.runPostgresCommand(my_query, {}, callback);
        })

    }, function(err){
            if(err){
                return callback(err);
            }
            console.log('managed to:',query.substring(0,20))
            callback();
        }



    )

}