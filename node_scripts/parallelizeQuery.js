var async = require('async');

var getIterators = function(pgBatch, prepareQuery, callback){
    pgBatch.pooledPg.query(prepareQuery, function(err, result){
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
    getIterators(pgBatch, function(err, result){
        if(err){
            return callback(err);
        }
        async.eachLimit(result, pgBatch.config.pgPoolSize, function iterator(item, callback){
            "use strict";
            console.log(item);
            var my_query = query;
            for(let variable in item){
                my_query = my_query.replace(new RegExp(':' + variable,'g'), item[variable])
            }
            runPostgresCommand(my_query, callback);
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