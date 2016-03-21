//TODO move this to separate library
/**pooledPg is a pg client that is fetched each time it is required and returned to the pool right after, thus reducing the total amount of time out of the pool and all the problems associated with forgetting returning a client**/

var PooledPg = module.exports = function (pgPool) {
    "use strict";
    this.pgPool = pgPool;
};

PooledPg.prototype.query = function (q, queryParams, callback) {
    "use strict";
    var pool = this.pgPool;
    pool.acquire(function (err, client) {
        if (err) {
            console.error('error fetching postgres client from pool', err)
            return callback(err, null);
        }
        client.query(q, queryParams, function (err, reply) {
            pool.release(client);
            callback(err, reply);
        });
    });
}
