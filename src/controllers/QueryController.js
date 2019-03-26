/**
 * @fn QueryController
 * @desc Controller to manage the mongo Queries
 * @param queryCollection mongo collection with the queries
 * @param dataCollection mongo collection with the patient data
 * @param queryHelper Helper to help build the mongo pipelines
 * @constructor
 */
function QueryController(queryCollection,dataCollection,queryHelper) {
    let _this = this;
    _this._queryCollection = queryCollection;
    _this._dataCollection = dataCollection;
    _this._queryHelper = queryHelper;

    // Bind member functions
    this.processQuery = QueryController.prototype.processQuery.bind(this);
}

/**
 * @fn getStatus
 * @desc HTTP method GET handler on this service status
 * @param req Incoming message
 * @param res Server Response
 */
QueryController.prototype.processQuery = function(req, res) {
    let _this = this;
    let queryId = req.body.query_id;
    try {
        _this._queryCollection.findOne({id : queryId}).then(function(query){
            let pipeline = _this._queryHelper.buildPipeline(query);
            // let match = {'31.0.0': { $in: ['Male'] }};
            // let fields = {_id: 0, m_eid: 1, '21022.0.0':1};
            // let pipeline = [
            //     {$match: match},
            //     {$project: fields}
            // ];

            if(pipeline === "Error"){
                res.status(500);
                res.json('Error while building the pipeline: ' + query );
            }

            _this._dataCollection.aggregate(pipeline).toArray().then(function (results) {
                res.status(200);
                res.json(results);
            },function(error){
                res.status(500);
                res.json('Mongo Query Error', error);
            });
        },function(error){
            res.status(401);
            res.json('Error while processing the query ' + queryId, error);
        });
    }
    catch (error) {
        res.status(510);
        res.json('Error occurred', error);
    }
};

module.exports = QueryController;

