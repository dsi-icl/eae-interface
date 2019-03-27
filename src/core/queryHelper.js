/**
 * @fn QueryHelper
 * @desc
 * @param config
 * @constructor
 */
function QueryHelper(config = {}) {
    let _this = this;
    _this.config = config;

    // bind private members
    _this._translateCohort = QueryHelper.prototype._translateCohort.bind(this);
    _this._createDerivedQuery = QueryHelper.prototype._createDerivedQuery.bind(this);

    // Bind member functions
    _this.buildPipeline = QueryHelper.prototype.buildPipeline.bind(this);
}

QueryHelper.prototype._createDerivedQuery = function(cohort) {


};


QueryHelper.prototype._translateCohort = function(cohort){
    let match = {};

    cohort.forEach(function (select) {

        switch (select.op){
            case "=":
                // select.value must be an array
                match[select.field] = { $in: select.value };
                break;
            case "!=":
                // select.value must be an array
                match[select.field] = { $ne: select.value };
                break;
            case ">":
                // select.value must be a float
                match[select.field] = { $lt: select.value };
                break;
            case "<":
                // select.value must be a float
                match[select.field] = { $gt: select.value };
                break;
            case "derived":
                // equation must only have + - * /
                let derivedOperation = select.value.split(" ");
                if (derivedOperation[0] === "="){match[select.field] = {$eq: select.value};}
                if (derivedOperation[0] === ">"){match[select.field] = {$gt: select.value};}
                if (derivedOperation[0] === "<"){match[select.field] = {$lt: select.value};}
                break;
            case "exists":
                // We check if the field exists. This is to be used for checking if a patient
                // has an image
                match[select.field] = { $exists: true };
                break;
            case "count":
                // counts can only be positive. NB: > and < are inclusive e.g. < is <=
                let countOperation = select.value.split(" ");
                if (countOperation[0] === "="){match[select.field] = {$eq: select.value};}
                if (countOperation[0] === ">"){match[select.field] = {$gt: select.value};}
                if (countOperation[0] === "<"){match[select.field] = {$lt: select.value};}
                break;
            default:
                break;
        }
    }
    );
    return match
};


/**
 * @fn buildPipeline
 * @desc Methods that builds thee pipeline for the mongo query
 * @param query
 * structure of the request
 * {
    "new_fields": [
        {
        "name": String // Name of the new field
        "value": String // Field in the form X.X for a count otherwise and equation defining the derived feature
        "op": String // Count or derived
        }
    ],
    "cohort": [
        [{
        "field": String, // Field identifier
        "value": String Or Float, // Value requested can either categorical
        "op": String // Logical operation
        },
        {
        "field": String, // Field identifier
        "value": String Or Float, // Value requested can either categorical
        "op": String // Logical operation
        }
     ],
     [
     {
        "field": String, // Field identifier
        "value": String Or Float, // Value requested can either categorical
        "op": String // Logical operation
        }
     ]],
    "data_requested": [ "field1",  "field2", "field3"] // Fields requested
    }
 }
 */
QueryHelper.prototype.buildPipeline = function(query){
    let _this = this;

    // m_eid  = patient_id ?
    let fields = {_id: 0, m_eid: 1};
    // We send back the requested fields
    query.data_requested.forEach(function (field) {
        fields[field] = 1;
    });

    let addFields = {};
    // We send back the newly created derived fields by default
    query.new_fields.forEach(function(field){
        if(field.op === "derived") {
            fields[field.name] = 1;
            addFields[field.name] = _this._createDerivedQuery(value);
        }
        else if(field.op === "count"){
            addFields[field.name] = { $cond: { if: { $exists: field.value }, then: { $size: field.value }, else: "NA"}};
        }else{
            return "Error"
        }
    });


    let match = {};
    if(query.cohort.length > 1){
        let subqueries = [];
        query.cohort.forEach(function (subcohort) {
            addFields.
            subqueries.push(_this._translateCohort(subcohort));
        });
        match={ $or: subqueries };
    }else{
        match=_this._translateCohort(query.cohort);
    }

    return pipeline =[
        {$addFields: addFields},
        {$match: match},
        {$project: fields}
    ];
};

module.exports = QueryHelper;
