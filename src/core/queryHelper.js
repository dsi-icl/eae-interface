/**
 * @fn QueryHelper
 * @desc Helper to manipulate and query the MongoDB database storing UK biobank data formatted in the
 * white paper format
 * @param config
 * @constructor
 */
function QueryHelper(config = {}) {
    let _this = this;
    _this.config = config;

    // bind private members
    _this._translateCohort = QueryHelper.prototype._translateCohort.bind(this);
    _this._createNewField = QueryHelper.prototype._createNewField.bind(this);
    _this._isEmptyObject = QueryHelper.prototype._isEmptyObject.bind(this);

    // Bind member functions
    _this.buildPipeline = QueryHelper.prototype.buildPipeline.bind(this);
}


/**
 * @fn _createNewdField
 * @desc Creates the new fields required to compare when using an expresion like BMI or an average
 * expression = {
 *   "left": json for nested or string for field id or Float,
 *   "right": json for nested or string for field id or Float,
 *   "op": String // Logical operation
 * @param expression
 * @return json formated in the mongo format in the pipeline stage addfield
 * @private
 */
QueryHelper.prototype._createNewField = function(expression) {
    let _this = this;
    let newField = {};

    switch (expression.op) {
        case '*':
            newField = {"$multiply": [_this._createNewField(expression.left), _this._createNewField(expression.right)]};
            break;
        case '/':
            newField = {"$divide": [_this._createNewField(expression.left), _this._createNewField(expression.right)]};
            break;
        case '-':
            newField = {"$subtract": [_this._createNewField(expression.left), _this._createNewField(expression.right)]};
            break;
        case '+':
            newField = {"$add": [_this._createNewField(expression.left), _this._createNewField(expression.right)]};
            break;
        case '^':
            //NB the right side my be an integer while the left must be a field !
            newField = {"$pow": [ '$' + expression.left,  parseInt(expression.right)]};
            break;
        case 'val':
            newField = parseFloat(expression.left);
            break;
        case 'field':
            newField =  '$' + expression.left;
            break;
        default:
            break;
    }

    return newField;
};

/**
 * @fn _isEmptyObject
 * @desc tests if an object is empty
 * @param obj
 * @returns {boolean}
 * @private
 */
QueryHelper.prototype._isEmptyObject = function(obj){
    return !Object.keys(obj).length;
};


/**
 * @fn _translateCohort
 * @desc Tranforms a query into a mongo query.
 * @param cohort
 * @private
 */
QueryHelper.prototype._translateCohort = function(cohort){
    let match = {};

    cohort.forEach(function (select) {

        switch (select.op){
            case '=':
                // select.value must be an array
                match[select.field] = { $in: [select.value] };
                break;
            case '!=':
                // select.value must be an array
                match[select.field] = { $ne: [select.value] };
                break;
            case '>':
                // select.value must be a float
                match[select.field] = { $lt: parseFloat(select.value) };
                break;
            case '<':
                // select.value must be a float
                match[select.field] = { $gt: parseFloat(select.value) };
                break;
            case 'derived':
                // equation must only have + - * /
                let derivedOperation = select.value.split(' ');
                if (derivedOperation[0] === '='){match[select.field] = {$eq: parseFloat(select.value)};}
                if (derivedOperation[0] === '>'){match[select.field] = {$gt: parseFloat(select.value)};}
                if (derivedOperation[0] === '<'){match[select.field] = {$lt: parseFloat(select.value)};}
                break;
            case 'exists':
                // We check if the field exists. This is to be used for checking if a patient
                // has an image
                match[select.field] = { $exists: true };
                break;
            case 'count':
                // counts can only be positive. NB: > and < are inclusive e.g. < is <=
                let countOperation = select.value.split(' ');
                let countfield = select.field + '.count';
                if (countOperation[0] === '='){match[countfield] = {$eq: parseInt(countOperation[1])};}
                if (countOperation[0] === '>'){match[countfield] = {$gt:  parseInt(countOperation[1])};}
                if (countOperation[0] === '<'){match[countfield] = {$lt:  parseInt(countOperation[1])};}
                break;
            default:
                break;
        }
    }
    );
    return match;
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
        "value": String // equation defining the derived feature
        "op": String // Derived only
        }
    ],
    "cohort": [
        [{
        "field": String, // Field identifier or field in the form X.X for a count
        "value": String Or Float, // Value requested can either categorical
        "op": String // Logical operation
        },
        {
        "field": String, // Field identifier or field in the form X.X for a count
        "value": String Or Float, // Value requested can either categorical
        "op": String // Logical operation
        }
     ],
     [
     {
        "field": String, // Field identifier or field in the form X.X for a count
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
    if (query.new_fields.length > 0) {
        query.new_fields.forEach(function (field) {
            if (field.op === 'derived') {
                fields[field.name] = 1;
                addFields[field.name] = _this._createNewField(field.value);
            } else {
                return 'Error';
            }
        });
    }

    let match = {};
    if(query.cohort.length > 1){
        let subqueries = [];
        query.cohort.forEach(function (subcohort) {
            // addFields.
            subqueries.push(_this._translateCohort(subcohort));
        });
        match = { $or: subqueries };
    }else{
        match = _this._translateCohort(query.cohort[0]);
    }

    if(_this._isEmptyObject(addFields)){
        return [
            {$match: match},
            {$project: fields}
        ];
    }else{
        return [
            {$addFields: addFields},
            {$match: match},
            {$project: fields}
        ];
    }
};

module.exports = QueryHelper;
