module.exports = function () {

    this.query = function (queryCollection, query, fields, options) {
        //joinStack holds all the join args to be used, is used like a callstack when exec is called
        var joinStack = [],
            finalCallback,//This is the final function specified as the callback to call
            cursor = queryCollection.find(query, fields, options),
            that = this;

        /**
         * Begin setting up the join operation
         *
         * @param args.getKey Function get the key for the resulting set
         * @param args.joinCollection MongoDB.Collection the collection to join on
         * @param args.joinKey String the keyname to join on
         * @param args.getJoinKey Function get the key to join on ??
         * @param args.newKey String The name of the property to map the joined document into
         * @param args.fields Object The fields to grab from the query
         * @param args.callback Function The function that will be called with the error message and results set at each
         * join level
         */
        this.join = function (args) {
            joinStack.push(args);
            return that;
        };

        /**
         * Start the join operations, don't stop til we get there
         * @param {Function} [callback] Optional callback function which will be called with the final result set
         * Same as supplying a callback on the final joint argument.
         */
        this.exec = function (callback) {
            finalCallback = callback;
            cursor.toArray(function (err, results) {
                arrayJoin(results, joinStack.shift());
            });
        };

        function addPlainValueToHashBin (bin, value, index) {
            if (Array.isArray(bin[value])) {
                bin[value].push(index);

            } else {
                bin[value] = [index];

            }
        }

        function putKeyInBin (bin, value) {
            if (isNullOrUndefined(bin[value])) {
                bin = bin[value] = {$val: value};
            } else {
                bin = bin[value];
            }

            return bin;//where we are in the hash tree
        }

        function pushOrPut (currentBin, index) {
            var temp = currentBin;
            if (Array.isArray(currentBin)) {
                if (currentBin.indexOf(index) === -1) {
                    currentBin.push(index);//only add if this is a unique index to prevent dup'ing indexes, and associations
                }
            } else {
                temp = [index];
            }
            return temp;
        }

        function buildOutHashTableIndices (bin, leftValue, index, accessors, rightKeys, inLookup, orLookups, currentOr) {
            var i,
                length = accessors.length,
                lastBin;

            for (i = 0; i < length; i += 1) {
                //if accessor [ leftValue ] is an array call this on each of those values
                var val = accessors[i](leftValue);
                if (typeof val === "undefined") {
                    return;
                }

                if (i + 1 === length) {
                    if (Array.isArray(val)) {
                        //for each value in val, put that key in
                        val.forEach(function (subValue) {
                            var boot = putKeyInBin(bin, subValue);
                            bin[subValue] = pushOrPut(boot, index);
                            bin[subValue].$val = subValue;
                        });

                    } else {
                        bin[val] = pushOrPut(bin[val], index);
                        bin[val].$val = val;
                    }
                } else if (Array.isArray(val)) {
                    lastBin = bin;
                    val.forEach(function (subDocumentValue) {//sub vals are for basically supposed to be payment ids
                        var rightKeySubset = rightKeys.slice(i + 1);

                        if (isNullOrUndefined(bin[subDocumentValue])) {
                            bin[subDocumentValue] = {$val: subDocumentValue};
                        }

                        try {
                            buildOutHashTableIndices(bin[subDocumentValue], leftValue, index, accessors.slice(i + 1), rightKeySubset, inLookup, orLookups, currentOr);

                        } catch (e) {debugger}
                    });
                    return;//don't go through the rest of the accessors, this recursion will take care of those
                } else {
                    bin = putKeyInBin(bin, val);
                }
            }
        }


        function buildQueriesFromHashBin (keyHashBin, rightKeys, level, valuePath, orQueries, inQueries) {
            try{
                var keys = Object.getOwnPropertyNames(keyHashBin),
                    or;
            } catch (e) {debugger}

            valuePath = valuePath || [];

            if (level === rightKeys.length) {
                or = {};
                rightKeys.forEach(function (key, i) {
                    inQueries[i].push(valuePath[i]);
                    or[key] = valuePath[i];
                });

                orQueries.push(or);
                //start returning
                //take the value path and begin making objects out of it
            } else {
                keys.forEach(function (key) {
                    if (key !== "$val") {
                        var newPath = valuePath.slice(),
                            value = keyHashBin[key].$val;

                        newPath.push(value);//now have a copied array
                        buildQueriesFromHashBin(keyHashBin[key], rightKeys, level + 1, newPath, orQueries, inQueries);
                    }
                });
            }
        }

        /**
         * Begin the joining process by compiling some data and performing a query for the objects to be joined.
         * @param results The results of the previous join or query
         * @param args The user supplied arguments which will configure this join
         */
        function arrayJoin (results, args) {
            var srcDataArray = results,//use these results as the source of the join
                joinCollection = args.joinCollection,//This is the mongoDB.Collection to use to join on
                rightKeyPropertyPaths = args.rightKeyPropertyPaths,//Get the value of the key being joined upon
                newKey = args.newKey,//The new field onto which the joined document will be mapped
                fields = args.fields,//The fields to retrieve for the join queries, must include the join key
                callback = args.callback,//The callback to call at this level of the join

                findArgs,
                length,
                i,

                subqueries,
                keyHashBin = {},
                accessors = [],
                joinLookups = [],
                inQueries = [],
                leftKeys = args.leftKeys,
                rightKeys = args.rightKeyPropertyPaths;//place to put incoming join results

            rightKeys.forEach(function () {
                inQueries.push([]);
            });

            console.time("Build accessors");
            leftKeys.forEach(function (key) {//generate the accessors for each entry in the composite key
                accessors.push(getKeyValueAccessorFromKey(key));
            });
            console.timeEnd("Build accessors");

            length = results.length;

            //get the path first
            console.time("Build hashmap");
            for (i = 0; i < length; i += 1) {
                buildOutHashTableIndices(keyHashBin, results[i], i, accessors, rightKeys, inQueries, joinLookups, {});
            }//create the path

            buildQueriesFromHashBin(keyHashBin, rightKeys, 0, [], joinLookups, inQueries);

            console.timeEnd("Build hashmap");
            if (!Array.isArray(srcDataArray)) {
                srcDataArray = [srcDataArray];
            }

            subqueries = getSubqueries(inQueries, joinLookups, args.pageSize || 5, rightKeys);//example
            console.time("join query");
            runSubqueries(subqueries, function (items) {
                var un;
                console.timeEnd("join query");
                console.time("performJoining");
                performJoining(srcDataArray, items, {
                    rightKeyPropertyPaths: rightKeyPropertyPaths,
                    newKey: newKey,
                    keyHashBin: keyHashBin
                });
                console.timeEnd("performJoining");

                if (joinStack.length > 0) {
                    arrayJoin(srcDataArray, joinStack.shift());
                } else {
                    callIfFunction(finalCallback, [un, srcDataArray]);
                }
                callIfFunction(callback, [un, srcDataArray]);
            }, joinCollection, []);

        }

        return this;
    };

    function getSubqueries (inQueries, orQueries, pageSize, rightKeys) {
        var subqueries = [],
            numberOfChunks,
            i,
            inQuery,
            orQuery,
            from,
            to;
        //                                                   this is a stupid way to turn numbers into 1
        numberOfChunks = (orQueries.length / pageSize) + (!!(orQueries.length % pageSize));

        for (i = 0; i < numberOfChunks; i += 1) {
            inQuery = {};
            from = i * pageSize;
            to = from + pageSize;

            rightKeys.forEach(function (key, index) {
                inQuery[rightKeys[index]] = {$in: inQueries[index].slice(from, to)};
            });

            orQuery = { $or: orQueries.slice(from, to)};

            subqueries.push([
                {
                    $match: inQuery
                },
                {
                    $match: orQuery
                }]);
        }

        return subqueries;
    }

    function runSubqueries (subQueries, callback, collection, findArgs) {
        var i,
            responsesReceived = 0,
            length = subQueries.length,
            joinedSet = [];//The array where the results are going to get stuffed

        if (subQueries.length > 0) {
            for (i = 0; i < subQueries.length; i += 1) {

                collection.aggregate(subQueries[i], function (err, results) {
                    joinedSet = joinedSet.concat(results);
                    responsesReceived += 1;

                    if (responsesReceived === length) {
                        callback(joinedSet);
                    }
                });
            }
        } else {
            callback([]);
        }

        return joinedSet;
    }

    /**
     * Use the lookup value type to build an accessor function for each join key. The lookup algorithm respects dot
     * notation. Currently supports strings and functions.
     * @param lookupValue The key being used to lookup the value.
     * @returns {Function} used to lookup value from an object
     */
    function getKeyValueAccessorFromKey (lookupValue) {
        var accessorFunction;
        if (typeof lookupValue === "string") {
            accessorFunction = function (resultValue) {
                var args = [resultValue];

                args = args.concat(lookupValue.split("."));

                return safeObjectAccess.apply(this, args);
            };
        } else if (typeof lookupValue === "function") {
            accessorFunction = lookupValue;
        }

        return accessorFunction;
    }

    /**
     * Join the join set with the original query results at the new key.
     * @param sourceData The original result set
     * @param joinSet The results returned from the join query
     * @param joinArgs The arguments used to join the source to the join set
     */
    function performJoining (sourceData, joinSet, joinArgs) {
        var length = joinSet.length,
            i,
            rightKeyAccessors = [];

        joinArgs.rightKeyPropertyPaths.forEach(function (keyValue) {
            rightKeyAccessors.push(getKeyValueAccessorFromKey(keyValue));
        });

        for (i = 0; i < length; i += 1) {
            var rightRecord = joinSet[i],
                currentBin = joinArgs.keyHashBin;

            if (isNullOrUndefined(rightRecord)) {
                continue;//move onto the next, can't join on records that don't exist
            }

            //for each entry in the join set add it to the source document at the correct index
            rightKeyAccessors.forEach(function (accessor) {
                currentBin = currentBin[accessor(rightRecord)];
            });
            try {
                currentBin.forEach(function (sourceDataIndex) {
                    var theObject = sourceData[sourceDataIndex][joinArgs.newKey];

                    if (isNullOrUndefined(theObject)) {//Handle adding multiple matches to the same sub document
                        sourceData[sourceDataIndex][joinArgs.newKey] = rightRecord;
                    } else if (Array.isArray(theObject)) {
                        theObject.push(rightRecord);
                    } else {
                        sourceData[sourceDataIndex][joinArgs.newKey] = [theObject, rightRecord];
                    }
                });
            } catch (e) {
                debugger;
            }
        }
    }

    function isNullOrUndefined (val) {
        return typeof val === "undefined" || val === null;
    }

    /**
     * Access an object without having to worry about "cannot access property '' of undefined" errors
     * @returns The value you were looking for or undefined
     */
    function safeObjectAccess () {
        var object = arguments[0],
            length = arguments.length,
            args = arguments,
            i,
            results,
            temp;

        if (!isNullOrUndefined(object)) {
            for (i = 1; i < length; i += 1) {
                if (Array.isArray(object)) {//if it's an array find the values from those results
                    results = [];
                    object.forEach(function (subDocument) {
                        try {
                            temp = safeObjectAccess.apply(
                                safeObjectAccess,
                                [subDocument].concat(Array.prototype.slice.apply(args, [i, length]))
                            );
                        } catch (e) {
                            debugger;
                        }



                        if (Array.isArray(temp)) {
                            if (typeof temp[0] === "undefined") {debugger}
                            results = results.concat(temp);
                        } else {
                            results.push(temp);
                        }
                    });
                    break;
                }
                if (typeof object !== "undefined") {
                    object = object[arguments[i]];
                } else {
                    break;
                }
            }
        }

        return results || object
    }

    /**
     * Simply call the first argument if it is the typeof a function
     * @param fn The argument to call if it is a function
     * @param args the arguments to call the function with
     */
    function callIfFunction (fn, args) {
        if (typeof fn === "function") {
            fn.apply(fn, args);
        }
    }
};
