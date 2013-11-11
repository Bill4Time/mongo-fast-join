/**
 * This module will perform an inner join on mongo native collections and any other collection which supports the
 * mongo-native collection API. We realize that mongodb is not meant to be used this way, but there are certainly instances
 * where a fk relationship is an appropriate way to NRDB and a necessary way to retrieve data. Honed in the fires of
 * artificial perf testing, this is mongo-fast-join.
 *
 * The novel aspect of this project is that we paginate requests for the join documents, which leverages the parallelism
 * of mongodb resulting in much quicker queries.
 *
 * Pass the query function, the queried collection, and the regular query arguments after that. At this stage you have
 * the join object. Call join on this object to begin compiling a join command. The main idea in the join arguments is
 * to specify the collection to join on, the key of the document in that collection to join on, and the key in the source
 * document to join on. It's an ad hoc foreign key relationship.
 */
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

        /**
         * Put a new key in the hash map/bin and return the new location.
         * @param bin The object in which the key should be placed
         * @param value The key value
         * @returns {*}
         */
        function putKeyInBin (bin, value) {
            if (isNullOrUndefined(bin[value])) {
                bin = bin[value] = {$val: value};
            } else {
                bin = bin[value];
            }

            return bin;//where we are in the hash tree
        }

        /**
         * Put a new index value into the array at the given bin, making a new array if there is none.
         * @param currentBin The location in which to put the array
         * @param index the index value to put in the array
         * @returns the newly created array if there was one created. Must be assigned into the correct spot
         */
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

        /**
         * Create the hash table which maps unique left key combination to an array of numbers representing the indexes of the
         * source data where those unique left key combinations were found. Use this map in the joining process to add
         * join subdocuments to the source data.
         * @param bin The bin object into which keys and indexes will be put
         * @param leftValue the left document from which to retrieve key values
         * @param index the index in the source data that the leftvalue lives at
         * @param accessors The accessor functions which retrieve the value for each join key from the leftValue.
         * @param rightKeys The keys in the right hand set which are being joined on
         */
        function buildHashTableIndices (bin, leftValue, index, accessors, rightKeys) {
            var i,
                length = accessors.length,
                lastBin;

            for (i = 0; i < length; i += 1) {
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
                            bin[subDocumentValue] = {$val: subDocumentValue};//Store $val to maintain the data type
                        }

                        buildHashTableIndices(bin[subDocumentValue], leftValue, index, accessors.slice(i + 1), rightKeySubset);
                    });
                    return;//don't go through the rest of the accessors, this recursion will take care of those
                } else {
                    bin = putKeyInBin(bin, val);
                }
            }
        }

        /**
         * Build the in and or queries that will be used to query for the join documents.
         * @param keyHashBin The bin to use to retrieve the query vals from
         * @param rightKeys The keys which will exists in the join documents
         * @param level The current level of recursion which will correspond to the accessor and the index of the right key
         * @param valuePath The values that have been gathered so far. Ordinally corresponding to the right keys
         * @param orQueries The total list of $or queries generated
         * @param inQueries The total list of $in queries generated
         */
        function buildQueriesFromHashBin (keyHashBin, rightKeys, level, valuePath, orQueries, inQueries) {
            var keys = Object.getOwnPropertyNames(keyHashBin),
                or;

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

            leftKeys.forEach(function (key) {//generate the accessors for each entry in the composite key
                accessors.push(getKeyValueAccessorFromKey(key));
            });

            length = results.length;

            //get the path first
            for (i = 0; i < length; i += 1) {
                buildHashTableIndices(keyHashBin, results[i], i, accessors, rightKeys, inQueries, joinLookups, {});
            }//create the path

            buildQueriesFromHashBin(keyHashBin, rightKeys, 0, [], joinLookups, inQueries);

            if (!Array.isArray(srcDataArray)) {
                srcDataArray = [srcDataArray];
            }

            subqueries = getSubqueries(inQueries, joinLookups, args.pageSize || 5, rightKeys);//example
            runSubqueries(subqueries, function (items) {
                var un;
                performJoining(srcDataArray, items, {
                    rightKeyPropertyPaths: rightKeyPropertyPaths,
                    newKey: newKey,
                    keyHashBin: keyHashBin
                });

                if (joinStack.length > 0) {
                    arrayJoin(srcDataArray, joinStack.shift());
                } else {
                    callIfFunction(finalCallback, [un, srcDataArray]);
                }
                callIfFunction(callback, [un, srcDataArray]);
            }, joinCollection);
        }

        return this;
    };

    /**
     * Get the paged subqueries
     */
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

    /**
     * Run the sub queries individually, leveraging concurrency on the server for better performance.
     */
    function runSubqueries (subQueries, callback, collection) {
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
                        temp = safeObjectAccess.apply(
                            safeObjectAccess,
                            [subDocument].concat(Array.prototype.slice.apply(args, [i, length]))
                        );

                        if (Array.isArray(temp)) {
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
