module.exports = function () {

    this.query = function (queryCollection, query, fields, options) {
        //joinStack holds all the join args to be used, is used like a callstack when exec is called
        var joinStack = [],
            finalCallback;//This is the final function specified as the callback to call

        var cursor = queryCollection.find(query, fields, options),
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
         * Simply call the first argument if it is the typeof a function
         * @param fn The argument to call if it is a function
         * @param args the arguments to call the function with
         */
        function callIfFunction (fn, args) {
            if (typeof fn === "function") {
                fn.apply(fn, args);
            }
        }

        /**
         * Add the index of the left result from the query to the hash map/bin being used to associate left results with
         * the leftKeys match pattern
         * @param leftResult The result being entered into the map/bin
         * @param index The index that this result lives at in the returned array
         * @param joinLookups The find arguments array being built
         * @param keyHashBin The hashmap of the being built
         * @param rightKeys The keys for the right side of the join to use for the find query
         * @param accessors The accessor functions built to find the value of the specified join keys
         */
        function addLeftResultAddressToHashBin (leftResult, index, joinLookups, keyHashBin, rightKeys, accessors) {
            var currentHashBin = keyHashBin,
                lookupObject = {},
                accessorLength = accessors.length;

            accessors.forEach(function (accessor, accessorIndex) {
                var value = accessor(leftResult);

                lookupObject[rightKeys[accessorIndex]] = value;

                if (accessorIndex + 1 === accessorLength) {
                    if (Array.isArray(currentHashBin[value])) {
                        currentHashBin[value].push(index);
                    } else {
                        currentHashBin[value] = [index];
                    }
                } else if (typeof currentHashBin[value] === "undefined") {
                    currentHashBin = currentHashBin[value] = {};
                } else {
                    currentHashBin = currentHashBin[value];
                }
            });

            joinLookups.push(lookupObject);
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
                leftKeys = args.leftKeys,
                rightKeys = args.rightKeyPropertyPaths;//place to put incoming join results

            console.time("Build accessors");
            leftKeys.forEach(function (key) {//generate the accessors for each entry in the composite key
                accessors.push(getKeyValueAccessorFromKey(key));
            });
            console.timeEnd("Build accessors");

            length = results.length;

            //get the path first
            console.time("Build hashmap");
            for (i = 0; i < length; i += 1) {
                addLeftResultAddressToHashBin(results[i], i, joinLookups, keyHashBin, rightKeys, accessors);
            }//create the path
            console.timeEnd("Build hashmap");
            if (!Array.isArray(srcDataArray)) {
                srcDataArray = [srcDataArray];
            }

            subqueries = getSubqueries(joinLookups, args.pageSize || 5);//example
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

            //query = { $or: joinLookups };//{ joinKey: { $or: keyArray } }

            findArgs = [query];

            if (fields) {
                findArgs.push(fields);
            }
            //Looks like find keys on arguments.length and passing an undefined breaks it, hence this apply klunk

            //for join collection / something to get the right page size, fire off a query for all perform joining when
            //the join set has been fully compiled
//            joinCollection.find.apply(joinCollection, findArgs)
//                .toArray();
        }

        return this;
    };

    function getSubqueries (queries, pageSize) {
        var subqueries = [],
            numberOfChunks,
            i;
        //                                                   this is a stupid way to turn numbers into 1
        numberOfChunks = (queries.length / pageSize) + (!!(queries.length % pageSize));

        for (i = 0; i < numberOfChunks; i += 1) {
            subqueries.push(queries.slice(i * pageSize, (i * pageSize) + pageSize));
        }

        return subqueries;
    }

    function runSubqueries (subQueries, callback, collection, findArgs) {
        var i,
            args,
            responsesReceived = 0,
            length = subQueries.length,
            joinedSet = [];//The array where the results are going to get stuffed

        for (i = 0; i < subQueries.length; i += 1) {
            args = [{ $or: subQueries[i]}];
            args = args.concat(findArgs);

            collection.find({ $or: subQueries[i]}).toArray(function (err, results) {
                joinedSet = joinedSet.concat(results);
                responsesReceived += 1;

                if (responsesReceived === length) {
                    callback(joinedSet);
                }
            });
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
            //for each entry in the join set add it to the source document at the correct index
            rightKeyAccessors.forEach(function (accessor) {
                currentBin = currentBin[accessor(rightRecord)];
            });

            currentBin.forEach(function (sourceDataIndex) {
                var theObject = sourceData[sourceDataIndex][joinArgs.newKey];

                if (typeof theObject === "undefined") {//Handle adding multiple matches to the same sub document
                    sourceData[sourceDataIndex][joinArgs.newKey] = rightRecord;
                } else if (Array.isArray(theObject)) {
                    theObject.push(rightRecord);
                } else {
                    sourceData[sourceDataIndex][joinArgs.newKey] = [theObject, rightRecord];
                }
            });
        }
    }

    /**
     * Access an object without having to worry about "cannot access property '' of undefined" errors
     * @returns The value you were looking for or undefined
     */
    function safeObjectAccess () {
        var object = arguments[0],
            length = arguments.length,
            i;

        if (typeof object !== "undefined" && object !== null) {
            for (i = 1; i < length; i += 1) {
                if (typeof object !== "undefined") {
                    object = object[arguments[i]];
                } else {
                    break;
                }
            }
        }

        return object
    }
};
