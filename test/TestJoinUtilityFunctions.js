var assert = require("assert"),
    Join = require("../Join");

describe("Join.query", function () {
    var join = (new Join());
    describe("#callIfFunction", function () {
        it("should not throw an exception when a non function is passed as a argument", function () {
            assert.doesNotThrow(function () {
                join._callIfFunction("not a function");
                join._callIfFunction(false);
                join._callIfFunction([]);
            });
        });
        it("should call the function passed in as an argument", function () {
            var itRan = false;
            join._callIfFunction(function () {
                itRan = true;
            });
            assert(itRan);
        });
    });

    describe("#_removeNonMatchesLeft", function () {
        var testArrayWithOneElement = function () { return [{
                k1: {},
                k2: {}
            }];
        };

        it("should remove items where there is no matching key in the object", function () {
            var testAr = testArrayWithOneElement();
            join._removeNonMatchesLeft(testAr, "non existent key");
            assert(testAr.length === 0);
        });

        it("should not remove elements when there is a matching key", function () {
            var testAr = testArrayWithOneElement();

            join._removeNonMatchesLeft(testAr, "k1");
            assert(testAr.length === 1);

            join._removeNonMatchesLeft(testAr, "k2");
            assert(testAr.length === 1);
        });
    });

    describe("#_safeObjectAccess", function () {
        it("should return a value when given a correct path to a property in a given object", function () {
            var testObj = { here: { is: { the: { path: { to: { test: true } } } } } };

            assert(join._safeObjectAccess(testObj, "here", "is", "the", "path", "to", "test") === true);
        });

        it("should return undefined when given and incorrect path to a property in a given object", function () {
            var testObj = {
                path: {}
            };

            assert(typeof join._safeObjectAccess(testObj, "path", "is", "not", "good") === "undefined");
            assert(typeof join._safeObjectAccess(undefined, "bogus", "path") === "undefined");
        });

        it("should return undefined when given an incorrect path in an undefined value", function () {

        });

        it("should return a list of value when an array is encountered in the lookup path", function () {

        });
    })
});