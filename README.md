##mongo-fast-join


Join sub documents from other collections into the original query documents and do it as fast as mongo can.

###Intro

This is our stab at the mongo join problem. As you know, joins are not supported by MongoDB natively, and this can be a pain.
We sought to create a project which performs a join query on mongodb and does it quickly. After a few attempts with less 
than stellar results, we arrived at the current implementation.

####How did we do it?

mongo-fast-join is fast because we paginate our join queries. Each document that is going to be joined to another document 
represents a unique query. This is accomplished with an $or clause. When dealing with only 10,000 original documents, this 
was miserably slow, taking up to a minute to return results on the local network. What a useless tool this would be if it 
took that long to join a measly 10,000 records. Turns out that splitting query into small queries with only 5 conditions 
in each $or clause sped the performance up by many orders of magnitude, joining 10,000 documents in less than 200ms.

We think that the reason the single query performed so poorly is because the $or clause was not intended to handle 10,000
conditions. It also seems that the query is executed in a single thread (just a guess). 


####Shut up and take my query!

This is the syntax we arrived at:

```

var MJ = require("mongo-fast-join"),
    mongoJoin = new MJ();

/*
    Say we have a collection of sales where each document holds a manual reference to the product sold. We can join the
    full product document into each sale document. Lets also assume that each product has a reference to some
    manufacturer info.
*/
        
mongoJoin
    .query(
      //say we have sales records and we store all the products for sale in a different collection
      db.collection("sales"),
        {}, //query statement
        {}, //fields
        {
            limit: 10000//options
        }
    )
    .join({
        joinCollection: db.collection("products"),
        //respects the dot notation, multiple keys can be specified in this array
        leftKeys: ["product_id"],
        //This is the key of the document in the right hand document
        rightKeys: ["_id"],
        //This is the new subdocument that will be added to the result document
        newKey: "product"
    })
    .join({
        //say that we want to get the users that commented too
        joinCollection: db.collection("manufacturers"),
        //This is cool, you can join on the new documents you add to the source document
        leftKeys: ["product.manufacturer_id"],//This field accepts many keys, which amounts to a composite key
        rightKeys: ["_id"],
        //unfortunately, as of now, you can only add subdocuments at the root level, not to arrays of subdocuments
        newKey: "manufacturer"//The upside is that this serve the majority of cases
    })
    //Call exec to run the compiled query and catch any errors and results, in the callback
    .exec(function (err, items) {
        console.log(items);
    });

```

The resulting document should have 10000 sales records with the product sold and manufaturer info attached to each sale
in a structure like this:

```

{
    _id: 1,
    product_id: 2,
    product: {
        _id: 2,
        manufacturer_id: 3
        name: "Wooden Spoon"
    },
    manufacturer: {
        _id: 3,
        name: "Betty Crocker"
    }
}

```


