exports.handler = function(event, context, callback){ 

/*

    Both of the following methods work; the first one was recommended as the most up-to-date
 
*/

//Method 1
//var doc = require('dynamodb-doc');
//var dynamo = new doc.DynamoDB();
//Method 2
var AWS = require('aws-sdk');
var dynamo = new AWS.DynamoDB.DocumentClient();

/*
  console.log('Body:', event.body);
  console.log('Headers:', event.headers);
  console.log('Method:', event.method);
  console.log('Params:', event.params);
  console.log('Query:', event.query);
  console.log('parsed:', JSON.stringify(event));  */
  console.log('EMAIL :', event.params.querystring.email);
  console.log('STARTDATE :', event.params.querystring.startDate)
  console.log('ENDDATE :', event.params.querystring.endDate)


/*  check for missing required paramteres and return a sensible error */

if (event.params.querystring.startDate === undefined || event.params.querystring.endDate === undefined ){
    
    var error = {
        
        "error" : "startDate and endDate are REQUIRED",
        "help" : "?startDate=2000-01-01&endDate=2000-02-01",
        "notes" : "Parameters: email, startDate, and endDate: email is not required, ommitting it will return all email events between the start and end date"
        
    };
    callback(null, error);
    return;
}


var email = event.params.querystring.email;
var startDate = parseInt((new Date(event.params.querystring.startDate).getTime() / 1000).toFixed(0));
var endDate = parseInt((new Date(event.params.querystring.endDate).getTime() / 1000).toFixed(0));

var params = {};

if (event.params.querystring.email){
    
    /* return emails in date reange for a specific email address */
    params = {
        TableName: "DeliveredEvents",
        IndexName: "email-timestamp-index",
        ProjectionExpression: "email, #date, category, #emailResponse",
        KeyConditionExpression:"#email = :emailValue AND #date BETWEEN :startDate AND :endDate ",
        ExpressionAttributeNames: {
            "#email":"email",
            "#date":"timestamp",
            "#emailResponse" : "response"
            },
        ExpressionAttributeValues: {
            ":emailValue" : email,
            ":startDate" : startDate,
            ":endDate" : endDate
            }
    };
    
}else {
    
    /* return all emails in date range */
    params = {
        TableName: "DeliveredEvents",
        IndexName: "timestamp-index",
        ProjectionExpression: "email, #date, category, #emailResponse",
        KeyConditionExpression:"#date BETWEEN :startDate AND :endDate ",
        ExpressionAttributeNames: {
            "#date":"timestamp",
            "#emailResponse" : "response"
            },
        ExpressionAttributeValues: {
            ":startDate" : startDate,
            ":endDate" : endDate
            }
    };
    
}

/* change function call logic...currently redundant */
var operation ="";
if (event.params.querystring.email) {
    
    operation = "query";
    
}else {
    
    operation = "query";
    
}

// TODO: Handle DynamoDB pagination

dynamo[operation](params, function(err, data) {
    if (err) { 
        console.log(err); // an error occurred
        callback(null, err);
        return;
    }
    else  {
        
        console.log(data); // successful response
    }
    
    
    //create new object to return processed results
    var newData = {
        
        "DeliveredEmailEvents" : [],
        "Count" : data.Count,
        "ScannedCount": data.ScannedCount
        
    };
    
    //get the items from the dynamo search results
    var items = data.Items;    
    
    
    //iterate through the results and convert unix timestamps to date/time string
    for (var j = 0; j < items.length; j++){
        
        var item = items[j];
        
        /*  */
        var d = new Date(item.timestamp * 1000);
        //played hell trying to inline the ternary operator
        var day = d.getDate().toString().length < 2 ? "0"+(d.getDate().toString()) : d.getDate().toString();
        var month = (d.getMonth()+1).toString().length < 2 ? "0"+((d.getMonth()+1).toString()) : (d.getMonth()+1).toString();
        var hour =  d.getHours().toString().length < 2 ? "0"+(d.getHours().toString()) : d.getHours().toString();
        var minute = d.getMinutes().toString().length < 2 ? "0"+(d.getMinutes().toString()) : d.getMinutes().toString();
        var second = d.getSeconds().toString().length < 2 ? "0"+(d.getSeconds().toString()) : d.getSeconds().toString();
        
        
        // flatten out the categories a bit
        var categories = [];
        var category = item.category.values;
        
        for (var i = 0; i < item.category.values.length; i++){
                categories.push(category[i]);
        }
        
        //create final JSON for user consumption
        newData.DeliveredEmailEvents.push(
            {
                "Event" : "Delivered",
                "EventReason" : "",
                "Category" : categories,
                "Email" : item.email,
                "Datetime" : d.getFullYear() + "-" + month +  "-" + day + " " + hour + ":" + minute + ":" + second
            }
        );
    }
    
    //return the processed results to the user 
    callback(null, newData);
});


}


