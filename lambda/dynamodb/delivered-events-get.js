/* 
	Example javascript AWS Lambda function to query DynamoDB tables.
	This was called from an API Gateway instance.
	
*/


exports.handler = function(event, context, callback){ 

/*

    Both of the following methods work; the first one was recommended as the most up-to-date
 
*/

//Method 1
var doc = require('dynamodb-doc');
var dynamo = new doc.DynamoDB();

/* Method 2 - This caused severe garbage collection problems when the result set sizes increased. */
//var AWS = require('aws-sdk');
//var dynamo = new AWS.DynamoDB.DocumentClient();

/*
  console.log('Body:', event.body);
  console.log('Headers:', event.headers);
  console.log('Method:', event.method);
  console.log('Params:', event.params);
  console.log('Query:', event.query);
  console.log('parsed:', JSON.stringify(event));  
  console.log('EMAIL :', event.params.querystring.email);
  console.log('STARTDATE :', event.params.querystring.startDate)
  console.log('ENDDATE :', event.params.querystring.endDate)
  */

var error = {};
/*  check for missing required paramteres and return a sensible error */

if (event.params.querystring.startDate === undefined || event.params.querystring.endDate === undefined ){
    
    error = {
        
        "error" : "startDate and endDate are REQUIRED",
        "help" : "?startDate=2000-01-01&endDate=2000-02-01",
        "notes" : "Required Parameters: startDate, endDate: Optional: email (ommitting it will return all email events between the start and end date)"
        
    };
    callback(null, error);
    return;
} else if (event.params.querystring.table === undefined) {
    
    error = {
        
        "error" : "table paramter is REQUIRED (if this error appeared from a previously working query please contact system administrator)",
        "help" : "?table=DeliveredEvents",
        "notes" : "Required Parameters: table, startDate, endDate: Optional: email (ommitting it will return all email events between the start and end date)"
        
    };
    callback(null, error);
    return;
    
}

/* Set up a mapping to configure the search queries based on the available columns in the result set.  Projections are limited and varied.  */
var dynamoTables = [
    
    {"table": "BounceEvents",       "project" : "email, #date, category, reason, #eventType", 
    "ExprAttributes" : {
            "#email" : "email",
            "#date":"timestamp",    //the expression attributes must satisfy the project attributes that have a hash tag
            "#eventType" : "type"
            } },
    
    {"table": "DeferredEvents",     "project" : "email, #date, category, #emailResponse", 
    "ExprAttributes" : {
            "#email":"email",
            "#date":"timestamp",
            "#eventType" : "type"
            } },
    
    {"table": "DeliveredEvents",    "project" : "email, #date, category", 
        "ExprAttributes" : {
            "#email":"email",
            "#date":"timestamp"
            } 
    },
    
    {"table": "DroppedEvents",      "project" : "email, #date, category, reason",  
    "ExprAttributes" : {
            "#email":"email",
            "#date":"timestamp",
            "#eventType" : "type"
            } },
    
    {"table": "SpamReportEvents",   "project" : "email, #date, category",  
    "ExprAttributes" : {
            "#email":"email",
            "#date":"timestamp",
            "#eventType" : "type"
            }  }
];


/* 

    Prime the query expression variables based on the table parameter passed in by the user. 

*/

var table = "";
var project = "";
var expressionAttributes = {};

for (var t = 0; t < dynamoTables.length; t++){
    
    if (dynamoTables[t].table == event.params.querystring.table){
        
        table = dynamoTables[t].table;
        project = dynamoTables[t].project;
        expressionAttributes = dynamoTables[t].ExprAttributes;
        
    }
}

if (table === ""){
        
        error = {
        
            "error" : "table paramter is INVALID (if this error appeared from a previously working query please contact system administrator)",
            "help" : "?table=DeliveredEvents",
            "notes" : "Required Parameters: table, startDate, endDate: Optional: email (ommitting it will return all email events between the start and end date)"
        
        };
        callback(null, error);
        return;
        
    }


var email = event.params.querystring.email;

/* Convert start and end date to Unix Epoch timestamps -- from the date text parameters passed by the user */
var startDate = parseInt((new Date(event.params.querystring.startDate).getTime() / 1000).toFixed(0));
var endDate = parseInt((new Date(event.params.querystring.endDate).getTime() / 1000).toFixed(0));

var params = {};

if (event.params.querystring.email){
    
    /* return emails in date range for a specific email address */
    params = {
        TableName: table,
        IndexName: "email-timestamp-index",
        ProjectionExpression: project,
        KeyConditionExpression:"#email = :emailValue AND #date BETWEEN :startDate AND :endDate ",
        ExpressionAttributeNames: expressionAttributes,
        ExpressionAttributeValues: {
            ":emailValue" : email,
            ":startDate" : startDate,
            ":endDate" : endDate
            },
        Limit : 1500
    };
    
}else {
    
    delete expressionAttributes["#email"];
    
    /* return all emails in date range */
    params = {
        TableName: table,
        IndexName: "timestamp-index",
        ProjectionExpression: project,
        FilterExpression:"#date BETWEEN :startDate AND :endDate ",
        ExpressionAttributeNames: expressionAttributes,
        ExpressionAttributeValues: {
            ":startDate" : startDate,
            ":endDate" : endDate
            },
        Limit : 1500
    };
    
}

/* change dynamo search type based on paramters */
var operation ="";
if (event.params.querystring.email) {
    
    operation = "query";
    
}else {
    
    operation = "scan";
    
}



//create new object to return processed results
    var newData = {
        
        "EmailEvents" : [],
        "LastEvaluatedKey" : {}    };


q(null);   


function q(lastKey) {
    
    if (lastKey != null ) {
        
        params.ExclusiveStartKey = lastKey;
    }
    
    dynamo[operation](params, processResults);
    
}


function processResults(err, data) {
        if (err) { 
            console.log(err); // an error occurred
            callback(null, err);
            return;
        }
        else  {
            //callback(null, data);
            //console.log(data.category); // successful response
        }
        
        //get the items from the dynamo search results
        var items = data.Items;  
        var item = {};
        var dealerCode = "";
        var newCategories = [];
        var categories = [];
        var event = "";
        var eventReason = "";
        var eventType = "";
        
        /* ************************************
        
            ***** Data processing loop ***** 
            
        * *************************************/
        
        for (var j = 0; j < items.length; j++){
            
            item = items[j];
            
            
            var d = new Date(item.timestamp * 1000);
            
            var day = d.getDate().toString().length < 2 ? "0"+(d.getDate().toString()) : d.getDate().toString();
            var month = (d.getMonth()+1).toString().length < 2 ? "0"+((d.getMonth()+1).toString()) : (d.getMonth()+1).toString();
            var hour =  d.getHours().toString().length < 2 ? "0"+(d.getHours().toString()) : d.getHours().toString();
            var minute = d.getMinutes().toString().length < 2 ? "0"+(d.getMinutes().toString()) : d.getMinutes().toString();
            var second = d.getSeconds().toString().length < 2 ? "0"+(d.getSeconds().toString()) : d.getSeconds().toString();
            
            newCategories = [];
            
            // flatten out the categories a bit
            if (item.category.contents) {
                
                //console.log(item.category.values.length);
                
                categories = item.category.contents;
                
            }
            
            
            for (var key in categories) { 
                
                if (categories.hasOwnProperty(key)) {
                    
                    var obj = categories[key];
                    
                    
                    /* Make the assumption that a number is the dealer code and exclude from the categories */
                    if (isNaN(obj)){
                    
                        newCategories.push(obj);
                        
                    }else {
                        
                        //do not type cast the dealer code as a number, for it is a lowly identifier (like a phone number or SSN)
                        dealerCode = obj;
                    }        
                       
                        
                    }
                }
            
            
            /* Following the naming conventions of EventNameEvents strip the Events word to yield EventName  */
            event = table.replace("Events","");
            
            eventReason = "";
            if (item.reason) {
                
                eventReason = item.reason;
            }
            
            eventType = "";
            if (item.type) {
                
                eventType = item.type;
            }
            
            
            //create final JSON for user consumption
            newData.EmailEvents.push(
                {
                    "Event"         : event,
                    "EventReason"   : eventReason,
                    "EventType"     : eventType,
                    "DealerCode"    : dealerCode,
                    "Category"      : newCategories,
                    "Email"         : item.email,
                    "Datetime"      : d.getFullYear() + "-" + month +  "-" + day + " " + hour + ":" + minute + ":" + second
                }
            );
        }
        
       
       if (data.LastEvaluatedKey != null){
           
           q(data.LastEvaluatedKey);
           
       }else {
             
             delete newData.LastEvaluatedKey;
             callback(null, newData);     
             
        }
       
      
    }




 


}


