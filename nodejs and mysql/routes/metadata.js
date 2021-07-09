var express = require('express');
var router = express.Router();

/* GET table data*/
router.get('/', function(req, res, next) {

	res.locals.connection.query('SHOW COLUMNS FROM metadata_aspect_v2 ; SELECT * FROM metadata_aspect_v2', function (error, results, fields) {
		if(error){
			res.json({"status": 500, "error": error, "response": null}); 
			//If there is error, we send the error in the error section with 500 status
		} else {
			res.json(({"status": 200, "error": null, "response": results}));
			//If there is no error, all is good and response is 200OK.
			
			
			}
		
		
	});

	
	
});

module.exports = router;
