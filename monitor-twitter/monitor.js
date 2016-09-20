
var gcloud = require('google-cloud');

var pubsub = gcloud.pubsub({
  projectId: 'in-full-gear',
  keyFilename: __dirname + '/../secret/auth_key.json'
});

var topic = pubsub.topic('twitter-trend-trump');

topic.get({autoCreate : true}, function(err, topic, apiResponse) {
  if(err){
    console.log('Error : ', err);
  }
  else{
    topic.publish({
      data: {
        text : 'The text of twitter message',
        source : 'iPhone',
        date : '12-12-12',
        etc : 'etc'
      }
    }, function(err, messageIds, apiResponse) {
      if(err){
        console.log('Error : ', err);
      }
      else{
        console.log('Success : ', messageIds);
      }
    });
  }
});
