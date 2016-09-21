
/*var gcloud = require('google-cloud');

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
});*/

var Twitter = require('twitter');
var client = new Twitter({
  consumer_key: process.env.TWITTER_CONSUMER_KEY,
  consumer_secret: process.env.TWITTER_CONSUMER_SECRET,
  access_token_key: process.env.TWITTER_ACCESS_TOKEN_KEY,
  access_token_secret: process.env.TWITTER_ACCESS_TOKEN_SECRET,
});

/*var stream = client.stream('statuses/filter', {track: 'javascript'});

stream.on('data', function(event) {
  console.log(event && event.text);
});

stream.on('error', function(error) {
  console.log(error);
});*/


client.stream('statuses/filter', {track: 'twitter'},  function(stream) {
  stream.on('data', function(tweet) {
    console.log(tweet.text);
  });

  stream.on('error', function(error) {
    console.log(error);
  });
});
