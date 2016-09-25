
var gcloud = require('google-cloud');
var pubsub = gcloud.pubsub({
  projectId: 'in-full-gear',
  keyFilename: __dirname + '/../secret/auth_key.json'
});
var Twitter = require('twitter');
var client = new Twitter({
  consumer_key: process.env.TWITTER_CONSUMER_KEY,
  consumer_secret: process.env.TWITTER_CONSUMER_SECRET,
  access_token_key: process.env.TWITTER_ACCESS_TOKEN_KEY,
  access_token_secret: process.env.TWITTER_ACCESS_TOKEN_SECRET,
});
var topic = pubsub.topic('usa_tweets');

topic.get({autoCreate : true}, function(err, topic, apiResponse) {
  if(err){
    console.log('Error getting topic : ', err);
  }
  else{
    startSurvelliance(topic)
  }
});

function startSurvelliance(topic){
  var isTweet = obj => obj && typeof obj.id_str === 'string' &&
      typeof obj.text === 'string' && obj.place && obj.place.country_code==="US";
  var handleTweet = tweet => {
    if(isTweet(tweet)){
      topic.publish({
        data : tweet,
        attributes : {created_at : tweet.created_at}
      }, function(err, messageIds, apiResponse){
        if(err)
          return console.log("Error publishing : %s", err);
        console.log("Message %s published", messageIds)
      });
    }
  }
  client.stream('statuses/filter', {track: 'Trump,Hillary,Clinton,debates' /* locations : "-74,40,-73,41" /* NY city */},  function(stream) {
    stream.on('data', handleTweet);
    stream.on('error', console.log);
  });
}
