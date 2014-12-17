require 'tweetstream'
require 'json'
require 'mongo'
include Mongo

TweetStream.configure do |config|
  config.consumer_key       = 'BSuwskJn3PIMXn5HtMfCtWxSR'
  config.consumer_secret    = 'jcZsfZHfq8asTkYocTzMbP8Gv2Hz2yljIMZkIlagOWX6mmKEGZ'
  config.oauth_token        = '2872706030-trkMewFbhlwlVHbjPFcjO1ndUl56YkA3Uqm0MOA'
  config.oauth_token_secret = 'OZSIh40x0dLD7KhPNgpOGSHT2WFHInVRTfZSBnTXzqvlm'
  config.auth_method        = :oauth
end


keyFilter = %w(id_str user created_at in_reply_to_status_id_str retweeted_status in_reply_to_user_id_str lang place retweeted retweet_count current_user_retweet favorite_count text entities)
userFilter = %w(id_str name screen_name lang location description followers_count statuses_count friends_count verified)
#puts keyFilter



while true do
begin

  mongo_client = MongoClient.new("localhost", 27017)
  coll = mongo_client['tweets']['20141217']

  dataList = []
  TweetStream::Client.new.sample do |status|
  # The status object is a special Hash with
  # method access to its keys.
  #da(status.to_h)
    data = status.to_h
    data.select!{|key,value| keyFilter.include? key.to_s}
    data[:user].select!{|key,value| userFilter.include? key.to_s}
  #puts JSON.pretty_generate(data)
    dataList.push(data)
    #puts data
    if dataList.length > 1000
  	 bulk = coll.initialize_ordered_bulk_op

  	 dataList.each do |data|
  		  bulk.insert(data)
  	 end
  	 p bulk.execute
     puts "Insert"
  	 dataList = []
    end

  
  end
rescue
  puts "GB says: Hey"
  sleep 10
end
end
#puts JSON.pretty_generate(x)
#rtc = retweet count id, text, lang, hashtags(emtity), media, created, screenname STC = "statuses_count,

