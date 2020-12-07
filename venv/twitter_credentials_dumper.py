import json

# Enter your keys/secrets as strings in the following fields
credentials = {'CONSUMER_KEY': 'B7RWCY30syijLKjhfiKcjzyTI',
               'CONSUMER_SECRET': 'zTxlc1LyIjIjGtogcX2Se9rETJWAsoy8NWTVwV1c9WlTZC6uvu',
               'ACCESS_TOKEN': '1244638402210742274-GfprfdtS15VeQYUNPY37y3sUUfCiXQ',
               'ACCESS_SECRET': 'mxQV4I8CK8ZU4mNw1TBwhJwU9r03ECLNVu8Sv8CI958Ue'}

# Save the credentials object to file
with open("twitter_credentials.json", "w") as file:
    json.dump(credentials, file)
