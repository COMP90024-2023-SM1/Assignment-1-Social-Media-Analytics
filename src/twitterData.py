import json
import re

#class twitterData:
 #   """ Data class for twitter data """

 #   def __init__(self, data: str):
        
class twitterData:
    """ Data class for twitter data """
    def __init__(self, data):
        self._id = data['_id']
        self._rev = data['_rev']
        self.author_id = data['data']['author_id']
        self.conversation_id = data['data']['conversation_id']
        self.created_at = data['data']['created_at']
        self.entities = data['data']['entities']
        self.geo = data['data']['geo']
        self.lang = data['data']['lang']
        self.public_metrics = data['data']['public_metrics']
        self.text = data['data']['text']
        self.sentiment = data['data']['sentiment']
        
        # extract nested properties
        if 'urls' in self.entities:
            self.urls = self.entities['urls']
        else:
            self.urls = None
        
        if 'mentions' in self.entities:
            self.mentions = self.entities['mentions']
        else:
            self.mentions = None
        
        if 'places' in data['includes']:
            self.places = data['includes']['places']
        else:
            self.places = None
    
    # getter methods
    def get_id(self):
        return self._id
    
    def get_rev(self):
        return self._rev
    
    def get_author_id(self):
        return self.author_id
    
    def get_conversation_id(self):
        return self.conversation_id
    
    def get_created_at(self):
        return self.created_at
    
    def get_entities(self):
        return self.entities
    
    def get_geo(self):
        return self.geo
    
    def get_lang(self):
        return self.lang
    
    def get_public_metrics(self):
        return self.public_metrics
    
    def get_text(self):
        return self.text
    
    def get_sentiment(self):
        return self.sentiment
    
    def get_urls(self):
        return self.urls
    
    def get_mentions(self):
        return self.mentions
    
    def get_places(self):
        return self.places
