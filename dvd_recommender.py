## Our Recommender System Script (dvd_recommender.py)
import sys
from pprint import pprint
from py2neo import authenticate, Graph

cid = sys.argv[1:]

authenticate("localhost:7474", "username", "password") 
g = Graph("http://localhost:7474/db/data/")

def cf_recommender(graph, cid, nearest_neighbors, num_recommendations):

    query = """
           MATCH (c1:Customer)-[:RENTED]->(f:Film)<-[:RENTED]-(c2:Customer)
           WHERE c1 <> c2 AND c1.customerID = {cid}
           WITH c1, c2, COUNT(DISTINCT f) as intersection
           
           MATCH (c:Customer)-[:RENTED]->(f:Film)
           WHERE c in [c1, c2]
           WITH c1, c2, intersection, COUNT(DISTINCT f) as union

           WITH c1, c2, intersection, union, 
                (intersection * 1.0 / union) as jaccard_index

           ORDER BY jaccard_index DESC, c2.customerID
           WITH c1, COLLECT(c2)[0..{k}] as neighbors
           WHERE SIZE(neighbors) = {k}                                              
           UNWIND neighbors as neighbor
           WITH c1, neighbor

           MATCH (neighbor)-[:RENTED]->(f:Film)         
           WHERE not (c1)-[:RENTED]->(f)                        
           WITH c1, f, COUNT(DISTINCT neighbor) as countnns
           ORDER BY c1.customerID, countnns DESC                            
           RETURN c1.customerID as customer, 
                  COLLECT(f.Title)[0..{n}] as recommendations      
           """

    recommendations = {}
    # cid = [str(c) for c in cid]
    for c in cid:
        for i in graph.cypher.execute(query, cid = c, k = nearest_neighbors, n = num_recommendations):
            recommendations[i[0]] = i[1]
    return recommendations

pprint(cf_recommender(g, cid, 25, 5))

# to return the data in json form for further usage with different apps
# from json import dumps
# recommendations = []
# for c in cid:
#   for i in graph.cypher.execute(query, cid = c, k = nearest_neighbors, n = num_recommendations):
#   recommendations.append({'userID': i[0], 'recommended_movies': i[1]})
# return dumps(recommendations)
# 
# recos = cf_recommender(g, cid, 25, 5)
# print(recos)
# 
# to try it in command line and save it to a file
# python dvd_recommender.py 13 11 19 91 > recos.txt
# 
# to reload the data and convert it to csv file
# < recos.txt json2csv | csvlook
# 
# | userID | recommended_movies                                                                                  |
# | ------ | --------------------------------------------------------------------------------------------------- |
# |     13 | ["Goodfellas Salute","Whisperer Giant","Mob Duffel","Fellowship Autumn","Pacific Amistad"]          |
# |     11 | ["Sweethearts Suspects","Tights Dawn","Island Exorcist","Jason Trap","Dying Maker"]                 |
# |     19 | ["Fatal Haunted","Dream Pickup","Honey Ties","Crossroads Casualties","Ridgemont Submarine"]         |
# |     91 | ["Forrester Comancheros","Hanover Galaxy","Anaconda Confessions","Greatest North","Bear Graceland"] |
#   
# to load it in python converting it to a data frame
# import json
# import pandas as pd
# from pprint import pprint
# data = []
# with open("recos.txt") as j:
#   data = json.load(j)
# 
# pprint(data)
# pd.DataFrame(data)
# 
# [{'recommended_movies': ['Goodfellas Salute',
#                          'Whisperer Giant',
#                          'Mob Duffel',
#                          'Fellowship Autumn',
#                          'Pacific Amistad'],
#   'userID': '13'},
#  {'recommended_movies': ['Sweethearts Suspects',
#                           'Tights Dawn',
#                           'Island Exorcist',
#                           'Jason Trap',
#                           'Dying Maker'],
#   'userID': '11'},
#  {'recommended_movies': ['Fatal Haunted',
#                           'Dream Pickup',
#                           'Honey Ties',
#                           'Crossroads Casualties',
#                           'Ridgemont Submarine'],
#    'userID': '19'},
#   {'recommended_movies': ['Forrester Comancheros',
#                           'Hanover Galaxy',
#                           'Anaconda Confessions',
#                           'Greatest North',
#                           'Bear Graceland'],
#    'userID': '91'}]
# 
#                                   recommended_movies userID
# 0  [Goodfellas Salute, Whisperer Giant, Mob Duffe...     13
# 1  [Sweethearts Suspects, Tights Dawn, Island Exo...     11
# 2  [Fatal Haunted, Dream Pickup, Honey Ties, Cros...     19
# 3  [Forrester Comancheros, Hanover Galaxy, Anacon...     91
