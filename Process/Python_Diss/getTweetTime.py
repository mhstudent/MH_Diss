from neo4j.v1 import GraphDatabase, basic_auth

def main():
    driver = GraphDatabase.driver("bolt://localhost:7687",auth=basic_auth("neo4j", "MH4j"))
    session = driver.session()
    carryon = True
    while carryon:
        start_time = time.time()
        result = session.run("MATCH (u:SEEDUSER)-[:TWEETED]-(s:SEEDTWEET) "
                             "WITH u, COUNT(DISTINCT s) AS n_Tweets "
                             "LIMIT 10000 "
                             "MATCH (u)-[:TWEETED]-(s2:STATUS) "
                             "WHERE NOT EXISTS(s2.CA_y) "
                             "RETURN s2.ID, s2.Created_at_L LIMIT 15000 "
                             )
        try:
            result.peek()
        except ResultError:
            carryon = False
        breakdowns = {}
        for record in result:
            postDate = time.localtime(record[1]/1000)
            #print("%s" % record_str)
            breakdowns[record[0]] = {'CA_y':time.strftime('%y',postDate).lstrip('0') or '0',
                                     'CA_m':time.strftime('%m',postDate).lstrip('0') or '0',
                                     'CA_d':time.strftime('%d',postDate).lstrip('0') or '0',
                                     'CA_j':time.strftime('%j',postDate).lstrip('0') or '0',
                                     'CA_H':time.strftime('%H',postDate).lstrip('0') or '0',
                                     'CA_M':time.strftime('%M',postDate).lstrip('0') or '0',
                                     'CA_S':time.strftime('%S',postDate).lstrip('0') or '0'
        }
            #print("{}: {}".format(record[0],breakdowns[record[0]]))
        print("... {} tweets analysed".format(len(breakdowns)))
        #print(breakdowns);
        print("... updating {} records".format(len(breakdowns)))
        for key in breakdowns.keys():
            session.run("MATCH (s:STATUS {{ID: {} }}) SET s+= {{CA_y: {}, CA_m: {}, CA_d: {}, CA_j: {}, CA_H: {}, CA_M: {}, CA_S: {} }} RETURN s".format(key,
                                    breakdowns[key]['CA_y'],
                                    breakdowns[key]['CA_m'],
                                    breakdowns[key]['CA_d'],
                                    breakdowns[key]['CA_j'],
                                    breakdowns[key]['CA_H'],
                                    breakdowns[key]['CA_M'],
                                    breakdowns[key]['CA_S'],
                                ))
        print("... records updated in {} seconds".format(time.time() - start_time))
    session.close()



import time
start_time = time.time()
main()
print("--- %s seconds ---" % (time.time() - start_time))
    
#SCRATCH
#    print("Report for:\t%s\n"
#"    List 1: %d\n"
#"    List 2: %d\n"
#"    Compound score: %d\n"
#          "    Overall Sentiment: %s"% (record[0],,num_words_in_string(wordlistZaydmanOrig,record_str),vs.get('compound'),str(vs)))
#    print("{:-<65} {}".format(record[0].encode('utf-8'), str(vs)))

