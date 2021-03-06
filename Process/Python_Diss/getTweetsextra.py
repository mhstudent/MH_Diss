from neo4j.v1 import GraphDatabase, basic_auth
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from nltk.tokenize import TweetTokenizer
from nltk.util import ngrams





def num_words_in_string(word_list, a_string):
    tknzr = TweetTokenizer()
    tokens  = tknzr.tokenize(a_string.upper())
    onegrams = set(ngrams(tokens,1))
    twograms = set(ngrams(tokens,2))
    threegrams = set(ngrams(tokens,3))
    fourgrams = set(ngrams(tokens,4))
    allgrams = set.union(onegrams,twograms,threegrams,fourgrams)
    return len(set(word_list).intersection(allgrams))


def main():
    driver = GraphDatabase.driver("bolt://localhost:7687",auth=basic_auth("neo4j", "MH4j"))
    session = driver.session()

    analyzer = SentimentIntensityAnalyzer()
    rawlistZaydman = ["abusive", "addict", "addiction", "alzheimers", "asylum", "autism", "bipolar", "bonkers", "brain damage", "brain dead", "breakdown", "coping", "crazy", "distressed", "distressing", "disturbed", "disturbing", "eating disorder", "escaped from an asylum", "few sandwiches short of a picnic basket", "freak", "gone in the head", "halfwit", "hallucinating", "hallucinations", "hand fed", "lived experience", "living with addiction", "living with alcoholism", "living with depression", "living with ptsd", "loony", "loony bin", "lunatic", "madness", "manic depression", "mass murderers", "mental", "mental health", "no-one upstairs", "not all there", "not quite there", "not the sharpest knife in the drawer", "numscull", "nutcase", "nuts", "nutter", "nutty as a fruitcake", "OCD", "off their rocker", "operational stress", "out of it", "psycho", "psychopath", "ptsd", "recovery", "retard", "schizo", "schizophrenia", "screw loose", "screwed", "self-control", "self-determination", "self-harm", "stressed", "suicide", "therapist", "therapy", "wheelchair jockey", "window licker", "you belong in a home", "demented", "depressed", "depression", "deranged", "difficulty learning", "dignity", "disabled", "handicapped", "head case", "hurting yourself", "in recovery", "insane", "intellectually challenged", "learning difficulties", "mental health challenges", "mental hospital", "mental illness", "mental institution", "mentally challenged", "mentally handicapped", "mentally ill", "padded cells", "paranoid", "pedophile", "perverted", "psychiatric", "psychiatric health", "psychiatrist", "shock syndrome", "sick in the head", "simpleton", "split personality", "stigma", "strait jacket", "stress","#abuse", "#addiction", "#alzheimers", "#anxiety", "#bipolar", "#bpd", "#Operationalstress", "#mhsm", "#trauma", "#spsm", "#alcoholism", "#depressed", "#depression", "#eatingdisorders", "#endthestigma", "#IAmStigmaFree", "#mentalhealth", "#pts", "#anxiety", "#therapy", "#endthestigma", "#AA", "#mentalhealthmatters", "#mentalhealthawareness", "#mentalillness", "#MH", "#nostigma", "#nostigmas", "#1SmallAct", "#psychology", "#mhchat", "#schizophrenia", "#ptsd", "#psychology", "#presspause", "#mentalhealthmatters", "#ocd", "#suicideprevention", "#therapy", "#trauma", "#WMHD2017", "#worldmentalhealthday"]
    rawlistZaydmanOrig = ["#mentalhealth", "#depression", "#worldmentalhealthday", "#WMHD2015", "#nostigma", "#nostigmas", "#eatingdisorders", "#suicide", "#ptsd", "#mentalhealthawareness ", "#mentalillness", "#stopsuicide", "#IAmStigmaFree", "#suicideprevention", "#MH", "#addiction", "#bipolar", "#stigma","Mental health", "depression", "stigma", "eating disorder", "suicide", "ptsd", "mental illness", "addiction", "bipolar"]
    wordlistZaydman  = map(lambda str:tuple(unicode(str.upper()).split()),rawlistZaydman)
    wordlistZaydmanOrig  = map(lambda str:tuple(unicode(str.upper()).split()),rawlistZaydmanOrig)
    while True:
        start_time = time.time()
        result = session.run("MATCH (u:SEEDUSER)-[:TWEETED]-(s:SEEDTWEET) "
                             "WITH u, COUNT(DISTINCT s) AS n_Tweets "
                             "LIMIT 1000 "
                             "MATCH (u)-[:TWEETED]-(s2:STATUS) "
                             "WHERE NOT EXISTS(s2.SentNeu) "
                             "RETURN s2.ID, s2.Text LIMIT 15000 "
                             )
        try:
            result.peek()
        except ResultError:
            break
        breakdowns = {}
        for record in result:
            record_str = record[1].encode('utf-8')
            #print("%s" % record_str)
            vs = analyzer.polarity_scores(record_str)
            breakdowns[record[0]] = {'OZ1':num_words_in_string(wordlistZaydmanOrig,record_str),
                                     'OZ2':num_words_in_string(wordlistZaydman,record_str),
                                     'SC':vs.get('compound'),
                                     'SP':vs.get('pos'),
                                     'SN':vs.get('neg'),
                                     'SZ':vs.get('neu')}
            #print("{}: {}".format(record[0],breakdowns[record[0]]))
        print("... {} tweets analysed".format(len(breakdowns)))
        #print(breakdowns);
        print("... updating {} records".format(len(breakdowns)))
        for key in breakdowns.keys():
            session.run("MATCH (s:STATUS {{ID: {} }}) SET s+= {{OZ1: {}, OZ2: {}, SentCompound: {}, SentPos: {}, SentNeg: {}, SentNeu: {} }} RETURN s".format(key,
                                    breakdowns[key]['OZ1'],
                                    breakdowns[key]['OZ2'],
                                    breakdowns[key]['SC'],
                                    breakdowns[key]['SP'],
                                    breakdowns[key]['SN'],
                                    breakdowns[key]['SZ'],
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

