from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import ConnectionFailure, OperationFailure
import csv
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import statistics
import threading
import traceback
from threading import Lock
import queue
import signal
import sys

# Global parameters
SAMPLE_SIZE = int(os.getenv('SAMPLE_SIZE', 50))
DURATION_IN_MINUTES = 1
MAX_POOL_SIZE = 150
MIN_POOL_SIZE = 50
RETRY_COUNT = 3  # Number of retries for failed operations
RETRY_DELAY = 1.0  # Seconds between retries

# MongoDB client (global)
mongo_client = None
results_summary = []
results_lock = Lock()  # Lock for thread-safe access to results_summary



import random

def get_random_index(input_list: list):
  """
  Checks the length of a list and returns a random index.

  Args:
    input_list: The list to check.

  Returns:
    A random integer index from the list, or None if the list is empty.
  """
  # First, check the length of the list.
  # 'if not input_list' is a concise way to check if the list is empty.
  if not input_list:
    print("The input list is empty.")
    return None

  # If the list is not empty, return a random index.
  # random.randrange(n) generates an integer from 0 to n-1.
  # This perfectly matches the index range of a list.
  return random.randrange(len(input_list))

def get_vectors():
    """
    Read vectors from CSV file for benchmarking.
    Returns:
        List of vector arrays (list of lists of floats)
    """
    array_of_arrays = []
    try:
        # Use absolute path to ensure we find the file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        vectors_path = os.path.join(script_dir, 'vectors.csv')
        print(f"Looking for vectors at: {vectors_path}")
        
        with open(vectors_path, newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if len(row) > 0:
                    row[0] = row[0].lstrip('[')
                    row[-1] = row[-1].rstrip(']')
                    vector = [float(value) for value in row]
                    array_of_arrays.append(vector)
        print(f"Loaded {len(array_of_arrays)} vectors from vectors.csv")
        return array_of_arrays
    except Exception as e:
        print(f"Error loading vectors: {e}")
        traceback.print_exc()  # Print the full stack trace for better debugging
        return []

# Define the pipeline functions
def search_and_filter_pipeline(queryVector, num_candidates, limit=10):
    return [
        {
            "$vectorSearch": {
                "filter": {
                    "$and": [
                        {"$and": [{"season": "Winter"}, {"$or": [{"gender": "men"}, {"gender": "men women"}]}]},
                        {"$nor": [{"brand": "Nike"}, {"brand": "Franco Leone"}]}
                    ]
                },
                "path": "embedding",
                "index": "vector_index_1",
                "limit": limit,
                "numCandidates": num_candidates,
                "queryVector": queryVector
            }
        },
        {"$project": {"embedding": 0}}
    ]

def search_and_bucket_pipeline(queryVector, num_candidates, limit=5):
    return [
        {
            "$vectorSearch": {
                "path": "embedding",
                "index": "vector_index_1",
                "limit": limit,
                "numCandidates": num_candidates,
                "queryVector": queryVector
            }
        },
        {"$project": {"_id": 1, "style_id": 1, "similarity_score": {"$meta": "vectorSearchScore"}}},
        {
            "$bucketAuto": {
                "groupBy": "$similarity_score",
                "buckets": 3,
                "output": {
                    "count": {"$sum": 1},
                    "avgValue": {"$avg": "$similarity_score"},
                    "bucketContents": {"$push": {"_id": "$_id", "style_id": "$style_id", "similarity_score": "$similarity_score"}}
                }
            }
        }
    ]

def search_bucket_sort_pipeline(queryVector, num_candidates, limit=5):
    return [
        {
            "$vectorSearch": {
                "path": "embedding",
                "index": "vector_index_1",
                "limit": limit,
                "numCandidates": num_candidates,
                "queryVector": queryVector
            }
        },
        {"$project": {"_id": 1, "style_id": 1, "valid_from": 1, "similarity_score": {"$meta": "vectorSearchScore"}}},
        {
            "$bucketAuto": {
                "groupBy": "$similarity_score",
                "buckets": 3,
                "output": {
                    "count": {"$sum": 1},
                    "avgValue": {"$avg": "$similarity_score"},
                    "bucketContents": {
                        "$push": {"_id": "$_id", "style_id": "$style_id", "similarity_score": "$similarity_score", "valid_from": "$valid_from"}
                    }
                }
            }
        },
        {
            "$set": {
                "sorted_desc_by_discounted_price": {
                    "$sortArray": {
                        "input": "$bucketContents",
                        "sortBy": {"discounted_price": 1, "similarity_score": -1}
                    }
                }
            }
        }
    ]

def functionalHybrid_pipeline(queryVector, num_candidates, limit):
    return [
        {
            "$rankFusion": {
                "input": {
                    "pipelines": {
                        "searchOne": [
                            {
                                "$vectorSearch": {
                                    "index": "vector_index_1",
                                    "queryVector": queryVector,
                                    "path": "embedding",
                                    "numCandidates": num_candidates,
                                    "limit": limit,
                                    "compound": {
                                        "must": [
                                            {
                                                "equals": {
                                                    "path": "brands_filter_facet",
                                                    "value": "Titan"
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        ],
                        "searchTwo": [
                            {
                                "$search": {
                                    "index": "ecom_search",
                                    "compound": {
                                        "must": [
                                            {
                                                "compound": {
                                                    "should": [
                                                        {
                                                            "text": {
                                                                "query": "women",
                                                                "path": "global_attr_gender"
                                                            }
                                                        },
                                                        {
                                                            "text": {
                                                                "query": "dress",
                                                                "path": "global_attr_sub_category"
                                                            }
                                                        },
                                                        {
                                                            "text": {
                                                                "query": "dresses",
                                                                "path": "global_attr_article_type"
                                                            }
                                                        },
                                                        {
                                                            "text": {
                                                                "query": "yellow",
                                                                "path": "global_attr_base_colour"
                                                            }
                                                        },
                                                        {
                                                            "text": {
                                                                "query": "maxi",
                                                                "path": "Length_article_attr"
                                                            }
                                                        }
                                                    ]
                                                }
                                            },
                                            {
                                                "equals": {
                                                    "path": "brands_filter_facet",
                                                    "value": "Titan"
                                                }
                                            }
                                        ]
                                    },    "concurrent": True

                                }
                            },
                            {
                                "$limit": limit
                            }
                        ]
                    }
                },
                "scoreDetails": True
            }
        },
        {
            "$limit": limit
        },
        {
            "$project": {
                "_id": 1,
                "score": {
                    "$meta": "vectorSearchScore"
                }
            }
        }
    ]

def vectoronly_pipeline(queryVector, num_candidates, limit):
    return [
        {
            "$vectorSearch": {
                "index": "vector_index_1",
                "queryVector": queryVector,
                "path": "embedding",
                "numCandidates": num_candidates,
                "limit": limit,
                "compound": {
                    "must": [
                        {
                            "equals": {
                                "path": "brands_filter_facet",
                                "value": "Titan"
                            }
                        }
                    ]
                }
            }
        },
        {"$project": {"_id": 1, "style_id": 1, "similarity_score": {"$meta": "vectorSearchScore"}}}
    ]


# [
#   {
#     "$search": {
#       "index": "hybrid_index",
#       "vectorSearch": {
#         "queryVector": [
#           -0.0201064832508564,
#           0.1487722545862198,
#           -0.11861132830381393,
#           0.0229044072329998,
#           -0.0019730646163225174,
#           -0.023072686046361923,
#           -0.02541542612016201,
#           -0.03159189596772194,
#           -0.0034571397118270397,
#           0.059709835797548294,
#           -0.03367619216442108,
#           -0.02184057980775833,
#           -0.0006119857425801456,
#           0.0522528812289238,
#           0.00040721037657931447,
#           0.013197406195104122,
#           -0.05151853337883949,
#           -0.05041160434484482,
#           -0.054251812398433685,
#           -0.04080095514655113,
#           0.01963498443365097,
#           -0.030432209372520447,
#           0.009034598246216774,
#           0.011506729759275913,
#           -0.007192128337919712,
#           -0.05418338626623154,
#           -0.02984573505818844,
#           0.0006448133499361575,
#           -0.04787407070398331,
#           -0.024125875905156136,
#           -0.033474937081336975,
#           -0.029562512412667274,
#           -0.03402352333068848,
#           -0.04825311899185181,
#           0.003963068127632141,
#           0.033241644501686096,
#           -0.042169272899627686,
#           0.002728058258071542,
#           0.005731653422117233,
#           0.022045446559786797,
#           0.040060315281152725,
#           0.008905114606022835,
#           0.07059171050786972,
#           0.0017800654750317335,
#           -0.029532959684729576,
#           0.007908648811280727,
#           0.056414611637592316,
#           -0.011271542869508266,
#           -0.025011712685227394,
#           -0.018029827624559402,
#           -0.0147182522341609,
#           0.05629323795437813,
#           0.021468160673975945,
#           -0.03550543636083603,
#           0.010463444516062737,
#           -0.04739793390035629,
#           0.01099634263664484,
#           0.017404984682798386,
#           0.04215577244758606,
#           -0.019746260717511177,
#           0.014893676154315472,
#           0.05662450194358826,
#           -0.011776681058108807,
#           0.008015912026166916,
#           0.025230256840586662,
#           0.02990671619772911,
#           -0.028732245787978172,
#           -0.055899906903505325,
#           -0.036207571625709534,
#           -0.04410932958126068,
#           -0.024256402626633644,
#           0.028572728857398033,
#           -0.050073981285095215,
#           0.02691655047237873,
#           -0.062156084924936295,
#           0.0019212361657992005,
#           0.039154086261987686,
#           -0.05710287764668465,
#           0.014912120066583157,
#           -0.05452258512377739,
#           0.038939423859119415,
#           -0.018312962725758553,
#           -0.027336863800883293,
#           0.0032198333647102118,
#           0.04884929209947586,
#           0.01658068597316742,
#           0.015144247561693192,
#           -0.04482986032962799,
#           0.08242899924516678,
#           -0.025874635204672813,
#           0.023520704358816147,
#           0.007822058163583279,
#           -0.06066882982850075,
#           -0.000035201046557631344,
#           -0.08744726330041885,
#           0.006970968563109636,
#           0.034547630697488785,
#           0.004913381766527891,
#           -0.03716353327035904,
#           -0.045016027987003326,
#           -0.003203599713742733,
#           -0.02317928895354271,
#           0.024737292900681496,
#           -0.026531027629971504,
#           -0.0039276136085391045,
#           0.017457472160458565,
#           0.08050452917814255,
#           -0.030590428039431572,
#           0.029549328610301018,
#           0.0043943943455815315,
#           -0.05179845169186592,
#           0.02954559400677681,
#           -0.01553270686417818,
#           0.028443947434425354,
#           0.05988426133990288,
#           0.03214225172996521,
#           -0.0015051852678880095,
#           0.0579645037651062,
#           -0.004705200903117657,
#           -0.02808568812906742,
#           0.012175232172012329,
#           0.005501444451510906,
#           0.03996625542640686,
#           -0.04129323735833168,
#           -0.050669532269239426,
#           -0.006859889719635248,
#           0.037986285984516144,
#           0.008467433042824268,
#           0.03349757567048073,
#           -0.0028377401176840067,
#           0.008423957042396069,
#           -0.026754748076200485,
#           0.029449276626110077,
#           -0.02529357746243477,
#           -0.01722736284136772,
#           -0.01697935350239277,
#           -0.009070142172276974,
#           0.028204698115587234,
#           -0.020755521953105927,
#           0.0900314599275589,
#           -0.021327393129467964,
#           0.018121007829904556,
#           -0.05593365058302879,
#           0.027152352035045624,
#           0.0207399670034647,
#           -0.00783876795321703,
#           -0.041106171905994415,
#           -0.10934920608997345,
#           0.03551144152879715,
#           -0.015877006575465202,
#           0.004449500702321529,
#           -0.010986820794641972,
#           0.3440491259098053,
#           -0.043883316218853,
#           0.05276532098650932,
#           0.008871939964592457,
#           -0.003299188334494829,
#           0.035851217806339264,
#           -0.02104111760854721,
#           -0.013994673267006874,
#           0.014328823424875736,
#           0.032021667808294296,
#           -0.035381246358156204,
#           -0.0242910198867321,
#           0.009634587913751602,
#           -0.07684724032878876,
#           0.019479624927043915,
#           0.029775802046060562,
#           -0.010768369771540165,
#           -0.03830777108669281,
#           0.0028358635026961565,
#           0.012304707430303097,
#           0.011001677252352238,
#           -0.044508080929517746,
#           -0.018534459173679352,
#           -0.10882432758808136,
#           -0.002067150780931115,
#           0.033103957772254944,
#           -0.21740175783634186,
#           -0.0582740493118763,
#           -0.031494516879320145,
#           0.02508736215531826,
#           0.008710451424121857,
#           -0.002269816817715764,
#           -0.0658140704035759,
#           -0.035105738788843155,
#           -0.03879809007048607,
#           -0.03878716006875038,
#           -0.03886638954281807,
#           -0.04268747940659523,
#           -0.007982713170349598,
#           -0.033567383885383606,
#           0.023365696892142296,
#           -0.003309665946289897,
#           0.012072956189513206,
#           -0.006269560195505619,
#           0.04793853312730789,
#           -0.05652637034654617,
#           -0.007795351557433605,
#           -0.0002124997554346919,
#           0.015115821734070778,
#           0.03829530254006386,
#           -0.01331008318811655,
#           -0.00021195539738982916,
#           -0.0003285462153144181,
#           -0.07418735325336456,
#           -0.01332167349755764,
#           -0.012815158814191818,
#           0.04997776448726654,
#           -0.010152582079172134,
#           0.022359317168593407,
#           -0.019989781081676483,
#           0.030714480206370354,
#           -0.07936056703329086,
#           -0.012950463220477104,
#           0.025299832224845886,
#           0.026892339810729027,
#           -0.02492212876677513,
#           0.025611666962504387,
#           -0.05382484942674637,
#           -0.03690319135785103,
#           -0.018875524401664734,
#           0.010574945248663425,
#           0.012263416312634945,
#           -0.015207992866635323,
#           0.02131807990372181,
#           -0.039710018783807755,
#           -0.003426468698307872,
#           0.0138442637398839,
#           -0.0037830513902008533,
#           -0.03565661609172821,
#           -0.02434716187417507,
#           -0.014042236842215061,
#           0.0000557036510144826,
#           -0.019472235813736916,
#           0.05627141520380974,
#           -0.007560985628515482,
#           0.04766027629375458,
#           -0.032502640038728714,
#           0.04132441058754921,
#           0.034257300198078156,
#           0.07223767787218094,
#           0.006616516970098019,
#           0.02434973418712616,
#           -0.005073179956525564,
#           0.033854465931653976,
#           -0.00478943157941103,
#           0.07028624415397644,
#           -0.006965024396777153,
#           0.01424737274646759,
#           0.10360682755708694,
#           0.01080971211194992,
#           -0.008123337291181087,
#           -0.02430424839258194,
#           0.047892164438962936,
#           0.08836265653371811,
#           0.005493690259754658,
#           0.0031945290975272655,
#           -0.026369580999016762,
#           0.0052146315574646,
#           0.02226981520652771,
#           -0.03866500407457352,
#           0.043518539518117905,
#           -0.00958794355392456,
#           -0.025816194713115692,
#           -0.034985221922397614,
#           -0.029373984783887863,
#           0.002973745111376047,
#           -0.005156893748790026,
#           0.00015849267947487533,
#           0.09768857061862946,
#           -0.030050503090023994,
#           0.0169169120490551,
#           0.01669398508965969,
#           -0.017113903537392616,
#           -0.039556607604026794,
#           -0.0030868700705468655,
#           -0.010477220639586449,
#           0.0011800522916018963,
#           0.0032737385481595993,
#           0.00518159382045269,
#           0.027609800919890404,
#           0.03790111094713211,
#           0.024905795231461525,
#           0.02160768397152424,
#           -0.007662704214453697,
#           0.028248010203242302,
#           0.046605899930000305,
#           0.04407903179526329,
#           0.009219176135957241,
#           -0.011537667363882065,
#           -0.009187108837068081,
#           0.0013908451655879617,
#           -0.06094221770763397,
#           -0.004330971743911505,
#           0.010779122821986675,
#           -0.006139194127172232,
#           -0.046905968338251114,
#           0.017690405249595642,
#           -0.01594666950404644,
#           0.03986065089702606,
#           0.014219988137483597,
#           -0.0072530522011220455,
#           -0.02085501328110695,
#           -0.005576051771640778,
#           0.024924136698246002,
#           -0.055733002722263336,
#           -0.34444460272789,
#           -0.020613329485058784,
#           0.02432492934167385,
#           -0.0017822608351707458,
#           -0.0010893502039834857,
#           -0.009683090262115002,
#           -0.0598880797624588,
#           0.0167591143399477,
#           -0.05458337441086769,
#           -0.011782759800553322,
#           -0.0411510095000267,
#           0.009064002893865108,
#           0.03433869034051895,
#           0.013435483910143375,
#           -0.003934064414352179,
#           -0.003895123954862356,
#           -0.1363372653722763,
#           0.04907556623220444,
#           -0.01960589736700058,
#           0.050925351679325104,
#           -0.005595958326011896,
#           0.006474367808550596,
#           0.028917251154780388,
#           -0.036556169390678406,
#           0.018556399270892143,
#           0.006946125999093056,
#           0.01595880463719368,
#           -0.002890524687245488,
#           0.039529986679553986,
#           0.03585628420114517,
#           0.015111241489648819,
#           -0.0027017982210963964,
#           0.028884468600153923,
#           0.024464977905154228,
#           0.02750825509428978,
#           -0.022654714062809944,
#           -0.034245092421770096,
#           -0.026008563116192818,
#           -0.012774558737874031,
#           -0.023166313767433167,
#           0.05885109305381775,
#           -0.1580580621957779,
#           0.023398904129862785,
#           -0.0315244197845459,
#           -0.04182368144392967,
#           -0.019902558997273445,
#           0.04979719594120979,
#           -0.009078259579837322,
#           0.04593415558338165,
#           -0.056588102132081985,
#           0.007315593771636486,
#           -0.03437407314777374,
#           -0.03162648528814316,
#           0.024972332641482353,
#           -0.023035580292344093,
#           -0.011015322990715504,
#           -0.0057333591394126415,
#           0.0003090178652200848,
#           -0.0008304817019961774,
#           -0.024970028549432755,
#           0.022835317999124527,
#           -0.0272944625467062,
#           -0.02296288125216961,
#           -0.00033593684202060103,
#           -0.06105673685669899,
#           -0.013106493279337883,
#           -0.02852444350719452,
#           0.03691049665212631,
#           0.09738779067993164,
#           0.053504522889852524,
#           0.012093735858798027,
#           -0.01718793623149395,
#           0.020589107647538185,
#           -0.00650152750313282,
#           -0.06820682436227798,
#           0.00020059883536305279,
#           -0.0029556001536548138,
#           -0.020614616572856903,
#           -0.0013122400268912315,
#           -0.03442579507827759,
#           -0.027307575568556786,
#           -0.028839558362960815,
#           -0.013216923922300339,
#           0.03438922390341759,
#           -0.03542375937104225,
#           0.035085584968328476,
#           -0.01673349365592003,
#           0.05554848164319992,
#           -0.04288926720619202,
#           -0.04800771176815033,
#           -0.017574427649378777,
#           0.024151289835572243,
#           0.002224074210971594,
#           0.007615805137902498,
#           0.006539061199873686,
#           0.005576377268880606,
#           0.0634303092956543,
#           -0.06612762063741684,
#           0.006145598832517862,
#           0.006522724404931068,
#           0.03386158496141434,
#           0.006463278084993362,
#           -0.03165983408689499,
#           0.0534995011985302,
#           0.04367407038807869,
#           0.09866061806678772,
#           0.04497949779033661,
#           0.0047086975537240505,
#           0.021141469478607178,
#           0.014851348474621773,
#           -0.00019661692203953862,
#           0.041900940239429474,
#           -0.016128189861774445,
#           -0.0012539078015834093,
#           0.021244939416646957,
#           0.034074824303388596,
#           0.030312862247228622,
#           -0.045688189566135406,
#           0.017932439222931862,
#           0.004433143883943558,
#           0.019912676885724068,
#           -0.010085484012961388,
#           0.029314469546079636,
#           -0.055090002715587616,
#           0.15774881839752197,
#           0.05118006095290184,
#           0.04271409288048744,
#           -0.01899629272520542,
#           0.004590462427586317,
#           0.02374769002199173,
#           0.01631883718073368,
#           0.04227214306592941,
#           0.020136700943112373,
#           0.02404659241437912,
#           0.020809631794691086,
#           -0.03540598228573799,
#           -0.002138636540621519,
#           -0.056582216173410416,
#           0.005902410950511694,
#           -0.01126278005540371,
#           0.05166744440793991,
#           0.06905107945203781,
#           -0.043626293540000916,
#           -0.0592670664191246,
#           -0.03066304512321949,
#           -0.05788843333721161,
#           -0.04987412318587303,
#           0.04425753653049469,
#           0.004797743167728186,
#           0.0070365965366363525,
#           -0.03303143382072449,
#           -0.001987610710784793,
#           -0.012195426970720291,
#           0.022353988140821457,
#           -0.013910778798162937,
#           0.029507670551538467,
#           0.017276663333177567,
#           -0.020782485604286194,
#           0.008643390610814095,
#           -0.01841263473033905,
#           -0.006143581587821245,
#           0.018205860629677773,
#           -0.030050255358219147,
#           0.0312272347509861,
#           0.021444307640194893,
#           0.007436156272888184,
#           0.02368069998919964,
#           0.04778039827942848,
#           0.04498480260372162,
#           0.06074325367808342,
#           0.020596375688910484,
#           0.0008783764205873013,
#           0.049973323941230774,
#           -0.05162253975868225,
#           -0.014244955964386463,
#           0.011760525405406952,
#           -0.015091171488165855,
#           -0.00566983874887228,
#           0.0014902836410328746,
#           -0.006302577443420887,
#           0.012170697562396526,
#           -0.03311685100197792,
#           -0.003587256884202361,
#           -0.18456053733825684,
#           -0.015866467729210854,
#           -0.02431931346654892,
#           -0.0034006312489509583,
#           -0.06821469962596893,
#           0.03903824836015701,
#           0.04347369074821472,
#           -0.08373434841632843,
#           -0.0024598329328000546,
#           0.01776745170354843,
#           -0.0034356415271759033,
#           0.01972811482846737,
#           0.038119394332170486,
#           -0.04459664225578308,
#           -0.007493751589208841,
#           -0.018679920583963394,
#           -0.014922508038580418,
#           -0.03541802614927292,
#           -0.05942815542221069,
#           0.044563956558704376,
#           0.007277531549334526,
#           0.05558481812477112
#         ],
#         "path": "embedding",
#         "numCandidates": 10,
#         "limit": 10,
#         "filter": {
#           "compound": {
#             "must": [
#               {
#                 "compound": {
#                   "should": [
#                     {
#                       "text": {
#                         "query": "women",
#                         "path": "gender"
#                       }
#                     },
#                     {
#                       "text": {
#                         "query": "dress",
#                         "path": "sub_category"
#                       }
#                     },
#                     {
#                       "text": {
#                         "query": "dresses",
#                         "path": "article_type"
#                       }
#                     },
#                     {
#                       "text": {
#                         "query": "yellow",
#                         "path": "base_colour"
#                       }
#                     }
#                   ]
#                 }
#               },
#               {
#                 "equals": {
#                   "path": "brand",
#                   "value": "Titan"
#                 }
#               }
#             ]
#           }
#         }
#       },
#       "concurrent": true
#     }
#   },
#   {
#     $project: {
#       _id:1,
#       score:{$meta:'searchScore'}
#     }
#   }
# ]
all_genders=["Boys","Unisex","Women","Men","Girls","Unisex Kids"]
all_styles=['Bundles','Single Styles']  
all_articles=["Kajal and Eyeliner","Peg Measure","Storage Drum","Baby Hair Oil","Foot Cream and Scrubs","Gloves","Roti Box","Photo Albums","Bra","Book Shelf","Makeup Brushes","Body Wash and Shower Gel","Sanitizer Sprayer","Lounge Shorts","Tennis Racquets","Beauty Combo","Masher","Bedding Set","Appliance covers","Lingerie Set","Outdoor Lamps","Hair Cream and Mask","Bins","Flats","Water Heater","Attar","Bodysuit","Sweets","Skirts","Hanger","Room Heater","Breast Firming Gels and Creams","Highlighter","Boxes","Towels","Bikini Trimmers","Speakers","Vacuum Cleaner","Pendant Diamond","High Chairs","Pet Bowls","Headphones","Cleaning Cloth","Face Shield","Action Figures and Play Set","Hair Oil","Shower Caps and Headbands","Pet Paw Creams","Pet Crates and Carriers","Toe Rings","Jodhpuri Pants","Mittens","Toothbrush","Induction Cooktop","Garden Accessories","Munchies","Fans","Safe Locker","Pet Scoopers","Shackets","Baby Oral Care","Playards","Suspenders","Bed","Footballs","Charms","Pendant Gold","Baby Sippers","Dog Sweatshirts","Home Audio","Subscription","Laptop Bag","Tea","Wine Accessory Sets","Pet Collars","Rain Suit","Massage Oils","Umbrellas","Conditioner","Strollers","Fountains","School Essentials","Syrups","Hand Cream","Lipstick","Kurtas","Nehru Jackets","Immunity Boosters","Flashlight","Warmers","Electric Pressure Cookers","Dog Jackets","Hip Flask","Shawl","Bindi","Shavers","Knife Sharpener","Sunscreen","Caps","Thermal Tops"]
all_colors=['Beige','Charcoal','Lavender','Fuchsia','Nude','null','Brown','Fluorescent Green','Blue','Metallic','Red','Taupe','Maroon','Khaki','Orange','Gold','Navy Blue','Peach','NA','Burgundy','Tan','Cream','Mustard','Assorted','Camel Brown','Coffee Brown','Mauve','Skin','Grey Melange','Off White','Black','Magenta','Copper','Grey','Olive','Red','Silver','Turquoise Blue','White','Yellow','Teal','Multi','Transparent','Mushroom Brown','Lime Green','Bronze','Violet','Cognac','Green','Sea Green','Rose','Coral','Rose Gold','Steel','Purple','Pink','MUSTARD','Champagne','Rust']
all_brands= ["RJ Denim","Creatures of Habit","Spruce Shave Club","ADIVER","HOLDIT","Farkraft","SKINAA","Bamboo Tribe","MAHISHAA","Galad","MANOHUNT","joley poley","Abena","RoyalDeco","Ikiriya","KVL","YOTHVIK","Manish Creations","FAVON","DARIDRA BHANJAN","BRAVE SOUL","HUNGOVER","Wipro","Roadking","Alamod","Bitto","Chhaya Mehrotra","SJ ART & CRAFTS","Saee","TIPY TIPY TAP","CASMARA","Baidyanath","AIRAVAT","ZOCI VOCI","Sitaram Designer","LACDINE","Concepts","DERMAVIVE","MELBIFY","A Homes Grace","Phosphorus","Peseta","Mom's Therapy","HASHCART","Order Happiness","SIGNORIA","FELIZ THE DESIGNER STUDIO","MINNI TC","Arzari","DOUBLE R BAGS","ANJALI","Leeposh","Ayuvya","THEFIGUREOUT","Precious and Natures","OCIO","Delonghi","VenderVilla","ARISTOBRAT","Ennoble","CUTIE&BOO","ArtiZenWeaves","UFO","MALHAAR","PENNY JEWELS","CraZArt","APNAPHOTO","DHUNKI","Garspelle","ALORNOR","RIG-TIE","BEYONCE","SIALIA","Coffeeza","Punekar Cotton","2Bme","LA LOFT","SECRETT CURVES","Quace","Rensilafab","La Vie en Rose","Umberto Giannini","ZUUL","BASIS","Bottega Pereira","Al Wadi","Aravi Organic","Vaya","Frozen","BOLTS and BARRELS","Threeness","Headway","kolapuri centre","UDBHAV TEXTILE","MEGADETH","MUCH MORE","AGIL ARMERO","Anny Deziner","Renee Label","Ubon"]
def new_index_dynamicfilter_pipeline(queryVector,num_candidates,limit):
    brand=all_brands[get_random_index(all_brands)]
    color=all_colors[get_random_index(all_colors)]
    articletype=all_articles[get_random_index(all_articles)]
    stylecategory=all_styles[get_random_index(all_styles)]
    gender=all_genders[get_random_index(all_genders)]
    return[
    {
        '$search': {
            'index': 'hybrid_index', 
            'vectorSearch': {
                'queryVector': queryVector, 
                'path': 'embedding', 
                'numCandidates': num_candidates, 
                'limit': limit, 
                'filter': {
                    'compound': {
                        'must': [
                            {
                                'compound': {
                                    'should': [
                                        {
                                            'text': {
                                                'query': gender, 
                                                'path': 'gender'
                                            }
                                        }, {
                                            'text': {
                                                'query': "dress", 
                                                'path': 'style_category'
                                            }
                                        }, {
                                            'text': {
                                                'query':articletype, 
                                                'path': 'article_type'
                                            }
                                        }, {
                                            'text': {
                                                'query': color, 
                                                'path': 'base_colour'
                                            }
                                        }
                                    ]
                                }
                            }, {
                                'text': {
                                    'path': 'brand', 
                                    'query': brand
                            }
                            }
                        ]
                    }
                }
            }, 
            'concurrent': True
        }
    }, {
        '$project': {
            '_id': 1, 
            'score': {
                '$meta': 'searchScore'
            }
        }
    }
]

def new_index_staticfilter_pipeline(queryVector,num_candidates,limit):
    return[
    {
        '$search': {
            'index': 'hybrid_index', 
            'vectorSearch': {
                'queryVector': queryVector, 
                'path': 'embedding', 
                'numCandidates': num_candidates, 
                'limit': limit, 
                'filter': {
                    'compound': {
                        'must': [
                            {
                                'compound': {
                                    'should': [
                                        {
                                            'text': {
                                                'query': "Women", 
                                                'path': 'gender'
                                            }
                                        }, {
                                            'text': {
                                                'query': "dresses", 
                                                'path': 'style_category'
                                            }
                                        }, {
                                            'text': {
                                                'query':"dress", 
                                                'path': 'article_type'
                                            }
                                        }, {
                                            'text': {
                                                'query': "yellow", 
                                                'path': 'base_colour'
                                            }
                                        }
                                    ]
                                }
                            }, {
                                'equals': {
                                    'path': 'brand', 
                                    'value': "Titan"
                            }
                            }
                        ]
                    }
                }
            }, 
            'concurrent': True
        }
    }, {
        '$project': {
            '_id': 1, 
            'score': {
                '$meta': 'searchScore'
            }
        }
    }
]

def new_index_staticfilter_pipeline2(queryVector,num_candidates,limit):
    return[
    {
        '$search': {
            'index': 'hybrid_index', 
            'vectorSearch': {
                'queryVector': queryVector, 
                'path': 'embedding', 
                'numCandidates': num_candidates, 
                'limit': limit, 
                'filter': {
                    'compound': {
                        'must': [
                            {
                                'compound': {
                                    'should': [
                                        {
                                            'text': {
                                                'query': "Women", 
                                                'path': 'gender'
                                            }
                                        }, {
                                            'text': {
                                                'query': "dresses", 
                                                'path': 'style_category'
                                            }
                                        }, {
                                            'text': {
                                                'query':"dress", 
                                                'path': 'article_type'
                                            }
                                        }, {
                                            'text': {
                                                'query': "yellow", 
                                                'path': 'base_colour'
                                            }
                                        }
                                    ]
                                }
                            }, {
                                'text': {
                                    'path': 'brand', 
                                    'query': "Titan"
                            }
                            }
                        ]
                    }
                }
            }, 
            'concurrent': True
        }
    }, {
        '$project': {
            '_id': 1, 
            'score': {
                '$meta': 'searchScore'
            }
        }
    }
]

def no_filter_pipeline(queryVector,num_candidates,limit):
    return [
    {
        '$search': {
            'index': 'hybrid_index', 
            'vectorSearch': {
                'queryVector':queryVector, 
                'path': 'embedding', 
                'numCandidates': num_candidates, 
                'limit': limit, 
            }, 'concurrent': True
        }
    }, {
        '$project': {
            '_id': 1, 
            'score': {
                '$meta': 'searchScore'
            }
        }
    }
]


# MongoDB connection initialization
def init_mongo_client(uri):
    """
    Initialize MongoDB client with connection pooling and retries
    """
    global mongo_client
    for attempt in range(RETRY_COUNT):
        try:
            mongo_client = MongoClient(
                uri, 
                server_api=ServerApi('1'), 
                maxPoolSize=MAX_POOL_SIZE, 
                minPoolSize=MIN_POOL_SIZE,
                connectTimeoutMS=5000,
                socketTimeoutMS=30000,
                waitQueueTimeoutMS=10000,
                retryWrites=True
            )
            # Test the connection
            mongo_client.admin.command('ping')
            print("✅ MongoDB connection established successfully")
            return True
        except ConnectionFailure as e:
            if attempt < RETRY_COUNT - 1:
                print(f"MongoDB connection attempt {attempt+1} failed: {e}. Retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"❌ Failed to connect to MongoDB after {RETRY_COUNT} attempts: {e}")
                return False

def run_pipeline_threading(pipeline, db_name, coll_name):
    """
    Run a MongoDB pipeline with proper error handling and retries
    
    Args:
        pipeline: MongoDB aggregation pipeline
        db_name: Database name
        coll_name: Collection name
        
    Returns:
        Duration in milliseconds or None if operation failed
    """
    global mongo_client
    
    for attempt in range(RETRY_COUNT):
        try:
            coll = mongo_client[db_name][coll_name]
            start = time.perf_counter()
            list(coll.aggregate(pipeline))
            duration = (time.perf_counter() - start) * 1000  # convert to ms
            return duration
        except (ConnectionFailure, OperationFailure) as e:
            if attempt < RETRY_COUNT - 1:
                print(f"Pipeline execution attempt {attempt+1} failed: {e}. Retrying...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"Pipeline execution failed after {RETRY_COUNT} attempts: {e}")
                return None

class BenchmarkManager:
    """
    Thread-safe manager for running benchmarks with proper synchronization
    """
    def __init__(self, vectors, db_name, coll_name):
        self.vectors = vectors
        self.db_name = db_name
        self.coll_name = coll_name
        self.running = False
        self.shutdown_event = threading.Event()
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
    
    def handle_signal(self, signum, frame):
        """Signal handler for graceful shutdown"""
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        self.shutdown_event.set()
    
    def benchmark_all_models(self, thread_count, pipeline_name, pipeline_fn, rpm, num_candidates, limit):
        """
        Run a benchmark with the specified parameters using thread-safe operations
        
        Args:
            thread_count: Number of concurrent threads
            pipeline_name: Name of the pipeline for reporting
            pipeline_fn: Function that creates the pipeline
            rpm: Target requests per minute
            num_candidates: Number of candidates for vector search
            limit: Limit for results
        """
        global mongo_client, results_summary
        
        # Mark as running
        self.running = True
        
        try:
            # Calculate total requests
            total_requests = rpm * DURATION_IN_MINUTES
            
            # Prepare test data
            sample_size = min(SAMPLE_SIZE, len(self.vectors))
            sampled_vectors = random.sample(self.vectors, sample_size)
            requested_vectors = (sampled_vectors * ((total_requests // sample_size) + 1))[:total_requests]
            pipelines = [pipeline_fn(v, num_candidates, limit) for v in requested_vectors]
            
            # Thread synchronization
            durations_queue = queue.Queue()
            error_count = 0
            error_lock = Lock()
            
            # Rate limiting setup
            enable_rate_limiting = rpm < 10000  # Only rate limit for lower RPMs
            request_interval = 60.0 / rpm if enable_rate_limiting else 0
            start_time = time.perf_counter()
            next_request_time = start_time
            next_time_lock = Lock()
            
            # Monitor thread
            self.shutdown_event.clear()
            
            def process_pipeline(pipeline_idx, pipeline):
                """Worker function to process a single pipeline with rate limiting"""
                nonlocal error_count, next_request_time
                
                # Check for shutdown signal
                if self.shutdown_event.is_set():
                    return "Shutdown requested"
                
                # Rate limiting
                if enable_rate_limiting:
                    with next_time_lock:
                        scheduled_time = next_request_time
                        next_request_time = scheduled_time + request_interval
                    
                    wait_time = scheduled_time - time.perf_counter()
                    if wait_time > 0:
                        time.sleep(wait_time)
                
                # Run the query
                try:
                    duration = run_pipeline_threading(pipeline, self.db_name, self.coll_name)
                    if duration is not None:
                        durations_queue.put(duration)
                    else:
                        with error_lock:
                            error_count += 1
                        return f"Error on request {pipeline_idx}: Duration is None"
                    return None
                except Exception as e:
                    with error_lock:
                        error_count += 1
                    return f"Error on request {pipeline_idx}: {str(e)}"
            
            # Progress tracking
            total_count = len(pipelines)
            completed_count = 0
            last_progress_time = time.time()
            progress_interval = 5  # seconds between progress updates
            
            def progress_monitor():
                """Monitor and report progress periodically"""
                nonlocal completed_count, last_progress_time
                
                while not self.shutdown_event.is_set() and completed_count < total_count:
                    current_time = time.time()
                    if current_time - last_progress_time >= progress_interval:
                        progress_pct = (completed_count / total_count) * 100
                        print(f"Progress: {completed_count}/{total_count} ({progress_pct:.1f}%)")
                        last_progress_time = current_time
                    time.sleep(1)
            
            # Start progress monitor
            monitor_thread = threading.Thread(target=progress_monitor)
            monitor_thread.daemon = True
            monitor_thread.start()
            
            # Use ThreadPoolExecutor for better thread management
            print(f"Starting benchmark with {thread_count} threads, targeting {rpm} RPM...")
            errors = []
            
            completed_count = 0
            with ThreadPoolExecutor(max_workers=thread_count) as executor:
                # Submit all tasks
                future_to_idx = {executor.submit(process_pipeline, i, p): i for i, p in enumerate(pipelines)}
                
                # Process results as they complete
                for future in as_completed(future_to_idx):
                    completed_count += 1
                    error = future.result()
                    if error:
                        errors.append(error)
                        if len(errors) <= 5:  # Limit error reporting
                            print(error)
                    
                    # Check for shutdown
                    if self.shutdown_event.is_set():
                        print("Shutdown detected, cancelling pending tasks...")
                        for f in future_to_idx:
                            if not f.done():
                                f.cancel()
                        break
            
            # Collect all durations from the queue
            durations = []
            while not durations_queue.empty():
                durations.append(durations_queue.get())
            
            # Calculate statistics
            if durations:
                # Calculate percentiles
                percentiles = {p: statistics.quantiles(durations, n=100)[p - 1] for p in [50, 70, 95, 99]}
                
                # Calculate actual RPM
                execution_time_minutes = (time.perf_counter() - start_time) / 60
                actual_rpm = len(durations) / (execution_time_minutes if execution_time_minutes > 0 else DURATION_IN_MINUTES)
                
                # Print results
                print("\n" + "─" * 55)
                print(f"Pipeline:         {pipeline_name}")
                print(f"RPM Target:       {rpm}")
                print(f"RPM Actual:       {actual_rpm:.2f}")
                print(f"Threads:          {thread_count}")
                print(f"Sample size:      {sample_size}")
                print(f"Completed:        {len(durations)}/{total_requests} ({len(durations)/total_requests*100:.1f}%)")
                print(f"Errors:           {error_count}")
                print(f"NumCandidates:    {num_candidates}")
                print(f"Limit:            {limit}")
                print(f"Execution time:   {execution_time_minutes:.2f} minutes")
                
                for p, val in percentiles.items():
                    print(f"P{p} latency:      {val:.2f} ms")
                
                # Save results for final table
                with results_lock:  # Thread-safe access to results_summary
                    results_summary.append({
                        "pipeline": pipeline_name,
                        "num_candidates": num_candidates,
                        "TopK": limit,
                        "rpm": rpm,
                        "actual_rpm": round(actual_rpm, 1),
                        "threads": thread_count,
                        "sample": sample_size,
                        "requests": total_requests,
                        "completed": len(durations),
                        "errors": error_count,
                        "p50": percentiles[50],
                        "p70": percentiles[70],
                        "p95": percentiles[95],
                        "p99": percentiles[99]
                    })
            else:
                print(f"ERROR: No successful requests completed for {pipeline_name}")
                
        except Exception as e:
            print(f"Error in benchmark_all_models: {e}")
            traceback.print_exc()
        finally:
            self.running = False

def print_final_table():
    """
    Print a formatted table with all benchmark results
    """
    if not results_summary:
        print("No results to display.")
        return
    
    print("\nFINAL SUMMARY TABLE:")
    print(f"Host CPU count: {os.cpu_count()}")
    print("=" * 170)
    print(f"{'Pipeline':<30} {'RPM Target':<10} {'RPM Actual':<10} {'Threads':<8} {'Sample':<8} {'Reqs':<8} {'Completed':<10} {'Errors':<8} {'NumCand':<9} {'TopK':<8} {'P50(ms)':<10} {'P70(ms)':<10} {'P95(ms)':<10} {'P99(ms)':<10}")
    print("=" * 170)
    
    for r in results_summary:
        # Get completed percentage
        completed_pct = (r['completed'] / r['requests']) * 100 if r['requests'] > 0 else 0
        
        print(f"{r['pipeline']:<30} {r['rpm']:<10} {r.get('actual_rpm', 0):<10.1f} {r['threads']:<8} {r['sample']:<8} {r['requests']:<8} {r['completed']:<6}({completed_pct:.1f}%) {r.get('errors', 0):<8} {r['num_candidates']:<9} {r['TopK']:<8} {r['p50']:<10.2f} {r['p70']:<10.2f} {r['p95']:<10.2f} {r['p99']:<10.2f}")
    
    print("=" * 170)
    print("Visualize here : https://saiteja05.github.io/datalog/performance_visualization.html")


if __name__ == '__main__':
    vectors = get_vectors()
    uri = os.getenv('MONGODB_URI', "mongodb+srv://locust:locust@cluster0.tcgzn.mongodb.net/?retryWrites=true&w=majority&readPreference=nearest&appName=Cluster0")
    db_name = os.getenv('MONGODB_DB', "ecommerce")
    coll_name = os.getenv('MONGODB_COLL', "catalog")

    # Initialize MongoDB client once globally
    if not init_mongo_client(uri):
        print("Exiting due to MongoDB connection failure")
        sys.exit(1)

    if not vectors:
        print("No vectors loaded. Exiting.")
        sys.exit(1)

    try:
        print(f"Starting benchmarks with vectors loaded")
        
        # Configure the tests
        rpms =[
# 100,
            # 1000,
             10000, 
             50000, 
             100000,
            #  200000
            ]
        
        # Choose which pipelines to benchmark
        # PIPELINES = [
        #     ("functionalHybrid", functionalHybrid_pipeline), 
        #     ("VectorSearchOnly", vectoronly_pipeline)
        # ]
        
        PIPELINES = [
                    #  ("fullNoFilterPipeline",no_filter_pipeline),
                    ("nativeTextFilterPipeline",new_index_staticfilter_pipeline2),
                     ("nativeTextFilterPipeline-static",new_index_staticfilter_pipeline),
                     ("nativeTextFilterPipeline-dynamic",new_index_dynamicfilter_pipeline)
                    #   ,("functionalHybrid", functionalHybrid_pipeline)
                     ]

        # Define thread counts - dynamic based on CPU count
        cpu_count = os.cpu_count()
        thread_counts = [cpu_count
                        #   , cpu_count*2
                        #  , cpu_count*3
                        #  , cpu_count*4
                        #  , cpu_count*8
                         ]
        
        # Define candidate counts and limits
        num_candidates_list = [(100, 100), (300, 100), (500, 100), (500, 250), (500, 500)]
        
        # Create the benchmark manager
        benchmark_mgr = BenchmarkManager(vectors, db_name, coll_name)
        
        for rpm in rpms:
            for pipeline_name, pipeline_fn in PIPELINES:
                for threads in thread_counts:
                    # Only run if we have enough threads for the RPM
                    if rpm / 2 > threads:
                        for nc, limit in num_candidates_list:
                            # Dynamically adjust sample size for larger workloads
                            factor = int((float(rpm) * 0.1) % 500)
                            current_sample_size = factor if factor > 50 else SAMPLE_SIZE
                            
                            # Update the global variable (thread-safe since we're still in setup)
                            SAMPLE_SIZE = current_sample_size
                            
                            print(f"\n[INFO] Starting benchmark of {pipeline_name}")
                            print(f"       Threads: {threads}")
                            print(f"       Target RPM: {rpm}")
                            print(f"       Sample size: {SAMPLE_SIZE}")
                            print(f"       Num candidates: {nc}")
                            print(f"       Limit: {limit}")
                            
                            # Run the benchmark
                            benchmark_mgr.benchmark_all_models(
                                threads, pipeline_name, pipeline_fn, rpm, nc, limit
                            )

        # Print the final summary table
        print_final_table()

    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user. Generating report with completed tests...")
        print_final_table()
    except Exception as e:
        print(f"Error during benchmark execution: {e}")
        traceback.print_exc()
    finally:
        if mongo_client:
            mongo_client.close()
            print("MongoDB connection closed")
