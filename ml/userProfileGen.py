import json
import os.path
import numpy  # efficient vector arithmetic
import re
import random


model_file_path = "/Users/ofek/PCA_CF/compareToDva/modelFile_main_201811060700_dva.json"
# the user_features.json file was generated using the following command:
# cat lfmModel12_201809010945.json.gz | gzip -cd | grep "\"creative_id\.[0-9]*\":\[" | sed -E -e "s/.*\"creative_id\.([0-9]*)\":\[(.*)\].*/\1 \2/g" | tr ',' ' ' > lfmModel12_201809010945.m
user_features_file_path = "/Users/ofek/PCA_CF/machine-learning-ex8/ex8/work/lfmModel12_201809010945_user_features.json"
#user_features_with_neutral_file_path = "/Users/ofek/PCA_CF/machine-learning-ex8/ex8/work/lfmModel12_201809010945_user_features_with_neutral.m"
#user_features_neutral_file_path = "/Users/ofek/PCA_CF/machine-learning-ex8/ex8/work/lfmModel12_201809010945_user_features_neutral.json"
user_vectors_file_path = "/Users/ofek/PCA_CF/machine-learning-ex8/ex8/work/lfmModel12_201809010945_user_vectors_4_210.m"

# section, recency, user_age, user_gender, state_woeid, device_type_id, delivered_match_type, relevanceScore, advertiser_acct_id, sonFrequencyCapAdvertiserDaily, source, external.userCategory, yic_mv
features_map = {
    "sectionId": "5498942",
    "day": "2",
    "age": "1988",
    "gender": "l",
    "state": "2347588",
}

# "externalFeature"
exp_feature = ["exp1_userCategory_914", "exp5_allMappi_NEUTRAL", "exp1_userCategory_818", "exp1_userCategory_906", "exp1_userCategory_884", "exp5_allMappi_UNKNOWN", "exp1_userCategory_NEUTRAL", "exp1_userCategory_866"]
# "yct_c_mv_w"
yct_feature = ["001000070", "001001186", "001001161", "001000012", "001000673", "001000297", "001001127", "001000798", "001001128", "001000358", "001000930", "001001126", "_NEUTRAL_", "001001076", "001001153", "001000001", "001000288", "001001157", "001000643", "001000644", "001000028", "001000721", "001000862", "001000106", "001000007"]


def get_user_features_string():
    if os.path.isfile(user_features_file_path):
        with open(user_features_file_path, 'r') as user_features_file:
            return user_features_file.read().replace('\n', '')

    with open(model_file_path, 'r') as model_file:
        line_list = []
        ok_to_write = False
        for line in model_file:
            if "\"user\":" in line:
                line_list.append("{")
                ok_to_write = True
            elif "\"ad\":" in line:
                line_list.pop()
                line_list.append("}")
                ret = ''.join(line_list)
                with open(user_features_file_path, 'w') as user_features_file:
                    user_features_file.write(ret)
                return ''.join(ret)
            else:
                if ok_to_write:
                    line_list.append(line)


def coin_toss(p=.5):
    return True if random.random() < p else False


def getUserFeaturesVector(user_features):
    feature_vector = numpy.ones(970)
    for feature_name in user_features:
        feature = user_features[feature_name]
        # if the feature contains any label with "NEUTRAL" skip it
        if len([p for p in feature.keys() if "NEUTRAL" in p]) > 0:
            continue;
        rand_index = random.randint(0, len(feature.keys()) - 1)
        chosen_label = list(feature)[rand_index]
        print "feature_name [" + feature_name + "] label [" + chosen_label + "]"
        feature_vector = numpy.multiply(feature_vector, feature[chosen_label])

    #print numpy.array2string(feature_vector, max_line_width=numpy.inf, separator=' ',suppress_small=True)
    return feature_vector


# for features that are added together before multiplication
def getListUserFeatureVector(user_features, feature_name, sub_feature_name = None, maxNumOfLabels = 100):
    considered_labels = []
    list_feature_vector = numpy.zeros(970)
    list_feature = user_features[feature_name]
    if len(list_feature) <= 0:
        raise Exception("Error with feature_name [" + feature_name + "]")
    if sub_feature_name is not None:
        # filter out any label that does not contain the sub_feature_name
        list_feature = { k: v for k, v in list_feature.items() if sub_feature_name in k }
        if len(list_feature) <= 0:
            raise Exception("Error with feature_name [" + feature_name + "] and sub_feature_name [" + sub_feature_name + "]")

    # add NEUTRAL vector
    neutral_label_map = { k: v for k, v in list_feature.items() if "NEUTRAL" in k }
    if len(neutral_label_map) is not 1:
        raise Exception("Error with feature_name [" + feature_name + "] and sub_feature_name [" + sub_feature_name + "]")
    neutral_label = neutral_label_map.iterkeys().next()
    considered_labels.append(neutral_label)
    list_feature_vector = numpy.add(list_feature_vector, numpy.array(list_feature[neutral_label]))
    # remove neutral label from list_feature
    list_feature = { k: v for k, v in list_feature.items() if "NEUTRAL" not in k }

    # choose UNKNOWN
    if coin_toss(0.05):
        # in this case only UNKNOWN (and NEUTRAL) should be picked
        unknown_label_map = { k: v for k, v in list_feature.items() if "UNKNOWN" in k }
        unknown_label = unknown_label_map.iterkeys().next()
        considered_labels.append(unknown_label)
        list_feature_vector = numpy.add(list_feature_vector, numpy.array(list_feature[unknown_label]))
        print "feature_name [" + feature_name + "] sub_feature_name [" + str(sub_feature_name) + "] labels " + str(considered_labels)
        #print numpy.array2string(list_feature_vector, max_line_width=numpy.inf, separator=' ',suppress_small=True)
        return list_feature_vector

    # remove UNKNOWN label from list_feature
    list_feature = { k: v for k, v in list_feature.items() if "UNKNOWN" not in k }

    # choose max maxNumOfLabels labels from list_feature
    labels = random.sample(list(list_feature), random.randint(1, min(len(list_feature), maxNumOfLabels)))
    considered_labels.append(labels)
    vector_values = [list_feature[label] for label in labels]

    for vector_value in vector_values:
        list_feature_vector = numpy.add(list_feature_vector, numpy.array(vector_value))

    print "feature_name [" + feature_name + "] sub_feature_name [" + str(sub_feature_name) + "] labels " + str(considered_labels)
    #print numpy.array2string(list_feature_vector, max_line_width=numpy.inf, separator=' ',suppress_small=True)
    return list_feature_vector


def generateUserVector():
    user_vector = numpy.ones(970)
    user_features = json.loads(get_user_features_string())

    user_vector = numpy.multiply(user_vector, getUserFeaturesVector(user_features))
    user_vector = numpy.multiply(user_vector, getListUserFeatureVector(user_features, "externalFeature", "exp5_allMappi"))
    user_vector = numpy.multiply(user_vector, getListUserFeatureVector(user_features, "externalFeature", "exp1_userCategory"))
    user_vector = numpy.multiply(user_vector, getListUserFeatureVector(user_features, "yic_mv"))
    user_vector = numpy.multiply(user_vector, getListUserFeatureVector(user_features, "yct_c_mv_w"))
    user_vector = numpy.multiply(user_vector, getListUserFeatureVector(user_features, "userSignal"))

    return user_vector


#open(user_vectors_file_path, 'w').close()  # delete file content
with open(user_vectors_file_path, 'a') as user_vector_file:
    for i in range(210):
        print "Generating user vector [" + str(i) + "]"
        user_vector = generateUserVector()
        user_vector = numpy.array2string(user_vector, max_line_width=numpy.inf, separator=' ',suppress_small=True)
        user_vector = ' '.join(user_vector.split())
        user_vector = re.sub(r'^\[|\]$', '', user_vector)
        user_vector_file.write(user_vector + "\n")
        print "\n"


# for feature in features_map:
#     feature_value = "_UNKNOWN_"
#     if features_map[feature] in user_features[feature]:
#         feature_value = features_map[feature]
#
#     vec = user_features[feature][feature_value]
#     user_feature_vector *= numpy.array(vec)
#
#     #print "Feature [" + feature + "] with value [" + feature_value + "]"
#     #print vec
#
# # "externalFeature"
# external_feature_vector = numpy.zeros(970)
# external_feature = user_features["externalFeature"]
# for feature in exp_feature:
#     #print "Will add for externalFeature [" + feature + "]"
#     #print external_feature[feature]
#     external_feature_vector += numpy.array(external_feature[feature])
# #print "externalFeature"
# #print external_feature_vector
#
# # "yct_c_mv_w"
# yct_feature_vector = numpy.zeros(970)
# yct_feature = user_features["yct_c_mv_w"]
# for feature in yct_feature:
#     #print "Will add for yct_c_mv_w [" + feature + "]"
#     #print yct_feature[feature]
#     yct_feature_vector += numpy.array(yct_feature[feature])
# #print "yct_c_mv_w"
# #print yct_feature_vector
#
# user_feature_vector *= external_feature_vector;
# user_feature_vector *= yct_feature_vector;


#print user_feature_vector




