import json
import os.path
import numpy  # efficient vector arithmetic
import re


model_file_path = "/Users/ofek/PCA_CF/machine-learning-ex8/ex8/work/lfmModel12_201809010945.json"
# the user_features.json file was jenerated using the following command:
# cat lfmModel12_201809010945.json.gz | gzip -cd | grep "\"creative_id\.[0-9]*\":\[" | sed -E -e "s/.*\"creative_id\.([0-9]*)\":\[(.*)\].*/\1 \2/g" | tr ',' ' ' > lfmModel12_201809010945.m
user_features_file_path = "/Users/ofek/PCA_CF/machine-learning-ex8/ex8/work/lfmModel12_201809010945_user_features.json"
label_vectors_file_path = "/Users/ofek/PCA_CF/machine-learning-ex8/ex8/work/lfmModel12_201809010945_label_vectors.m"

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


def getUserFeaturesVector(label_vectors, row_idx, user_features):
    labels_count = 0
    for feature_name in user_features:
        label_count = 0
        feature = user_features[feature_name]
        # if the feature contains any label with "NEUTRAL" skip it, it will be added later
        if len([p for p in feature.keys() if "NEUTRAL" in p]) > 0:
            continue;
        for label in feature:
            label_vectors[row_idx] = numpy.array(feature[label]);
            row_idx += 1
            label_count += 1
        print "Done for feature_name [" + feature_name + "] label_count [" + str(label_count) + "]"
        labels_count += label_count
    return labels_count


# for features that are added together before multiplication
def getListUserFeatureVector(label_vectors, row_idx, user_features, feature_name, sub_feature_name = None):
    labels_count = 0
    list_feature = user_features[feature_name]
    if len(list_feature) <= 0:
        raise Exception("Error with feature_name [" + feature_name + "]")
    if sub_feature_name is not None:
        # filter out any label that does not contain the sub_feature_name
        list_feature = { k: v for k, v in list_feature.items() if sub_feature_name in k }
        if len(list_feature) <= 0:
            raise Exception("Error with feature_name [" + feature_name + "] and sub_feature_name [" + sub_feature_name + "]")

    # get NEUTRAL vector
    neutral_label_map = { k: v for k, v in list_feature.items() if "NEUTRAL" in k }
    if len(neutral_label_map) is not 1:
        raise Exception("Error with feature_name [" + feature_name + "] and sub_feature_name [" + sub_feature_name + "]")
    neutral_label = neutral_label_map.iterkeys().next()
    neutral_label_vec = numpy.array(neutral_label_map[neutral_label])
    # remove neutral label from list_feature
    list_feature = { k: v for k, v in list_feature.items() if "NEUTRAL" not in k }

    for label in list_feature:
        list_feature_vector = numpy.array(list_feature[label])
        list_feature_vector = numpy.add(list_feature_vector, neutral_label_vec)
        label_vectors[row_idx] = list_feature_vector
        row_idx += 1
        labels_count += 1

    print "Done for feature_name [" + feature_name + "] sub_feature_name [" + str(sub_feature_name) + "] labels_count [" + str(labels_count) + "]"
    return labels_count


def generateLabelVectors():
    user_features = json.loads(get_user_features_string())
    max_num_of_rows = sum([len(user_features[feature]) for feature in user_features])
    print("max_num_of_rows [" + str(max_num_of_rows) + "]")
    label_vectors = numpy.zeros(shape=(max_num_of_rows, 970))
    row_idx = 0

    row_idx += getUserFeaturesVector(label_vectors, row_idx, user_features)
    row_idx += getListUserFeatureVector(label_vectors, row_idx, user_features, "externalFeature", "exp5_allMappi")
    row_idx += getListUserFeatureVector(label_vectors, row_idx, user_features, "externalFeature", "exp1_userCategory")
    row_idx += getListUserFeatureVector(label_vectors, row_idx, user_features, "yic_mv")
    row_idx += getListUserFeatureVector(label_vectors, row_idx, user_features, "yct_c_mv_w")
    row_idx += getListUserFeatureVector(label_vectors, row_idx, user_features, "userSignal")

    return label_vectors


#open(user_vectors_file_path, 'w').close()  # delete file content
with open(label_vectors_file_path, 'w') as label_vectors_file:
    label_vectors = generateLabelVectors()
    for label_vector in label_vectors:
        label_vector_str = numpy.array2string(label_vector, max_line_width=numpy.inf, separator=' ',suppress_small=True)
        label_vector_str = ' '.join(label_vector_str.split())
        label_vector_str = re.sub(r'^\[|\]$', '', label_vector_str)
        label_vectors_file.write(label_vector_str + "\n")
