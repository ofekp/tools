import re
import numpy
import json
import time
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import pandas as pd

main_folder = "/Users/ofek/PCA_CF/compareToDva/"
model_file_path = main_folder + "modelFile_main_201811060700.json"
model_low_file_path = main_folder + "modelFile_main_201811060700_low.json"
# choose the number of values per ad in the new model
K = 668

tmp_pca_matrix_file_path = main_folder + "tmp_pca_matrix.tmp"
tmp_low_matrix_file_path = main_folder + "tmp_low_matrix.tmp"

ad_vec_pattern = re.compile(r".*\"creative_id\.([0-9]*)\":\[(.*)\].*")  # for ad vectors count
ad_vec_sub_pattern = re.compile(r"(.*\"creative_id\.[0-9]*\":\[).*(\].*)")  # for vector substitution

# **************************************
# code for building label vectors matrix
# ======================================

def get_user_features(model_file_path):
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
                return json.loads(''.join(ret))
            else:
                if ok_to_write:
                    line_list.append(line)
    return None


def getUserFeaturesVector(label_vectors, row_idx, user_features):
    labels_count = 0
    for feature_name in user_features:
        label_count = 0
        feature = user_features[feature_name]
        # if the feature contains any label with "NEUTRAL" skip it, it will be added later
        if len([p for p in feature.keys() if "NEUTRAL" in p]) > 0:
            continue
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


def generate_label_vectors_matrix(model_file_path):
    user_features = get_user_features(model_file_path)
    max_num_of_rows = numpy.sum([len(user_features[feature]) for feature in user_features])
    print("max_num_of_rows [" + str(max_num_of_rows) + "]")
    # count how many NEUTRAL vectors there are
    num_of_neutral_vectors = 0
    for feature_name in user_features:
        feature = user_features[feature_name]
        num_of_neutral_vectors += len([p for p in feature.keys() if "NEUTRAL" in p])
    label_vectors_matrix = numpy.zeros(shape=(max_num_of_rows - num_of_neutral_vectors, 970))

    row_idx = 0
    row_idx += getUserFeaturesVector(label_vectors_matrix, row_idx, user_features)
    row_idx += getListUserFeatureVector(label_vectors_matrix, row_idx, user_features, "externalFeature", "exp5_allMappi")
    row_idx += getListUserFeatureVector(label_vectors_matrix, row_idx, user_features, "externalFeature", "exp1_userCategory")
    row_idx += getListUserFeatureVector(label_vectors_matrix, row_idx, user_features, "yic_mv")
    row_idx += getListUserFeatureVector(label_vectors_matrix, row_idx, user_features, "yct_c_mv_w")
    row_idx += getListUserFeatureVector(label_vectors_matrix, row_idx, user_features, "userSignal")

    return label_vectors_matrix


# ********************************************
# code for building label vectors matrix (end)
# ============================================

start_time_sec = int(time.time())

# get number of ads in the model
num_of_ads = 0
with open(model_file_path, 'r') as model_file:
    for line in model_file:
        if ad_vec_pattern.match(line):
            num_of_ads += 1

print("ad num [" + str(num_of_ads) + "]")

# prepare the matrix with all the ad vectors in rows
X = numpy.zeros(shape=(num_of_ads, 970))
with open(model_file_path, 'r') as model_file:
    ad_idx = 0
    for line in model_file:
        m = ad_vec_pattern.match(line)
        if m:
            creative_id = m.group(1)  # currently not in use
            X[ad_idx] = numpy.fromstring(m.group(2), sep=",")
            ad_idx += 1

assert ad_idx == num_of_ads
assert numpy.sum(X[:,968]) == 0  # all values in col are 0
assert numpy.sum(X[:,969]) == X.shape[0]  # all values in col are 1

# generate the label vectors
label_vectors_matrix = generate_label_vectors_matrix(model_file_path)

# the last col is the label vector bias and is equal in all label vectors
# argmax - returns the index of the maximum value along the axis
# the bias is taken from one of the "day" user feature vector (any of them)
user_vector_bias = label_vectors_matrix[(label_vectors_matrix[:, 969] != 1.0).argmax(axis=0), 969]

# all values in col are user_vector_bias
# 8 amounts to the number of "day" feature labels - (1-7) + 1 UNKNOWN label. located here label_vectors_matrix[15460:15470,:]
# Hifa choose to add the bias in the "day" feature. Since it is only in one feature, any user vector that is generated will have
# the bias in the last value of the vector.
assert numpy.sum(label_vectors_matrix[:,969] != 1.0) == 8

# calculate PCA
# 1) append the matrices
X_for_pca = numpy.concatenate((X, label_vectors_matrix), axis=0)

assert X_for_pca.shape[1] == 970

# 2) remove the last two cols which contain 0 and 1 for bias
X_for_pca = numpy.delete(X_for_pca, 969, 1)  # all ones
X_for_pca = numpy.delete(X_for_pca, 968, 1)  # all zeros

# 3) calculate the covariance matrix Sigma
Sigma = numpy.matmul(numpy.transpose(X_for_pca), X_for_pca) / X_for_pca.shape[0]  # (1/|rows in X|) * (X' * X)
UX, SX, _ = numpy.linalg.svd(Sigma)  # [U S V] = svd(Sigma);

assert UX.shape[0] == UX.shape[1] and UX.shape[0] == 968



# use PCA UX matrix to reduce the dimension of the ad vectors
# calculate preserved variance percentage for declaration in the model file
preserved_variance_pct = (numpy.sum(SX[0:K]) * 100.0) / numpy.sum(SX)
# reduce the dimension of the ad vectors
X_low = numpy.matmul(X[:, 0:968], UX[:, 0:K])  # Z = X * U(:, 1:K);

assert X_low.shape[1] == K
assert X_low.shape[0] == X.shape[0]

# add a ones col for user vector bias addition
X_low = numpy.concatenate((X_low, numpy.ones((X_low.shape[0], 1))), axis=1)

assert X_low.shape[1] == (K + 1)
assert X_low.shape[0] == X.shape[0]
assert X_low.shape[0] == num_of_ads

# **********************************
# write the new model to a json file
# ==================================

# copy all the lines in the file
open(model_low_file_path, 'w').close()  # delete output file content
open(tmp_pca_matrix_file_path, 'w').close()  # delete the content of the tmp pca matrix file
open(tmp_low_matrix_file_path, 'w').close()  # delete the content of the tmp X_low matrix file
with open(model_file_path, 'r') as model_file, open(model_low_file_path, 'a') as model_low_file:
    # pca data is inserted before the feature vectors section starts
    while True:
        line = model_file.readline()
        if "\"user\":{\n" in line:
            break
        model_low_file.write(line)
    model_low_file.write("\"pca\":{\n")
    model_low_file.write("\"usedVecDim\": \"" + str(K) + "\",\n")
    model_low_file.write("\"preservedVariancePct\": \"" + str(preserved_variance_pct) + "\",\n")
    model_low_file.write("\"userVectorBias\": \"" + str(user_vector_bias) + "\",\n")
    model_low_file.write("\"UX\": \n")
    with open(tmp_pca_matrix_file_path, 'w') as tmp_pca_matrix_file:
        numpy.savetxt(tmp_pca_matrix_file, UX, delimiter=",", fmt="%1.6E")
    model_low_file.write("[\n")
    with open(tmp_pca_matrix_file_path, 'r') as tmp_pca_matrix_file:
        count = 0
        for pca_matrix_row in tmp_pca_matrix_file:
            count += 1
            model_low_file.write("[" + pca_matrix_row.strip().replace("E-0", "E-").replace("E+0", "E+") + "]" + (",\n" if count < 968 else "\n"))
    # delete the tmp file
    open(tmp_pca_matrix_file_path, 'w').close()
    model_low_file.write("]\n")
    model_low_file.write("},\n")  # close for pca section

    # feature vectors remain as they are (original dimension of 970)
    model_low_file.write("\"user\":{\n")
    while True:
        line = model_file.readline()
        model_low_file.write(line)
        if "\"ad\":" in line:
            break

    # write all the new lower dimension creatives' vectors
    # in bash: `cat lfmModel12_201809010945.json.gz | gzip -cd | grep "\"creative_id\.[0-9]*\":\[" | sed -E -e "s/.*\"creative_id\.([0-9]*)\":\[(.*)\].*/\1 \2/g" | tr ',' ' ' > lfmModel12_201809010945.m`

    with open(tmp_low_matrix_file_path, 'w') as tmp_low_matrix_file:
        numpy.savetxt(tmp_low_matrix_file, X_low, delimiter=",", fmt="%1.6E")
    with open(tmp_low_matrix_file_path, 'r') as tmp_low_matrix_file:
        ad_idx = 0
        while True:
            line = model_file.readline()
            if not line:
                assert not tmp_low_matrix_file.readline()
                break
            m = ad_vec_sub_pattern.match(line)
            if m:
                X_low_row = tmp_low_matrix_file.readline()
                X_low_row = X_low_row.replace('\n', '').replace(' ', '').replace("E-0", "E-").replace("E+0", "E+")
                new_ad_vector_line = m.group(1) + X_low_row + m.group(2) + "\n"
                # TODO: sanity, make sure the line is correct using the `ad_vec_pattern` regex
                ms = ad_vec_pattern.match(new_ad_vector_line)
                if ms:
                    ad_idx += 1
                    model_low_file.write(new_ad_vector_line)
                else:
                    print "ERROR with line [" + new_ad_vector_line + "]"
            else:
                model_low_file.write(line)

    # delete the tmp file
    open(tmp_low_matrix_file_path, 'w').close()

print "Done in [" + str(int(time.time()) - start_time_sec) + "] sec"
print "Number of written ads/number of ads in original model [" + str(ad_idx) + "/" + str(num_of_ads) + "]"
