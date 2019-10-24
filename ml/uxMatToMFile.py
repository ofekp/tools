import json
import numpy
import re
import sys

if len(sys.argv) == 1:
    model_file_path = "/Users/ofek/PCA_CF/compareToDva/modelFile_main_201811060700_low.json"
else:
    model_file_path = sys.argv[1]

def extract_ux_matrix(model_file_path):
    with open(model_file_path, 'r') as model_file:
        line_list = []
        ok_to_write = False
        for line in model_file:
            if "\"UX\":" in line:
                ok_to_write = True
                #line_list.append("{")
            elif "\"user\":" in line:
                line_list.pop()  # an unneeded line was already inserted so we're removing it here
                #del line_list[len(line_list) - 2]  # remove a ','
                #line_list.append("}")
                ret = ''.join(line_list)
                return ''.join(ret)
            else:
                if ok_to_write:
                    line_list.append(line)
                    # if '[' in line and ']' in line:
                    #     line_list.append(',')
    return None


def save_matrix_as_m_file(matrix, model_file_path):
    split_path = model_file_path.split(".")
    ux_matrix_m_file_path = split_path[0] + "_ux_matrix.m"
    open(ux_matrix_m_file_path, 'w').close()  # delete file content
    with open(ux_matrix_m_file_path, 'a') as ux_matrix_m_file:
        count = 0
        for vector in matrix:
            print count
            count += 1
            vector = numpy.array2string(numpy.array(vector), max_line_width=numpy.inf, separator=' ', suppress_small=True, precision=12)
            vector = ' '.join(vector.split())
            vector = re.sub(r'^\[|\]$', '', vector)
            ux_matrix_m_file.write(vector + "\n")

json_string = extract_ux_matrix(model_file_path)
json_string = json_string.replace("\n", "")
user_features = json.loads(json_string)
save_matrix_as_m_file(user_features, model_file_path)