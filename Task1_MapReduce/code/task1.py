
import pandas as pd
import numpy as np


# It will take the input csv file result
# Return the total cost formula array and its length
def Map(datingTest):
    x = np.array(datingTest)
    # print(x)
    k = 0
    h = 0
    index = []
    index1 = []
    string = []
    s1 = "unit price of residence space"
    s2 = "residence space"
    s3 = "unit price of building space"
    s4 = "building space"
    s5 = "exchange rate"
    length_of_formula = 5

    # Read and match s1, s2 ... s5 from datingTest and record its index
    for i in datingTest:
        if i == s1:
            index.append(h)
            string.append(i)
            k += 1
        elif i == s2:
            index.append(h)
            string.append(i)
            k += 1
        elif i == s3:
            index.append(h)
            string.append(i)
            k += 1
        elif i == s4:
            index.append(h)
            string.append(i)
            k += 1
        elif i == s5:
            index.append(h)
            string.append(i)
            k += 1
        elif len(index) == length_of_formula:
            break
        h += 1

    # Sort the index array, in order to fit the total cost formula order, result in index1
    k = 0
    while (len(index1) != length_of_formula):
        h = 0
        for j in string:
            if j == s1 and k == 0:
                index1.append(index[h])
                k += 1
            elif j == s2 and k == 1:
                index1.append(index[h])
                k += 1
            elif j == s3 and k == 2:
                index1.append(index[h])
                k += 1
            elif j == s4 and k == 3:
                index1.append(index[h])
                k += 1
            elif j == s5 and k == 4:
                index1.append(index[h])
                k += 1
            h += 1
    # print(index1)

    # Construct the total cost formula array
    # out = [serial number, unit price of residence space, residence space, unit price of building space,
    # building space, exchange rate]
    serial_number = 1
    length_of_output_array = 6
    out = np.zeros(shape=(len(x), length_of_output_array))
    for i in range(len(x)):
        for j in range(length_of_output_array):
            if j == 0:
                out[i][j] = serial_number  # serial number
                serial_number += 1
            elif j == 1:
                out[i][j] = x[i][index1[0]]  # unit price of residence space
            elif j == 2:
                out[i][j] = x[i][index1[1]]  # residence space
            elif j == 3:
                out[i][j] = x[i][index1[2]]  # unit price of building space
            elif j == 4:
                out[i][j] = x[i][index1[3]]  # building space
            elif j == 5:
                out[i][j] = x[i][index1[4]]  # exchange rate

    # print(out)
    return out


# It will take the result array from the map
# Return the total cost array out = [serial number, total cost]
def Reduce(arr):
    length_of_output = 2
    out = np.zeros(shape=(len(arr), length_of_output))
    for i in range(len(arr)):
        # total cost = (unit price of residence space * residence space + unit price of building space * building space)
        # * exchange rate
        out[i][0] = arr[i][0]  # serial number
        out[i][1] = (arr[i][1] * arr[i][2] + arr[i][3] * arr[i][4]) * arr[i][5]  # total cost
    # print(out)
    return out

# Combine each output from the map, and split it into multiple parts based on the number of reducers.
def shuffle(*arrays):
    array = arrays[0]
    for i in arrays[1:]:
        array = np.concatenate((array, i))
    # recalculate the serial number
    serial_number = 1
    for i in range(len(array)):
        array[i][0] = serial_number
        serial_number += 1
    arr1, arr2 = split_array(array, num_of_reducer=2)
    return arr1, arr2


# Split an array into half
def split_array(array, num_of_reducer=2):
    arr1, arr2 = np.split(array, num_of_reducer)
    return arr1, arr2


# Fill the total cost blank in original data
def add_total_cost_to_train_data(reduce_result, final_result):

    for i in range(len(final_result)):
        final_result.loc[i, 'total cost'] = np.around(reduce_result[i][1], decimals=2)

    return final_result


def main():
    # Read the origin data and five map inputs
    datingTest = pd.read_csv('MapReduce/data/Train_Data.csv')
    datingTest1 = pd.read_csv('MapReduce/data/data1.csv')
    datingTest2 = pd.read_csv('MapReduce/data/data2.csv')
    datingTest3 = pd.read_csv('MapReduce/data/data3.csv')
    datingTest4 = pd.read_csv('MapReduce/data/data4.csv')
    datingTest5 = pd.read_csv('MapReduce/data/data5.csv')

    # Assign each input to each mapper
    map_result1 = Map(datingTest1)
    map_result2 = Map(datingTest2)
    map_result3 = Map(datingTest3)
    map_result4 = Map(datingTest4)
    map_result5 = Map(datingTest5)

    # Combine the results from the map, split, and assign to each reducer
    reduce_input1, reduce_input2 = shuffle(map_result1, map_result2, map_result3, map_result4, map_result5)
    reduce_result1 = Reduce(reduce_input1)
    reduce_result2 = Reduce(reduce_input2)
    # print(reduce_result1, "\n")
    # print(reduce_result2, "\n")

    # Combine the result from each reducer
    combined_reduce_result = np.concatenate((reduce_result1, reduce_result2))

    # Put the total cost into the origin data, and save it as a new CSV file
    datingTest = add_total_cost_to_train_data(combined_reduce_result, datingTest)
    datingTest.to_csv("data/Train_Data1.csv")
    print(combined_reduce_result, "\n")
    print(datingTest)


if __name__ == "__main__":
    main()

