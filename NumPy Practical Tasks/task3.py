import numpy as np
from task1 import print_array


def create_array(size=(6, 6)):
    """
    Generate a multi-dimensional NumPy array with random values.
    :param size: Size of the created array
    :return: array
    """
    np.random.seed(777)
    return np.random.randint(100, size=size)


def transpose_array(array):
    """
    Transpose array
    :param array: input array
    :return: transposed array
    """
    return array.T


def reshape_array(array, new_shape=(3, 12)):
    """
    Reshape array with given shape
    :param array: Input array
    :param new_shape: New array shape
    :return: Array in new shape
    """
    return array.reshape(new_shape)


def split_array(array, num_splits=6, axis=0):
    """
    Splits the array into multiple sub-arrays along a specified axis
    :param array: Input array
    :param num_splits: number of splits
    :param axis: Axis on which array will be splitted
    :return: Splited sub-arrays
    """
    return np.array_split(array, num_splits, axis=axis)


def combine_array(array, axis=0):
    """
    Combine splited arrays along axis
    :param array: Input array
    :param axis: Axis to combine
    :return: Combined array
    """
    return np.concatenate(array, axis=axis)


if __name__ == '__main__':
    # Create array with shape [6, 6]
    array = create_array()
    print_array(array, "A multi-dimensional NumPy array with random values:", addition_info=True)

    # Transpose
    transposed_array = transpose_array(array)
    print_array(transposed_array, "Transposed original array:")

    # Reshape
    reshaped_array = reshape_array(array, new_shape=(3, 12))
    print_array(reshaped_array, "Reshaped array:", addition_info=True)

    # Split array
    splited_array = split_array(array)
    print_array(splited_array, "Splitted array:")

    # Combined array from splitted
    combined_array = combine_array(splited_array, axis=0)
    print_array(combined_array, "Combined array after spliting:")