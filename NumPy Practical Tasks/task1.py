import numpy as np


def print_array(array, message='', addition_info=False):
    """
    Output function that prints array to console
    :param array: array to print
    :param message: message to show before array printing
    :param addition_info:  Checkmark to show shape and number of dimension to array
    """
    print()
    print(message)
    print(array)
    if addition_info:
        print(f"Shape of array: {array.shape}\n"
              f"Number of dimensions: {array.ndim}")


def create_one_dim():
    """
    Create one-dimensional array with values ranging from 1 to 10.
    :return: array(10)
    """
    return np.arange(1, 11)


def create_two_dim():
    """
    Create two-dimensional NumPy array (matrix) with shape (3, 3) containing values from 1 to 9.
    :return: array(3, 3)
    """
    return np.arange(1, 10).reshape(3, 3)


if __name__ == "__main__":
    # array creation
    one_dim_array = create_one_dim()
    print_array(one_dim_array, "Original one-dim array:", addition_info=True)

    two_dim_array = create_two_dim()
    print_array(two_dim_array, "Original two-dim array:", addition_info=True)

    # array indexing and slicing
    print_array(one_dim_array[2], "The third element of the one-dimensional array:")
    print_array(two_dim_array[:2, :2], "First two rows and columns of the two-dimensional array:")

    # basic arithmetic
    one_dim_array += 5
    print_array(one_dim_array, "One-dimensional array after adding 5 to all elements:")

    two_dim_array *= 2
    print_array(two_dim_array, "Two-dimensional array after multiplying by 2:")