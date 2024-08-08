import numpy as np
from task1 import print_array


def create_array(size=(10, 10)):
    """
    Generate a multi-dimensional NumPy array with random values.
    :param size: Size of generated array
    :return: array
    """
    np.random.seed(187)
    return np.random.randint(100, size=size)


def save_array(array, filename='numpy_array', extension='npy'):
    """
    Save array to file
    :param array: Array to save
    :param filename: Name of file
    :param extension: Extension of file
    """
    if extension == 'txt':
        np.savetxt(f"{filename}.txt", array)
        print(f"[INFO] array saved to {filename}.txt")
    elif extension == 'csv':
        np.savetxt(f"{filename}.csv", array, delimiter=',')
        print(f"[INFO] array saved to {filename}.csv")
    elif extension == 'npy':
        np.save(f"{filename}.npy", array)
        print(f"[INFO] array saved to {filename}.npy")
    else:
        raise ValueError(f"Incorrect extension of file: {extension}. Chose one of: ['txt', 'csv', 'npy']")


def read_array(filename):
    """
    Read array from file
    :param filename: path to file with array
    :return: array
    """
    extension = filename.split(".")[-1]
    if extension == 'txt':
        return np.loadtxt(filename)
    elif extension == 'csv':
        return np.loadtxt(filename, delimiter=',')
    elif extension == 'npy':
        return np.load(filename)
    raise ValueError(f"Incorrect filename {filename}")


def sum_func(array):
    """
    Calculate sum of array
    :param array: input array
    :return : Sum of array
    """
    return np.sum(array)


def mean_func(array):
    """
    Calculate mean of array
    :param array: input array
    :return: Mean of array
    """
    return np.mean(array)


def median_func(array):
    """
    Calculate median of array
    :param array: input array
    :return: median of array
    """
    return np.median(array)


def std_func(array):
    """
    Calculate Standard Deviation of array
    :param array: input array
    :return: std of array
    """
    return np.std(array)


def aggregate(array, func='mean', axis=0):
    """
    Aggregate one of [mean, sum ,std, median] operation to array along axis
    :param array: Array to aggregate
    :param func: type of function to aggregate
    :param axis: axis to aggreagete
    :return: agregated array
    """
    if func == 'mean':
        return np.mean(array, axis=axis)
    elif func == 'median':
        return np.median(array, axis=axis)
    elif func == 'std':
        return np.std(array, axis=axis)
    elif func == 'sum':
        return np.sum(array, axis=axis)


if __name__ == "__main__":
    # Create and show original array
    array = create_array()
    print_array(array, "Generated array:", addition_info=True)

    # Save arrays
    save_array(array, extension="txt")
    save_array(array, extension="csv")
    save_array(array, extension="npy")

    # Read and output arrays
    array_txt = read_array("numpy_array.txt")
    print_array(array_txt, "Array from .txt file")

    array_csv = read_array("numpy_array.csv")
    print_array(array_csv, "Array from .csv file")

    array_npy = read_array("numpy_array.npy")
    print_array(array_npy, "Array from .npy file")

    array_sum = sum_func(array)
    print_array(array_sum, "Sum of the array:")

    array_mean = mean_func(array)
    print_array(array_mean, "Mean of the array:")

    array_std = std_func(array)
    print_array(array_std, "Standard Deviation of the array:")

    array_median = median_func(array)
    print_array(array_median, "Median of the array:")

    aggregate_sum = aggregate(array, 'sum', axis=0)
    print_array(aggregate_sum, "Sum of the array over columns")

    aggregate_mean = aggregate(array, 'mean', axis=1)
    print_array(aggregate_mean, "Mean of the array over rows")

    aggregate_std = aggregate(array, 'std', axis=0)
    print_array(aggregate_std, "Standard Deviation of the array over columns")

    aggregate_median = aggregate(array, 'median', axis=1)
    print_array(aggregate_median, "Median of the array over rows")
