import numpy as np
from datetime import datetime
from task1 import print_array

def create_transactions(l):
    """
    Create data of transaction
    :param l: number of transactions
    :return: array of transaction
    """
    np.random.seed(555)
    # indexes
    transaction_id = np.arange(1, l + 1, dtype=int)

    # ID of users from 1 to array_length / 2
    user_id = np.random.randint(1, l // 2, l, dtype=int)

    # ID of product from 101 to 100 + array_length / 2
    product_id = np.random.randint(1, l // 2, l, dtype=int) + 100

    # Random price
    price = (np.random.randint(15, 100, l) / 4).astype(float)
    # Random quantity
    quantity = np.random.randint(0, 15, l, dtype=int)

    # Created datetime
    date = np.array([np.datetime64('2024-07-21T14:00')])
    for _ in range(l - 1):
        date = np.append(date, date[-1] + np.random.randint(50, 500))
    date = date.astype(object)

    return np.vstack((transaction_id, user_id, product_id, price, quantity, date)).T


def get_total_revenue(transactions):
    total_revenue = np.sum(transactions[:, 3] * transactions[:, 4])
    return total_revenue


def get_unique_users(transactions):
    unique_users = np.unique(transactions[:, 1])
    return unique_users


def most_purchased_product(transactions):
    # find all unique products
    unique_products = np.unique(transactions[:, 2])

    # id and count of most purchased item
    product_id = None
    product_count = 0
    for p_id in unique_products:
        s = transactions[transactions[:, 2] == p_id, 4].sum()
        if s > product_count:
            product_id = p_id
            product_count = s

    return product_id, product_count


def convert_price_to_int(transactions):
    # Make a copy of the array to avoid changing the original array
    new_transactions = transactions.copy()
    # Convert the price column from float to integer
    new_transactions[:, 3] = new_transactions[:, 3].astype(float).astype(int)
    return new_transactions


def check_column_types(array):
    column_types = []
    for col in range(array.shape[1]):
        column_type = type(array[0, col])
        column_types.append(column_type)
    return column_types


def get_product_quantify(array):
    unique_product = np.unique(array[:, 2])
    quantify = [np.sum(array[array[:, 2] == p_id, 4]) for p_id in unique_product]

    return np.array([unique_product, quantify]).T


def user_transaction_count(array):
    users, n_trans = np.unique(array[:, 1], return_counts=True)

    return np.array([users, n_trans]).T

def hide_quantity_zero_trans(array):
    mask = array[:, 4] != 0
    return array[mask]


def increase_price(array, value=5):
    increase_coef = 1 + value / 100
    new_array = array.copy()

    new_array[:, 3] = np.round(new_array[:, 3].astype(float) * increase_coef, 2)
    return new_array


def filter_transaction(array):
    filter_mask = array[:, 4] > 1
    return array[filter_mask]


def calculate_revenue(transactions, start_time, end_time):
    # Convert timestamps to datetime objects for comparison
    start_time = datetime.fromisoformat(start_time)
    end_time = datetime.fromisoformat(end_time)

    total_revenue = 0
    for transaction in transactions:
        transaction_time = datetime.fromisoformat(transaction[5])
        if start_time <= transaction_time <= end_time:
            total_revenue += transaction[3] * transaction[4]  # quantity * price

    return total_revenue


def compare_revenue(transactions, period1_start, period1_end, period2_start, period2_end):
    revenue_period1 = calculate_revenue(transactions, period1_start, period1_end)
    revenue_period2 = calculate_revenue(transactions, period2_start, period2_end)

    return revenue_period1, revenue_period2


def get_user_transactions(transaction, user_id):
    return transaction[transaction[:, 2] == user_id]


def top_products_by_revenue(transactions, top_n=5):

    # Calculate revenue for each transaction (quantity * price)
    revenue = transactions[:, 3].astype(float) * transactions[:, 4].astype(float)

    # Create a dictionary to store total revenue per product_id
    product_revenue = {}

    for transaction, rev in zip(transactions, revenue):
        product_id = transaction[2]
        if product_id in product_revenue:
            product_revenue[product_id] += rev
        else:
            product_revenue[product_id] = rev

    # Sort the products by total revenue and get the top N product_ids
    top_product_ids = sorted(product_revenue, key=product_revenue.get, reverse=True)[:top_n]

    # Retrieve transactions that match the top product_ids
    top_transactions = transactions[np.isin(transactions[:, 2], top_product_ids)]

    return top_transactions


if __name__ == "__main__":
    transactions = create_transactions(30)
    print_array(transactions, "Transaction array:")

    total_revenue = get_total_revenue(transactions)
    print_array(total_revenue, "Total revenue:")

    unique_users = get_unique_users(transactions)
    print_array(unique_users, "Unique users:")

    product = most_purchased_product(transactions)
    print_array(product, "Id and Quantity of most purchased product:")

    new_transactions = convert_price_to_int(transactions)
    print_array(new_transactions[:5], "Converted price type to 'int' in transactions (first 5 rows):")

    old_dtype = check_column_types(transactions)
    print_array(old_dtype, "Dtype of columns in array before converting 'price' column to int")

    new_dtype = check_column_types(new_transactions)
    print_array(new_dtype, "Dtype of columns in array after converting price column to int")

    product_quantify = get_product_quantify(transactions)
    print_array(product_quantify, "Product quantify:")

    user_transaction = user_transaction_count(transactions)
    print_array(user_transaction, "Transaction counts per user:")

    hide_zero_quantify = hide_quantity_zero_trans(transactions)
    print_array(hide_zero_quantify, "Transaction without zero quantify transactions:")

    increased_price = increase_price(transactions)
    print_array(increased_price, "Transaction with increased price (by 5%):")

    filtered_transaction = filter_transaction(transactions)
    print_array(filtered_transaction, "Only include transactions with a quantity greater than 1.")

    # Don't worked function with datetime
    # period1_start = '2024-07-21T14:00'
    # period1_end = '2024-07-21T20:00'
    # period2_start = '2024-07-21T20:00'
    # period2_end = '2024-08-01T8:20:00'
    #
    # revenue1, revenue2 = compare_revenue(new_transactions, period1_start, period1_end, period2_start, period2_end)
    # if revenue1 > revenue2:
    #     print_array(revenue1 - revenue2, "Second period get more revenue than first by:")
    # else:
    #     print_array(revenue2 - revenue1, "Second period get more revenue than first by:")

    specific_user_transaction = get_user_transactions(transactions, 101)
    print_array(specific_user_transaction, "All transaction of 101 user")

    # Retrieve transactions of the top 5 products by revenue
    top_transactions = top_products_by_revenue(transactions, top_n=5)
    print_array(top_transactions, "Retrieve transactions of the top 5 products by revenue")
