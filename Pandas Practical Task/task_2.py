import pandas as pd
from task_1 import set_option


def print_grouped_data(df, message):
    print(message)
    print(df)


if __name__=="__main__":
    set_option()
    df = pd.read_csv("../data/cleaned_airbnb_data.csv")
    print_grouped_data(df.head(), "First 5 rows of dataset")

    # Loc and iloc
    print(f'Selecting with loc:\n {df.loc[5:7, ["neighbourhood", "latitude", "longitude"]]}')
    print(f"Selecting with iloc:\n {df.iloc[5:8, 5:8]}")

    # Filtering
    filtered_df = df[(df["price"] > 100) & (df["number_of_reviews"] > 10)]
    columns_of_interest = ["neighbourhood_group", "price", "minimum_nights", "number_of_reviews", "price_category",
                           "availability_365"]
    df_interests = filtered_df[columns_of_interest]
    print_grouped_data(df_interests.head(), "Dataframe after filtering")

    # Aggregation and Grouping:
    group_df = df_interests.groupby(["neighbourhood_group", "price_category"])
    print(group_df[["price", "minimum_nights"]].mean(), "Average price and minimum nights per group")
    print(group_df[["number_of_reviews", "availability_365"]].mean(), "Average number_of_reviews and availability_365")

    # Data Sorting and Ranking
    # Sorting
    sorted_df = df_interests.sort_values(["price", "number_of_reviews"], ascending=[False, True])
    print_grouped_data(sorted_df, "Sorted data by price and number of reviews")
    # Ranking
    price_ranking = df_interests.groupby("neighbourhood_group")[["price"]].mean().sort_values("price", ascending=False)
    print_grouped_data(price_ranking, "Ranking of neighborhoods based on the price")
    num_reviews_ranking = df_interests.groupby("neighbourhood_group")[["number_of_reviews"]].sum().sort_values("number_of_reviews", ascending=False)
    print_grouped_data(num_reviews_ranking, "Ranking of neighborhoods based on the total number of listings")

    # Saving filtered and selected data
    df_interests.to_csv("../data/aggregated_airbnb_data.csv.")

