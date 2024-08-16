import pandas as pd


def price_category_filter(x):
    if x < 100:
        return 'Low'
    elif x < 300:
        return 'Medium'
    else:
        return 'High'


def length_of_stay_filter(x):
    if x <= 3:
        return "short-term"
    elif x <= 14:
        return "medium-term"
    else:
        return "long-term"


def print_dataframe_info(df, message):
    print(message, end="\n\n")
    print(f"Dataframe shape: {df.shape}")
    print("First 5 rows of dataframe:")
    print(df.head())
    print("Dataframe info:")
    print(df.info())
    print("Number of missing values:")
    print(df.isna().sum())


def set_option():
    pd.set_option('display.max_columns', 20)


if __name__=="__main__":
    # Read and show df
    filepath = "../data/AB_NYC_2019.csv"
    df = pd.read_csv(filepath)
    set_option()
    print_dataframe_info(df, "DataFrame before cleaning:")

    # Deal with missing values
    df.loc[:, ['name', 'host_name']].fillna("Unknown", inplace=True)
    df['last_review'].fillna("NaT", inplace=True)
    df['reviews_per_month'].fillna(0, inplace=True)

    # Drop rows where price = 0
    df.drop(df[df["price"] <= 0].index, inplace=True)

    # Add column "price_category" with Low(price<100), Medium(100<price<300) and High(price > 300) values
    df['price_category'] = df['price'].apply(price_category_filter)

    # Categorize listings based on their minimum_nights into short-term, medium-term, and long-term stays.
    df["length_of_stay_category"] = df["minimum_nights"].apply(length_of_stay_filter)

    # Show the cleaned df
    print_dataframe_info(df, "DataFrame after cleaning:")

    # Save dataframe
    df.to_csv("../data/cleaned_airbnb_data.csv", index=False)