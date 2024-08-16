import pandas as pd
from task_1 import set_option


def classify_listings(value):
    if value < 50:
        return "Rarely Available"
    elif value < 200:
        return "Occasionally Available"
    return "Highly Available"


def print_analysis_results(df, message):
    print(message)
    print(df)


if __name__=="__main__":
    set_option()
    df = pd.read_csv("../data/cleaned_airbnb_data.csv")
    print_analysis_results(df.head(), "First 5 rows of df")

    # Pivot table to Analyze Pricing Trends Across Neighborhoods and Room Types
    pivot_df = pd.pivot_table(df, index=['neighbourhood_group', 'room_type'])
    print_analysis_results(pivot_df, "Detailed summary that reveals the average price for different combinations of neighbourhood_group and room_type")
    # Prepare Data for In-Depth Metric Analysis
    df_melted = pd.melt(pivot_df,
                        value_vars=['price', 'minimum_nights'],
                        var_name='metric',
                        value_name='value',
                        ignore_index=False)
    print_analysis_results(df_melted, "Long format of data with detailed analysis if price and minimum_nights")

    # Classify Listings by Availability
    df["availability_status"] = df["availability_365"].apply(classify_listings)

    availability_neighbourhood = df.pivot_table(
        index='neighbourhood_group',
        columns='availability_status',
        values=['price', "number_of_reviews"],
        aggfunc='mean'
    )
    print(availability_neighbourhood, "Correlation between availability and city")

    descriptive_stat = df.loc[:, ["price", "minimum_nights", "number_of_reviews"]].apply(["mean", "median", "std"])
    print(descriptive_stat, "Descriptive statistics")

    # Create copy of dataframe
    df_time_series = df.copy()
    # Convert last_review column to datetime format
    df_time_series["last_review"] = pd.to_datetime(df["last_review"])
    # Drop rows with "NaT" values
    df_time_series.dropna(subset=['last_review'], inplace=True)
    # Ser last_review as index
    df_time_series.set_index("last_review", inplace=True)
    # Sort data by date
    df_time_series.sort_index(inplace=True)
    print_analysis_results(df_time_series, "Dataframe for Time Series Analysis")

    # Identify Monthly Trends
    monthly_trends = df_time_series.resample('M').agg({
        'number_of_reviews': 'sum',  # Sum of reviews per month
        'price': 'mean'  # Average price per month
    }).fillna(0)
    print_analysis_results(monthly_trends.iloc[:10], "Monthly trends in the number of reviews and average prices")

    # Analyze Seasonal Patterns:
    seasonal_pattern = df_time_series.groupby(by=df_time_series.index.month).agg("mean")[["price", "reviews_per_month"]]
    print_analysis_results(seasonal_pattern, "Price and reviews average per month")

    df_time_series.to_csv("../data/time_series_airbnb_data.csv")