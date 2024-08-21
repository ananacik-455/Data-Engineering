import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np


# Function to create and save neighborhood distribution plot
def plot_neighborhood_distribution(df):
    df = df['neighbourhood_group'].value_counts().reset_index()
    df.columns = ['neighbourhood_group', 'count']
    plt.figure(figsize=(10, 6))
    ax = sns.barplot(x='neighbourhood_group',
                     y='count',
                     data=df,
                     palette='Set3',
                     edgecolor='black')
    ax.bar_label(ax.containers[0])
    plt.title('Distribution of Listings Across Neighborhood Groups', fontsize=16)
    plt.xlabel('Neighborhood Group', fontsize=14)
    plt.ylabel('Number of Listings', fontsize=14)
    plt.xticks(rotation=0)
    ax.bar_label(ax.containers[0])
    plt.tight_layout()
    plt.savefig('Plots/neighborhood_distribution.png')
    plt.show()


# Function to create and save price distribution plot
def plot_price_distribution(df):
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df, x='neighbourhood_group', y='price', palette='husl')
    plt.title('Price Distribution Across Neighborhood Groups', fontsize=16)
    plt.xlabel('Neighborhood Group', fontsize=14)
    plt.ylabel('Price', fontsize=14)
    plt.ylim(0, 500)  # Limit y-axis for better visualization
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig('Plots/price_distribution.png')
    plt.show()


# Function to create and save room type vs availability plot
def plot_room_type_vs_availability(df):
    plt.figure(figsize=(12, 8))
    sns.barplot(data=df,
                x='neighbourhood_group',
                y='availability_365',
                hue='room_type',
                estimator=np.mean,
                capsize=.1,
                palette='husl',
                edgecolor='black')
    plt.title('Average Availability for Each Room Type Across Neighborhoods', fontsize=16)
    plt.xlabel('Neighborhood Group', fontsize=14)
    plt.ylabel('Average Availability (days)', fontsize=14)
    plt.xticks(rotation=0)
    plt.legend(title='Room Type')
    plt.tight_layout()
    plt.savefig('Plots/room_type_vs_availability.png')
    plt.show()


# Function to create and save price vs number of reviews plot
def plot_price_vs_reviews(df):
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df,
                    x='price',
                    y='number_of_reviews',
                    hue='room_type',
                    palette='husl',
                    alpha=0.6)
    sns.regplot(data=df, x='price', y='number_of_reviews', scatter=False, color='black')
    plt.title('Correlation Between Price and Number of Reviews', fontsize=16)
    plt.xlabel('Price', fontsize=14)
    plt.ylabel('Number of Reviews', fontsize=14)
    plt.xlim(0, 500)  # Limit x-axis for better visualization
    plt.ylim(0, 300)  # Limit y-axis for better visualization
    plt.legend(title='Room Type')
    plt.tight_layout()
    plt.savefig('Plots/price_vs_reviews.png')
    plt.show()


# Function to create and save time series analysis of reviews plot
def plot_time_series_reviews(df):
    plt.figure(figsize=(14, 8))
    for group in df['neighbourhood_group'].unique():
        group_df = df[df['neighbourhood_group'] == group]
        group_df = group_df.groupby('last_review')['number_of_reviews'].sum().rolling(window=12).mean()
        plt.plot(group_df, label=group)
    plt.title('Trend of Number of Reviews Over Time by Neighborhood Group', fontsize=16)
    plt.xlabel('Date of Last Review', fontsize=14)
    plt.ylabel('Number of Reviews', fontsize=14)
    plt.legend(title='Neighborhood Group')
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig('Plots/time_series_reviews.png')
    plt.show()


# Function to create and save price and availability heatmap
def plot_price_availability_heatmap(df):
    pivot_table = df.pivot_table(values='price', index='neighbourhood_group', columns='availability_365', aggfunc='mean')
    plt.figure(figsize=(14, 8))
    sns.heatmap(pivot_table, cmap='coolwarm', annot=False)
    plt.title('Heatmap of Price vs. Availability Across Neighborhoods', fontsize=16)
    plt.xlabel('Availability (days)', fontsize=14)
    plt.ylabel('Neighborhood Group', fontsize=14)
    plt.tight_layout()
    plt.savefig('Plots/price_availability_heatmap.png')
    plt.show()


# Function to create and save room type and review count analysis
def plot_room_type_review_count(df):
    review_count = df.groupby(['neighbourhood_group', 'room_type'])['number_of_reviews'].sum().unstack()
    review_count.plot(kind='bar', stacked=True, colormap='tab10', figsize=(12, 8))
    plt.title('Number of Reviews by Room Type Across Neighborhood Groups', fontsize=16)
    plt.xlabel('Neighborhood Group', fontsize=14)
    plt.ylabel('Total Number of Reviews', fontsize=14)
    plt.legend(title='Room Type')
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig('Plots/room_type_review_count.png')
    plt.show()


if __name__=="__main__":
    # Load the dataset
    df = pd.read_csv('../data/AB_NYC_2019.csv')

    # Data Preparation
    # Fill missing values
    df['last_review'] = pd.to_datetime(df['last_review'])
    df['reviews_per_month'].fillna(0, inplace=True)
    df['name'].fillna('Unknown', inplace=True)

    # Execute and Save Plots
    plot_neighborhood_distribution(df)
    plot_price_distribution(df)
    plot_room_type_vs_availability(df)
    plot_price_vs_reviews(df)
    plot_time_series_reviews(df)
    plot_price_availability_heatmap(df)
    plot_room_type_review_count(df)
