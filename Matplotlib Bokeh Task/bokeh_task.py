import pandas as pd
from bokeh.io import output_file, show
from bokeh.plotting import figure
from bokeh.models import HoverTool, ColumnDataSource, CategoricalColorMapper, Select
from bokeh.layouts import column, row
from bokeh.transform import factor_cmap
from bokeh.palettes import Spectral6, Viridis256

# Load the Titanic dataset
titanic = pd.read_csv('../data/Titanic-Dataset.csv')
titanic.columns = [x.lower() for x in titanic.columns]
# 1. Data Preparation

# Handle missing values
titanic['age'].fillna(titanic['age'].median(), inplace=True)
titanic['embarked'].fillna(titanic['embarked'].mode()[0], inplace=True)

# Create AgeGroup column
bins = [0, 12, 18, 35, 60, 80]
labels = ['Child', 'Teenager', 'Young Adult', 'Adult', 'Senior']
titanic['age_group'] = pd.cut(titanic['age'], bins=bins, labels=labels, right=False)

# Calculate SurvivalRate within each group
survival_rate_age_group = titanic.groupby('age_group')['survived'].mean().reset_index()
survival_rate_class_gender = titanic.groupby(['pclass', 'sex'])['survived'].mean().reset_index()

# 2. Visualization using Bokeh

# Age Group Survival - Bar Chart
source_age_group = ColumnDataSource(survival_rate_age_group)
p1 = figure(x_range=labels, title="Survival Rates by Age Group",
            toolbar_location=None, tools="")
p1.vbar(x='age_group', top='survived', width=0.9, source=source_age_group, legend_field="age_group",
        line_color='white', fill_color=factor_cmap('age_group', palette=Spectral6, factors=labels))

hover = HoverTool()
hover.tooltips = [("Age Group", "@age_group"), ("Survival Rate", "@survived{0.00%}")]
p1.add_tools(hover)

p1.xgrid.grid_line_color = None
p1.y_range.start = 0
p1.y_range.end = 1
p1.legend.orientation = "horizontal"
p1.legend.location = "top_center"

# Class and Gender - Grouped Bar Chart
pclass_labels = ['1st Class', '2nd Class', '3rd Class']
survival_rate_class_gender['pclass'] = survival_rate_class_gender['pclass'].map({1: '1st Class', 2: '2nd Class', 3: '3rd Class'})
source_class_gender = ColumnDataSource(survival_rate_class_gender)

p2 = figure(x_range=pclass_labels, title="Survival Rates by Class and Gender",
            toolbar_location=None, tools="")

p2.vbar(x='pclass', top='survived', width=0.4, source=source_class_gender, legend_field="sex",
        line_color='white', fill_color=factor_cmap('sex', palette=Spectral6, factors=['male', 'female']))

hover = HoverTool()
hover.tooltips = [("Class", "@pclass"), ("Gender", "@sex"), ("Survival Rate", "@survived{0.00%}")]
p2.add_tools(hover)

p2.xgrid.grid_line_color = None
p2.y_range.start = 0
p2.y_range.end = 1
p2.legend.orientation = "horizontal"
p2.legend.location = "top_center"

# Fare vs. Survival - Scatter Plot
fare_mapper = CategoricalColorMapper(factors=['1st Class', '2nd Class', '3rd Class'], palette=Spectral6)
titanic['pclass'] = titanic['pclass'].map({1: '1st Class', 2: '2nd Class', 3: '3rd Class'})
source_fare_survival = ColumnDataSource(titanic)

p3 = figure(title="Fare vs. Survival",
            x_axis_label="Fare", y_axis_label="Survived", tools="pan,wheel_zoom,box_zoom,reset")

p3.circle(x='fare', y='survived', source=source_fare_survival, size=10,
          color={'field': 'pclass', 'transform': fare_mapper}, legend_field='pclass', fill_alpha=0.6)

hover = HoverTool()
hover.tooltips = [("Fare", "@fare"), ("Survived", "@survived"), ("Class", "@pclass")]
p3.add_tools(hover)

p3.legend.orientation = "horizontal"
p3.legend.location = "top_center"

# 3. Interactivity - Adding Select Widgets for Filtering
class_select = Select(title="Class", value="All", options=["All"] + pclass_labels)
gender_select = Select(title="Gender", value="All", options=["All", "male", "female"])

# 4. Output - Saving visualizations as HTML files
output_file("Plots/titanic_survival_analysis.html")
layout = column(p1, p2, p3, row(class_select, gender_select))
show(layout)
