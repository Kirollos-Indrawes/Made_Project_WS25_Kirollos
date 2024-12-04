import kaggle
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import sqlite3 as sql
import os

kaggle.api.authenticate()

kaggle.api.dataset_download_files('abdelrahman16/co2-emissions-usa', path='./data', unzip=True)
kaggle.api.dataset_download_files('isaacfemiogunniyi/co2-emission-of-vehicles-in-canada', path='./data', unzip=True)


print(os.listdir('./data'))

co2_usa = pd.read_csv('./data/emissions.csv') 
url = "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD"
cars = pd.read_csv(url)
co2_can = pd.read_csv('./data/CO2 Emissions_Canada.csv')

# Deleting Columns which are not important and irrelevant 
cars = cars.drop(['VIN (1-10)' , 'DOL Vehicle ID'], axis = 1) 

# First Electric Car was invented in 2008
#filtered_cars = cars[cars['Model Year'] < 2008][['Make', 'Model Year']]
#print(filtered_cars.head(20))
cars.drop(cars[cars['Model Year'] < 2008].index, inplace=True)

cars['County'] = cars['County'].fillna('Unknown')
cars['City'] = cars ['City'].fillna('Unknown')
cars['Postal Code'] = cars['Postal Code'].fillna('Unknown')
cars['Legislative District'] = cars['Legislative District'].fillna('Unknown')
cars['2020 Census Tract'] = cars['2020 Census Tract'].fillna('Unknown')
cars['Vehicle Location'] = cars['Vehicle Location'].fillna('Unknown')

Group_by_Make = cars.groupby('Make')['Electric Utility'].apply(lambda x: x.mode()[0] if not x.mode().empty else None)
Group_by_Make_Year_ER = cars.groupby(['Make', 'Model Year'])['Electric Range'].apply(lambda x: x.mode()[0] if not x.mode().empty else None)
Group_by_Make_Year_MSRP = cars.groupby(['Make', 'Model Year'])['Base MSRP'].apply(lambda x: x.mode()[0] if not x.mode().empty else None)


cars['Electric Utility'] = cars.apply(
    lambda row: Group_by_Make[row['Make']] if pd.isnull(row['Electric Utility']) else row['Electric Utility'],
    axis = 1
)

cars['Electric Range'] = cars.apply(
    lambda row: Group_by_Make_Year_ER.get((row['Make'], row['Model Year']), None) if pd.isnull(row['Electric Range']) else row['Electric Range'],
    axis=1
)

cars['Base MSRP'] = cars.apply(
    lambda row: Group_by_Make_Year_MSRP.get((row['Make'], row['Model Year']), None) if pd.isnull(row['Base MSRP']) else row ['Base MSRP'],
    axis = 1 
)

# Wanted to see the Data
#print(cars.duplicated().sum())
#print(cars['Make'].unique())
#print(cars['Model'].unique())
#print(cars['County'].unique())
#print(cars['Make'].value_counts())
#print(cars['State'].value_counts())
#print(cars['Electric Vehicle Type'].value_counts())
#print(co2_usa.head())
#print(co2_can.head())

#sns.boxplot(x=cars['Electric Range'])
#plt.show()
#sns.boxplot(x=cars['Base MSRP'])
#plt.show()
#sns.boxplot(x=co2_usa['value'])
#plt.show()

#Checking
#print(cars['Model Year'].min(), cars['Model Year'].max())

outliers = cars[cars['Base MSRP'] > 200000]

for index, outlier_row in outliers.iterrows():
    make = outlier_row['Make']
    model_year = outlier_row['Model Year']
    
    mode_for_make = cars[(cars['Make'] == make) & (cars['Model Year'] == model_year)]['Base MSRP'].mode()
    cars.at[index, 'Base MSRP'] = float(mode_for_make.iloc[0])


#For Testing the Only
#sns.boxplot(x=cars['Electric Range'])
#plt.show()
#sns.boxplot(x=cars['Base MSRP'])
#plt.show()
#sns.boxplot(x=co2_usa['value'])
#plt.show()

#print(co2_can.head(10))
#print(co2_can.describe())
#print(co2_can.isnull().sum())
# Dropping irrelevant Columns:

columns_to_drop = ['Fuel Consumption Hwy (L/100 km)', 'Fuel Consumption City (L/100 km)', 'Fuel Consumption Comb (mpg)']


co2_can.drop(columns=columns_to_drop, inplace=True)

# Checking if i have duplicates in the second dataset
#duplicate_count = co2_can.duplicated().sum()
#print(f'Number of duplicates: {duplicate_count}')
co2_can.drop_duplicates(inplace=True)
# Checking my Result
#print(co2_can.head())

# Checking Data Types
#print(co2_can.dtypes)
co2_can['Make'] = co2_can['Make'].astype('category')
co2_can['Model'] = co2_can['Model'].astype('category')

#print(co2_can.columns)

#sns.boxplot(x=co2_can['Engine Size(L)'])
#plt.title('Engine Size(L)')
#plt.show()

#sns.boxplot(x=co2_can['Cylinders'])
#plt.title('Cylinders')
#plt.show()

#sns.boxplot(x=co2_can['Fuel Consumption Comb (L/100 km)'])
#plt.title('Fuel Consumption Comb (L/100 km)')
#plt.show()

#sns.boxplot(x=co2_can['CO2 Emissions(g/km)'])
#plt.title('CO2 Emissions(g/km)')
#plt.show()
#large_engine= co2_can[co2_can['Engine Size(L)'] > 5] 
#print(large_engine)

# Checking the relation ship and if there any outliers

#sns.scatterplot(x=co2_can['Engine Size(L)'], y=co2_can['Cylinders'])
#plt.title('Engine Size vs. Cylinders')
#plt.show()

#print(co2_usa.isnull().sum())
#print(co2_usa.duplicated().sum())
#print(co2_usa.head(10))
#print(co2_usa.dtypes)

#sns.boxplot(x=co2_usa['fuel-name'], y = co2_usa['value'])
#plt.title('Fuel Name/ Value')
#plt.show()

folder_path = r'C:/Users/kirol/Desktop/Made_Project_WS25_Kirollos/data'

if not os.path.exists(folder_path):
    os.makedirs(folder_path)

db_path = os.path.join(folder_path, 'pipeline.db')

conn = sql.connect(db_path)

co2_usa.to_sql('co2_usa', conn, if_exists='replace', index=False)
cars.to_sql('cars', conn, if_exists='replace', index=False)
co2_can.to_sql('co2_canada', conn, if_exists='replace', index=False)

conn.commit()
conn.close()

print('Done !?')