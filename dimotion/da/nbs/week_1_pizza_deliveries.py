# Challenge Questions
# How many deliveries took more than 45 minutes?
# How many deliveries were made in less than 20 minutes?
# Identify orders where the customer phone number is missing. How many such instances are there? Also why is this a problem for the business?
# What is the most common payment method for orders that took more than 45 minutes to deliver?
# What is the average delivery fee for orders that took more than 30 minutes?
# How many orders took more than 45 minutes to deliver and also had a delivery fee of more than $5?
# Does the pizza size affect the delivery time? Calculate the average delivery time for each pizza size.
# Create your own question and answer it.
# Bonus:, Create a dashboard to showcase your insights to management

#%% dependencies
import pandas as pd
import numpy as np
import sqlalchemy as sql

#%% read csv
df = pd.read_csv('pizza_delivery.csv')
df.head()

#%% number of rows
df.shape[0]

#%% create engine and connection
engine = sql.create_engine('sqlite:///pizza')
connection = engine.connect()
df.to_sql('pizza', con=connection)

#%% get metadata and create table
metadata = sql.MetaData()
pizza = sql.Table('pizza', metadata, autoload=True, autoload_with=engine)
print(repr(pizza))
print(metadata.tables.keys())

#%% inspector object
inspector = sql.inspect(engine)
cols = []

for i in range(df.shape[1]):
    cols.append({inspector.get_columns('pizza')[i]['name']:inspector.get_columns('pizza')[i]['type']})

#%% eda
# deliveries that took more than 45 minutes
stmt = sql.select(sql.func.count(pizza.columns.order_id))
stmt = stmt.where(pizza.columns.delivery_time > 45)
print(f"Total orders with delivery time over 45 minutes: {connection.execute(stmt).scalar()}")

#%%
# deliveries that took less than 20 minutes
stmt = sql.select(sql.func.count(pizza.columns.order_id))
stmt = stmt.where(pizza.columns.delivery_time < 20)
print(f"Total orders with delivery time less than 20 minutes: {connection.execute(stmt).scalar()}")

#%%
# customers with no phone number
stmt = sql.select(sql.func.count(pizza.columns.order_id))
stmt = stmt.where(pizza.columns.customer_phone == None)
print(f"Customers that didn't input there phone number: {connection.execute(stmt).scalar()}")
print("Business cannot contact the customers of more than third of their orders.")

#%%
# most common payment type for orders that took more than 30 minutes
stmt = sql.select(pizza.columns.payment_method, sql.func.count(pizza.columns.order_id))
stmt = stmt.group_by(pizza.columns.payment_method)
stmt = stmt.where(pizza.columns.delivery_time > 30)
print(connection.execute(stmt).fetchall())

#%%
# What is the average delivery fee for orders that took more than 30 minutes?
stmt = sql.select(sql.func.avg(pizza.columns.delivery_fee))
stmt = stmt.where(pizza.columns.delivery_time > 30)
print(f"Average delivery fee for orders over 30 minutes: {connection.execute(stmt).scalar()}")

#%%
# How many orders took more than 45 minutes to deliver and also had a delivery fee of more than $5?
stmt = sql.select(sql.func.count(pizza.columns.order_id))
stmt = stmt.where(sql.and_(pizza.columns.delivery_time > 45,
                           pizza.columns.delivery_fee > 5))
print(f"Orders that took more than 45 minutes and are more than $5: {connection.execute(stmt).scalar()}")

#%% 
# Does the pizza size affect the delivery time? Calculate the average delivery time for each pizza size.
stmt = sql.select(pizza.columns.pizza_size, 
                  sql.func.avg(pizza.columns.delivery_time))
stmt = stmt.group_by(pizza.columns.pizza_size)
print(f"Mean delivery time for each pizza size: {connection.execute(stmt).fetchall()}")

#%% bivariate analysis
# Create question and answer it
# Is there a correlation between delivery_time and driver_rating
import seaborn as sns
import matplotlib.pyplot as plt
# fig, ax = plt.subplots(1, 1, figsize=(12, 7))
fig = sns.scatterplot(data=df,
                x='delivery_time',
                y='driver_rating')
fig = fig.get_figure()
print("It seems like there is no correlation/pattern between these two variables.")
plt.show()
fig.savefig("corr.png")

#%% dashboard?
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

class Config(object):
    SQLALCHEMY_DATABASE_URL = 'sqlite:///app.db'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
db = SQLAlchemy()

def create_app():
    server = Flask(__name__)
    server.config.from_object(Config)
    db.init_app(server)
    
    with server.app_content():
        df = pd.read_csv('pizza_delivery.csv')
        df.to_sql('pizza', con=db.engine, if_exists='replace')
        
    from .dashboard import create_dashapp
    dash_app = create_dashapp(server)
    return server
