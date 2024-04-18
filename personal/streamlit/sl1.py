import streamlit as st
# import mlcroissant as mlc 
import pandas as pd

# st.title('DTSC 2102')
# st.write('Hello, world!')
# # st.caption('Bing bong')
# st.code("""
# for i in range(8):
#     bingbong()
# """)

# st.text_area('Hello there')
# st.camera_input('Camera')

# st.slider('Slide me', 0, 10, 5)
# df = pd.read_json('STUDIO2/personal/streamlit/food-prices-in-india-metadata.json')
df = pd.read_csv('STUDIO2/personal/streamlit/food_prices_ind.csv')

df.head()
