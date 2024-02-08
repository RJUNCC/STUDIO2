# imports 
import streamlit as st
import pandas as pd
import time
import numpy as np

# 
df = pd.DataFrame({'name':['Ro', 'Jacobian'],
                   'location':['nyc', 'boston']})

#main function
def main():
    st.title("Hello, World! Streamlit Page")
    st.write("This is a simple Streamlit app.")
    st.write(df)

if __name__ == "__main__":
    main()


def stream_data():
    for letter in "AOWDnANWDJANWDIAuwbd  awdbahuwdb awdbawbdh awjuhbd aw d jhwdjha wdaw djawd":
        yield letter + " "
        time.sleep(0.02)
    
    yield pd.DataFrame(np.random.randn(5, 10), columns=["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"])

    for letter in "AOWDnANWDJANWDIAuwbd  awdbahuwdb awdbawbdh awjuhbd aw d jhwdjha wdaw djawd":
        yield letter + " " 
        time.sleep(0.02)


if st.button("Stream data"):
    st.write_stream(stream_data)


