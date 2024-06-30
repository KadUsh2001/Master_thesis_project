import streamlit as st
import pandas as pd
import numpy as np
from streamlit_folium import st_folium
import folium
from folium import IFrame
from ast import literal_eval
from utils import create_map, data_show


def page_kappa():
    st.title('Paris Metro Live 7 - Waiting Time App')
    st.subheader('Raw data')


    path_C = "kafka_data/data_C.csv"
    path_I = "kafka_data/data_I.csv"
    path_V = "kafka_data/data_V.csv"

    df_C = pd.read_csv(path_C)
    df_I = pd.read_csv(path_I)
    df_V = pd.read_csv(path_V)


    direction = st.selectbox(
        'Select the direction of the train:',
        ('Courneuve', 'Ivry', 'Villejuif')
    )

    # Create map based on the selected direction
    if direction == 'Courneuve':
        m = create_map(df_C)
    elif direction == 'Ivry':
        m = create_map(df_I)

    else:
        m = create_map(df_V)




    #Display the map in Streamlit
    st_folium(m, width=700, height=500)

    if direction == 'Courneuve':

        st.table(data_show(df_C))
    elif direction == 'Ivry':
        st.table(data_show(df_I))
    else:
        st.table(data_show(df_V))


def page_lambda():
    st.title('Paris Metro Live 7 - Waiting Time App')
    st.subheader('Raw data')


    path_C = "kafka_data/data_C.csv"
    path_I = "kafka_data/data_I.csv"
    path_V = "kafka_data/data_V.csv"

    df_C = pd.read_csv(path_C)
    df_I = pd.read_csv(path_I)
    df_V = pd.read_csv(path_V)


    direction = st.selectbox(
        'Select the direction of the train:',
        ('Courneuve', 'Ivry', 'Villejuif')
    )

    # Create map based on the selected direction
    if direction == 'Courneuve':
        m = create_map(df_C)
    elif direction == 'Ivry':
        m = create_map(df_I)

    else:
        m = create_map(df_V)




    #Display the map in Streamlit
    st_folium(m, width=700, height=500)

    if direction == 'Courneuve':

        st.table(data_show(df_C))
    elif direction == 'Ivry':
        st.table(data_show(df_I))
    else:
        st.table(data_show(df_V))


    path_C_historical = "kafka_data/data_C_historical.csv"
    path_I_historical = "kafka_data/data_I_historical.csv"
    path_V_historical = "kafka_data/data_V_historical.csv"

    df_C_historical = pd.read_csv(path_C_historical)
    df_I_historical = pd.read_csv(path_I_historical)
    df_V_historical = pd.read_csv(path_V_historical)

    df_C_historical_show = df_C_historical.groupby('Stop')['Next Train'].agg(
        Mean_Wait_Time='mean',
        Measurements='count'
    ).reset_index()

    df_C_historical_show['Mean Wait Time'] = df_C_historical_show['Mean_Wait_Time'].round(1)
    df_C_historical_show = df_C_historical_show[["Stop", "Mean Wait Time", "Measurements"]]

    df_I_historical_show = df_I_historical.groupby('Stop')['Next Train'].agg(
        Mean_Wait_Time='mean',
        Measurements='count'
    ).reset_index()

    df_I_historical_show['Mean Wait Time'] = df_I_historical_show['Mean_Wait_Time'].round(1)
    df_I_historical_show = df_I_historical_show[["Stop", "Mean Wait Time", "Measurements"]]

    df_V_historical_show = df_V_historical.groupby('Stop')['Next Train'].agg(
        Mean_Wait_Time='mean',
        Measurements='count'
    ).reset_index()

    df_V_historical_show['Mean Wait Time'] = df_V_historical_show['Mean_Wait_Time'].round(1)
    df_V_historical_show = df_V_historical_show[["Stop", "Mean Wait Time", "Measurements"]]




    if direction == 'Courneuve':
        st.table(df_C_historical_show)
    elif direction == 'Ivry':
        st.table(df_I_historical_show)
    else:
        st.table(df_V_historical_show)


def main():
    page = st.selectbox(
    'Choose a page',
    ('Kappa Version', 'Lambda Version'))

    # Conditionally render subpage based on user selection
    if page == 'Kappa Version':
        page_kappa()
    elif page == 'Lambda Version':
        page_lambda()

if __name__ == "__main__":
    main()