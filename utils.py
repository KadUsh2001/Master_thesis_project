from datetime import datetime
import datetime as datetimee
import ast
import pandas as pd
import numpy as np
import folium
from folium import IFrame
from ast import literal_eval



important_keys = ["RecordedAtTime", "DirectionName", "EstimatedCalls"]
processed_list = []

def get_processed_list(list_stops):
    processed_list = []
    for element in list_stops:
        shorter_dict={}
        for key in important_keys:
            shorter_dict[key] = element[key]
        processed_list += [shorter_dict]
    return processed_list


def get_calls(processed_list):
    results = []
    for element in processed_list:
        for value in element["EstimatedCalls"]["EstimatedCall"]:
            results += [value]
    return results

def extract_value_stoppoint(dict_str):
    try:
        dict_str = dict_str.strip('{}')
        key, value = dict_str.split(': ', 1)
        value = value.strip().strip("'")
        return value
    except Exception as e:
        return None

def extract_value_destination(list_dict_str):
    try:
        value_list = ast.literal_eval(list_dict_str)
        if isinstance(value_list, list) and len(value_list) > 0 and isinstance(value_list[0], dict):
            return value_list[0].get('value', None)
        else:
            return None
    except (ValueError, SyntaxError):
        return None

def process_timestamps(timestamp_list, reference_datetime):
    processed_list = []
    for timestamp in timestamp_list:
        dt = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
        dt_plus_two_hours = dt + datetimee.timedelta(hours=2)
        difference_minutes = round((dt_plus_two_hours - reference_datetime).total_seconds() / 60)
        processed_list.append(difference_minutes)
        processed_list = sorted(processed_list)
    return processed_list

def data_preprocessing(req):
    dicionario = pd.read_csv("dicionario.csv",sep=";")
    json = req.json()
    list_stops = json["Siri"]["ServiceDelivery"]["EstimatedTimetableDelivery"][0]['EstimatedJourneyVersionFrame'][0]["EstimatedVehicleJourney"]
    processed_list = get_processed_list(list_stops)
    calls = get_calls(processed_list)
    calls_df = pd.DataFrame(calls)
    calls_df["StopPointRef"] = calls_df["StopPointRef"].astype(str)
    calls_df["DestinationDisplay"] = calls_df["DestinationDisplay"].astype(str)
    calls_df['StopPointRef'] = calls_df['StopPointRef'].apply(extract_value_stoppoint)
    calls_df['DestinationDisplay'] = calls_df['DestinationDisplay'].apply(extract_value_destination)
    mapping = dict(zip(dicionario["MonitoringRef_ArR"], dicionario["ArRName"]))
    calls_df['StopPointName'] = calls_df['StopPointRef'].map(mapping)
    grouped_df = calls_df.groupby(['StopPointName', 'DestinationDisplay'])['ExpectedDepartureTime'].apply(list).reset_index()
    return grouped_df


def kafka_processing(df_after_kafka, time):
    final_results_C = df_after_kafka[df_after_kafka["DestinationDisplay"] == "La Courneuve-8-Mai-1945"]
    final_results_I = df_after_kafka[df_after_kafka["DestinationDisplay"] == "Mairie d'Ivry"]
    final_results_V = df_after_kafka[df_after_kafka["DestinationDisplay"] == "Villejuif-Louis Aragon"]
    final_results_C['Waiting_time'] = final_results_C['ExpectedDepartureTime'].apply(lambda x: process_timestamps(x, time))
    final_results_I['Waiting_time'] = final_results_I['ExpectedDepartureTime'].apply(lambda x: process_timestamps(x, time))
    final_results_V['Waiting_time'] = final_results_V['ExpectedDepartureTime'].apply(lambda x: process_timestamps(x, time))
    final_results_C = final_results_C.drop(columns=["ExpectedDepartureTime"])
    final_results_I = final_results_I.drop(columns=["ExpectedDepartureTime"])
    final_results_V = final_results_V.drop(columns=["ExpectedDepartureTime"])
    return final_results_C, final_results_I, final_results_V

def send_api_data(producer, topic, df):
    json_data = df.to_dict(orient='records')
    producer.send(topic, json_data)
    producer.flush()

def process_message(message):
    df = pd.DataFrame(message)
    return df

latitudes = {
    "La Courneuve - 8 Mai 1945": 48.9282,
    "Fort d'Aubervilliers": 48.9143,
    "Aubervilliers-Pantin Quatre Chemins": 48.8951,
    "Porte de la Villette": 48.8973,
    "Corentin Cariou": 48.8940,
    "Crimée": 48.8917,
    "Riquet": 48.8911,
    "Stalingrad": 48.8847,
    "Louis Blanc": 48.8826,
    "Château Landon": 48.8804,
    "Gare de l'Est": 48.8764,
    "Poissonnière": 48.8783,
    "Cadet": 48.8756,
    "Le Peletier": 48.8743,
    "Chaussée d'Antin - La Fayette": 48.8731,
    "Opéra": 48.8709,
    "Pyramides": 48.8663,
    "Palais Royal - Musée du Louvre": 48.8627,
    "Pont Neuf": 48.8585,
    "Châtelet": 48.8581,
    "Pont Marie (Cité des Arts)": 48.8534,
    "Sully - Morland": 48.8500,
    "Jussieu": 48.8463,
    "Place Monge": 48.8432,
    "Censier - Daubenton": 48.8419,
    "Les Gobelins": 48.8377,
    "Place d'Italie": 48.8318,
    "Tolbiac": 48.8276,
    "Maison Blanche": 48.8213,
    "Porte d'Italie": 48.8193,
    "Porte de Choisy": 48.8170,
    "Porte d'Ivry": 48.8160,
    "Pierre et Marie Curie": 48.8150,
    "Mairie d'Ivry": 48.8130,
    "Le Kremlin-Bicêtre": 48.8114,
    "Villejuif Léo Lagrange": 48.8079,
    "Villejuif Paul Vaillant-Couturier": 48.8004,
    "Villejuif - Louis Aragon": 48.7933
}

longitudes = {
    "La Courneuve - 8 Mai 1945": 2.3951,
    "Fort d'Aubervilliers": 2.3840,
    "Aubervilliers-Pantin Quatre Chemins": 2.3974,
    "Porte de la Villette": 2.3887,
    "Corentin Cariou": 2.3760,
    "Crimée": 2.3702,
    "Riquet": 2.3671,
    "Stalingrad": 2.3681,
    "Louis Blanc": 2.3619,
    "Château Landon": 2.3626,
    "Gare de l'Est": 2.3588,
    "Poissonnière": 2.3508,
    "Cadet": 2.3458,
    "Le Peletier": 2.3432,
    "Chaussée d'Antin - La Fayette": 2.3323,
    "Opéra": 2.3323,
    "Pyramides": 2.3358,
    "Palais Royal - Musée du Louvre": 2.3370,
    "Pont Neuf": 2.3428,
    "Châtelet": 2.3470,
    "Pont Marie (Cité des Arts)": 2.3551,
    "Sully - Morland": 2.3603,
    "Jussieu": 2.3564,
    "Place Monge": 2.3554,
    "Censier - Daubenton": 2.3549,
    "Les Gobelins": 2.3510,
    "Place d'Italie": 2.3551,
    "Tolbiac": 2.3587,
    "Maison Blanche": 2.3613,
    "Porte d'Italie": 2.3642,
    "Porte de Choisy": 2.3688,
    "Porte d'Ivry": 2.3707,
    "Pierre et Marie Curie": 2.3742,
    "Mairie d'Ivry": 2.3761,
    "Le Kremlin-Bicêtre": 2.3628,
    "Villejuif Léo Lagrange": 2.3638,
    "Villejuif Paul Vaillant-Couturier": 2.3649,
    "Villejuif - Louis Aragon": 2.3681
}

def add_not_available(lst):
    if len(lst) == 1:
        lst.append(lst[0] + 7)
    return lst


def create_map(df):

    df['latitude'] = df['StopPointName'].map(latitudes)
    df['longitude'] = df['StopPointName'].map(longitudes)
    df['Waiting_time'] = df['Waiting_time'].apply(literal_eval)
    df['Waiting_time'] = df['Waiting_time'].apply(add_not_available)
    mean_lat = df['latitude'].mean()
    mean_lon = df['longitude'].mean()

    m = folium.Map(location=[mean_lat, mean_lon], zoom_start=12)

    for idx, row in df.iterrows():
        popup_content = f"""
        <table style="width:100%; border-collapse: collapse;">
          <tr style="background-color: #f2f2f2;">
            <th style="border: 1px solid black; padding: 5px;">Stop</th>
            <th style="border: 1px solid black; padding: 5px;">Next Train</th>
            <th style="border: 1px solid black; padding: 5px;">Second Next Train</th>
          </tr>
          <tr>
            <td style="border: 1px solid black; padding: 5px;">{row['StopPointName']}</td>
            <td style="border: 1px solid black; padding: 5px;">{row['Waiting_time'][0]} min</td>
            <td style="border: 1px solid black; padding: 5px;">{row['Waiting_time'][1]} min</td>
          </tr>
        </table>
        """
        iframe = IFrame(popup_content, width=300, height=120)
        popup = folium.Popup(iframe, max_width=2650)
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=popup,
            icon=folium.Icon(icon='info-sign', color='blue')
        ).add_to(m)

    return m


def data_show(df):
    df["Stop"] = df["StopPointName"]
    df['Waiting_time'] = df['Waiting_time'].apply(add_not_available)
    df['Next Train'] = df['Waiting_time'].apply(lambda x: x[0] if len(x) > 0 else None)
    df['Second Next Train'] = df['Waiting_time'].apply(lambda x: x[1] if len(x) > 0 else None)
    df = df[["Stop", "Next Train", "Second Next Train"]]
    return df
