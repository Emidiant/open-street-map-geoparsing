import overpy
import pandas as pd

def get_terrian_query(user_input):
    q = user_input[2] + ',' + user_input[0] + ',' + user_input[1]  # (radius,latitude,longitude)
    built_query = f"[out:json][timeout:50];(node(around:{q});relation(around:{q});way(around:{q}););out body;>;out skel qt;"
    return built_query

def extract_nodes_data_from_OSM(built_query, file_path):
    api = overpy.Overpass()
    print(f"Query: {built_query}")
    result = api.query(built_query)
    list_of_node_tags = []
    for node in result.nodes:
        node.tags['latitude'] = node.lat
        node.tags['longitude'] = node.lon
        node.tags['id'] = node.id
        list_of_node_tags.append(node.tags)
    data_frame = pd.DataFrame(list_of_node_tags)
    data_frame.to_csv(file_path)
    return data_frame

if __name__ == '__main__':

    latitude = '59.97897'
    longitude = '30.26889'
    search_radius = '200'
    user_input = [latitude, longitude, search_radius]
    built_query = get_terrian_query(user_input)
    file_path = "data/csv/nodes_responce.csv"
    extract_nodes_data_from_OSM(built_query, file_path)
