import os, json
import pandas as pd
import pprint

class Extract_Flights:
    # init method or constructor
    def __init__(self, file):
        self.file = file

    def extract_data(self):
        if os.path.exists(self.file):
            # Opening JSON file
            f = open(self.file)
            # Load JSON into object
            data = json.load(f)
            # Close file
            f.close()
            
            # Extract Header Data
            id = data['id']
            start_time = data['centre_ctrl'][0]['start_time']
            aircraft_type = data['fpl']['fpl_base'][0]['aircraft_type']
            flight_rules = data['fpl']['fpl_base'][0]['flight_rules']
            #print("id: ", id, " unique_aircraft_type: ",unique_aircraft_type, " unique_aircraft_type: ", unique_flight_rules)

            #Extract Item Data
            df = pd.DataFrame(data['plots'])
            df_final = pd.DataFrame()
            try:
                df['plots_altitude'] = df['I062/380'].apply(lambda x: x['subitem7']['altitude'] if isinstance(x, dict) and 'subitem7' in x and 'altitude' in x['subitem7'] else None)
                df['plots_baro_vert_rate'] = df['I062/380'].apply(lambda x: x['subitem13']['baro_vert_rate'] if isinstance(x, dict) and 'subitem13' in x and 'baro_vert_rate' in x['subitem13'] else None)
                df['plots_mach'] = df['I062/380'].apply(lambda x: x['subitem27']['mach'] if isinstance(x, dict) and 'subitem27' in x and 'mach' in x['subitem27'] else None)
                df['plots_measured_flight_level'] = df['I062/136'].apply(lambda x: x['measured_flight_level'] if isinstance(x, dict) and 'measured_flight_level' in x else None)
            except Exception as e:
                for col in df.columns:
                    if 'altitude' in df[col].values:
                        df['plots_altitude'] = df[col].apply(lambda x: x['subitem7']['altitude'] if isinstance(x, dict) and 'subitem7' in x and 'altitude' in x['subitem7'] else None)
                    elif 'baro_vert_rate' in df[col].values:
                        df['plots_baro_vert_rate'] = df[col].apply(lambda x: x['subitem13']['baro_vert_rate'] if isinstance(x, dict) and 'subitem13' in x and 'baro_vert_rate' in x['subitem13'] else None)
                    elif 'mach' in df[col].values:
                        df['plots_mach'] = df[col].apply(lambda x: x['subitem27']['mach'] if isinstance(x, dict) and 'subitem27' in x and 'mach' in x['subitem27'] else None)
                    elif 'measured_flight_level' in df[col].values:
                        df['plots_measured_flight_level'] = df[col].apply(lambda x: x['measured_flight_level'] if isinstance(x, dict) and 'measured_flight_level' in x else None)
            df_final = df[['plots_altitude', 'plots_baro_vert_rate', 'plots_mach', 'plots_measured_flight_level', 'time_of_track']]
            # print(df_final.shape)
                        
            total_row = df_final.shape[0]
            # print("total row", total_row)
            if(total_row == 0):
                raise Exception("no data found, please check manually if it has correct format") 
            else:
                df_final = df_final.copy() # This is to remove warning
                df_final['time_of_track'] = pd.to_datetime(df_final['time_of_track'], errors='coerce')
                df_final['start_time'] = start_time
                df_final['start_time'] = pd.to_datetime(df_final['start_time'])
                df_final['id'] = id
                df_final['aircraft_type'] = aircraft_type
                df_final['flight_rules'] = flight_rules
                # df_final.loc[:, 'id'] = id
                # df_final.loc[:, 'aircraft_type'] = aircraft_type
                # df_final.loc[:, 'flight_rules'] = flight_rules
                df_final = df_final[['id', 'aircraft_type', 'flight_rules', 'plots_altitude', 'plots_baro_vert_rate', 'plots_mach', 'plots_measured_flight_level', 'start_time', 'time_of_track']]
                df_final = df_final.drop_duplicates()
                df_cleaned = df_final.dropna(subset=['time_of_track'])

                #return value
                return df_cleaned


# folder_path = 'D:\\00 Project\\00 My Project\\Dataset\\Revalue_Nature\\Case 2\\' # Assuming this where all json file will be stored
# json_files = [pos_json for pos_json in os.listdir(folder_path) if pos_json.endswith('.json')]
# file_name = '100002.json'
# flight_obj = Extract_Flights(folder_path+file_name)
# df = flight_obj.extract_data()
# print(df.dtypes)
# print("Lenght DF:", len(df. index))

# sql_list = []
# for index, row in df.iterrows():
#         sql_values = ", ".join([str(val) if isinstance(val, (int, float)) else f"'{val}'" for val in row])
#         sql_statement = f"INSERT INTO TABLE VALUES ({sql_values});"
#         sql_list.append(sql_statement)
        
# sql_multiple_stmts = "".join(sql_list)
# print(sql_multiple_stmts)