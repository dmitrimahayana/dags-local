import os
import pprint
import pandas as pd
from moduleFlights.ExtractFlights import Extract_Flights

class Collect_Flights:
    
    # init method or constructor
    def __init__(self, folder_source, limit):
        self.folder_source = folder_source
        self.limit = limit

    def collect_data(self):
        folder_path = self.folder_source
        json_files = [pos_json for pos_json in os.listdir(folder_path) if pos_json.endswith('.json')]

        counter_file = 0
        max_file = self.limit
        df_final = pd.DataFrame()
        for file_name in json_files:
            if(counter_file < max_file):
                try:
                    flight_obj = Extract_Flights(folder_path+file_name)
                    flight = flight_obj.extract_data()
                    # df_final = df_final.append(flight, ignore_index=True)
                    empty_columns_df = flight.columns[flight.isna().all()].tolist()
                    df_final = pd.concat([df_final, flight.drop(columns=empty_columns_df)], ignore_index=True)
                    print("successfully extract file: ", str(folder_path+file_name))
                except Exception as e:
                    print("error reading file: ", str(folder_path+file_name), str(e))
                counter_file += 1
            elif(counter_file >= max_file):
                break
        
        return df_final

# collect_obj = Collect_Flights('D:\\00 Project\\00 My Project\\Dataset\\Revalue_Nature\\Case 2\\', 10)
# df = collect_obj.collect_data()
# # print(df)

# # Convert SQL Insert Format
# sql_list = []
# for index, row in df.head(2).iterrows():
#         sql_values = ", ".join([str(val) if isinstance(val, (int, float)) else f"'{val}'" for val in row])
#         sql_statement = f"INSERT INTO TABLE VALUES ({sql_values});"
#         sql_list.append(sql_statement)
        
# sql_multiple_stmts = " ".join(sql_list)
# print(sql_multiple_stmts)