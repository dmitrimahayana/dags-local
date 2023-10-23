import pandas as pd
import pprint
from moduleFlights.CollectFlights import Collect_Flights

collect_obj = Collect_Flights('D:\\00 Project\\00 My Project\\Dataset\\Revalue_Nature\\Case 2\\', 10)
df_result = collect_obj.collect_data()
# pp = pprint.PrettyPrinter(indent=2, width=30, compact=True)
# pp.pprint(dict_result)
# df = pd.DataFrame(dict_result)
# print(df_result)
df_max = df_result.groupby('id').max().reset_index()
print(df_max)