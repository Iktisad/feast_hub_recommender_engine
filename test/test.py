import psycopg2;
import pandas as pd;
# conn = psycopg2.connect(host="localhost",dbname="postgres",user="postgres",password="admin",port=5432)
conn = psycopg2.connect(host="localhost",dbname="feast_hub_sub",user="iktis",password="",port=5434)
data: pd.DataFrame;

df: pd.DataFrame;
try:
    cur1 = conn.cursor()
    cur2 = conn.cursor()
    cur1.execute('SELECT * FROM users_ratings')
    cur2.execute("SELECT * FROM cusines")
    revisited_restaurants = cur1.fetchall()
    cuisine = cur2.fetchall()
    
    data = pd.DataFrame(revisited_restaurants)
    # Using DataFrame.iloc[] to drop last n columns
    # Drop first column of dataframe using drop()
    data.drop(columns=data.columns[0], axis=1,  inplace=True)
    data = data.iloc[:, :-2]
    # print(data)
    data.columns=['userID','placeID','overall_rating','food_rating','service_rating']
    
    df = pd.DataFrame(cuisine)
    # Using DataFrame.iloc[] to drop last n columns        
    df = df.iloc[:, :-2]
    df.columns=['id','placeID','Rcuisine']
    
    currentDbCount = data.size
    # dataframeVisitCount = data.size
    
except Exception as error:
    print(error)
finally:
    if cur1 is not None:
        cur1.close()
    if cur2 is not None:
        cur2.close()
    if conn is not None:    
        conn.close()
data.loc[data['userID'] == 'U1047']
print(data.loc[data['userID'] == 'U1065'])