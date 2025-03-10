import psycopg2;
import pandas as pd;
# import sys;
#import numpy as np;

class RecommenderEngine:
    __conn = None
    __data: pd.DataFrame
    __df:pd.DataFrame
    __recommCache:dict = {}
    __currentDbCount: int
    __merge:pd.DataFrame
    
    # _dataframeVisitCount:int

    def __init__(self):
        self.__prepareDBData()       
        self.__prepareRecommendationCache()

    def __prepareDBData(self):
        # conn = psycopg2.connect(host="localhost",dbname="postgres",user="postgres",password="admin",port=5432)
        self.__conn = psycopg2.connect(host="localhost",dbname="feast_hub_sub",user="USER",password="",port=5434)
        try:
            cur1 = self.__conn.cursor()
            cur2 = self.__conn.cursor()
            cur1.execute('SELECT * FROM users_ratings')
            cur2.execute("SELECT * FROM cusines")
            revisited_restaurants = cur1.fetchall()
            cuisine = cur2.fetchall()
            
            self.__data = pd.DataFrame(revisited_restaurants)
            # Using DataFrame.iloc[] to drop last n columns
            # Drop first column of dataframe using drop()
            self.__data.drop(columns=self.__data.columns[0], axis=1,  inplace=True)
            self.__data = self.__data.iloc[:, :-2]
            # print(data)
            self.__data.columns=['userID','placeID','overall_rating','food_rating','service_rating']
            
            self.__df = pd.DataFrame(cuisine)
            # Using DataFrame.iloc[] to drop last n columns        
            self.__df = self.__df.iloc[:, :-2]
            self.__df.columns=['id','placeID','Rcuisine']
            self.__merge = pd.merge(self.__data, self.__df , on='placeID')
            self.__currentDbCount = self.__data.size
            # self.__dataframeVisitCount = self.__data.size
            
        except Exception as error:
            print(error)
        finally:
            if cur1 is not None:
                cur1.close()
            if cur2 is not None:
                cur2.close()
            if self.__conn is not None:    
                self.__conn.close()

    def __dbVisitCount(self) -> int:
        value: int=0
        self.__conn = psycopg2.connect(host="localhost",dbname="feast_hub_sub",user="USER",password="",port=5434)
        try:
            cur = self.__conn.cursor()
            cur.execute('SELECT COUNT(*) FROM users_ratings')
            value+= cur.fetchone()[0]
           
        except Exception as error:
            print(error)
        finally:
            if cur is not None:
                cur.close()
            if self.__conn is not None:    
                self.__conn.close()
        return value
    

    

    def __recommendRestaurants(self, userID :str , number_of_similar_user: int,number_of_top_restaurants:int):

        # Number of similar users
        n:int = number_of_similar_user
        #User similarity threshold
        user_similarity_threshold : float =0.3
        #Get top n similar users
        
        # User Matrix
        # Create user-item matrix
        matrix = self.__merge.pivot_table(index='userID', columns='placeID', values='overall_rating')
        # Normalize user-item matrix
        matrix_norm = matrix.subtract(matrix.mean(axis=1), axis = 'rows') 
        # User Similarity matrix using Pearson's correlation
        user_similarity = matrix_norm.T.corr(method='pearson')
        #Remove picked user Id from the lsit
        user_similarity.drop(index=userID, inplace=True)     
        # Get top n similar users
        similar_users = user_similarity[user_similarity[userID]>user_similarity_threshold][userID].sort_values(ascending=False)[:n]
        # print(similar_users)      
        # Places that the target user has visited
        userID_visited = matrix_norm[matrix_norm.index == userID].dropna(axis=1, how='all')
        # print(userID_visited)
        #Restaurant that similar user visited. Remove restaurants that none of the similar user have visited   
        similar_user_visits = matrix_norm[matrix_norm.index.isin(similar_users.index)].dropna(axis=1, how='all')
        # Remove visited restaurant from the list
        similar_user_visits.drop(userID_visited.columns,axis=1, inplace=True, errors='ignore')

        # A dictionary to store item scores
        item_score = {}

        # Loop through items
        for i in similar_user_visits.columns:
            # Get the ratings for restaurant i
            restaurant_rating = similar_user_visits[i]
            # Create a variable to store the score
            total:int = 0
            # Create a variable to store the number of scores
            count:int = 0
            # Loop through similar users
            for u in similar_users.index:
                # If the restaurant has rating
                # if pd.isna(restaurant_rating[u]) == False:
                if pd.isna(restaurant_rating[u]) == False:
                    # Score is the sum of user similarity score multiply by the restaurant rating
                    score = similar_users[u] * restaurant_rating[u]
                    # Add the score to the total score for the restaurant so far
                    total += score
                    # Add 1 to the count
                    count +=1
            # Get the average score for the item
            item_score[i] = total / count

        # Convert dictionary to pandas dataframe
        item_score = pd.DataFrame(item_score.items(), columns=['place', 'place_score'])
            
        # Sort the restaurant by score
        ranked_item_score = item_score.sort_values(by='place_score', ascending=False)

        # Select top m restaurant
        m :int  = number_of_top_restaurants
        # ranked_item_score.head(m)
       
            # Average rating for the picked user
        avg_rating = matrix[matrix.index == userID].T.mean()[userID]

        # Calcuate the predicted rating
        ranked_item_score['predicted_rating'] = ranked_item_score['place_score'] + avg_rating
        ranked_item_score['avg_rating'] = avg_rating

        # Take a look at the data
        ranked_item_score.head(m)


        # Take a look at the data
        return ranked_item_score.head(m)

    def __recommendPopularRestaurants(self) -> pd.DataFrame: 
        unique_place_list=self.__df['placeID'].unique().tolist()
        final_list= pd.DataFrame(columns=['placeID','overall_rating'])
        for place in unique_place_list:
            place_exists=self.__data.loc[self.__data['placeID'] == place]
            # drop the columns food, service rating
            place_exists = place_exists.drop(['food_rating', 'service_rating'], axis=1);
            if (len(place_exists.index)>0):
                # print(place)
                new_row = pd.DataFrame(place_exists.groupby('placeID', as_index=False).mean(numeric_only=True))
                # print(new_row)
                final_list = pd.concat([new_row,final_list.loc[:]]).reset_index(drop=True)
                
        # join with restaurants
        cuisines = self.__df.copy()
        # print(cuisines)
        restaurants =  cuisines.merge(final_list, on="placeID", how="right")
        # sort values by asc
        restaurants = restaurants.sort_values(by='overall_rating', ascending=False)
        # drop the id column
        restaurants = restaurants.drop(['id'], axis=1)
        return restaurants
    

    
    def processRecommendation(self, userID:str):
        
        specificUserData: pd.DataFrame =self.__data.loc[self.__data['userID'] == userID]
          
        if (len(specificUserData.index)>0):  
            number_of_similar_user: int =100
            number_of_top_restaurants: int =50
            recommendList: pd.DataFrame = self.__recommendRestaurants(userID,number_of_similar_user,number_of_top_restaurants)
            cusines_type: pd.DataFrame = recommendList.rename(columns={'place':'placeID'})
            joined_frame: pd.DataFrame = cusines_type.merge(self.__df, on='placeID', how='left')
            #Drop N/A
            joined_frame_without_NAN: pd.DataFrame =joined_frame.dropna()
            result: pd.DataFrame =joined_frame_without_NAN.drop('id', axis=1)
            # print(result.to_json(orient='records', indent=2))
            if len(result.index) > 0:
                return result.to_json(orient='records')
            else:
                return self.__recommendPopularRestaurants().to_json(orient='records')
            
        popular_restaurants = self.__recommendPopularRestaurants()
        
        # convert to json response
        # return restaurants.to_json(orient='records', indent=2)
        return popular_restaurants.to_json(orient='records')
    
    def __prepareRecommendationCache(self):
        # {U001: [{placeID:1234, rcuisine:"spanish" placeScore: 0.552}]}
        # prepare recommendation array
        unique_users = self.__data['userID'].unique();
        for user in unique_users:

            self.__recommCache[user] = self.processRecommendation(user)
            # print("user "+ user)
            # print(self.__recommCache[user])
            # print("--------------X----------------")

    
    def getRecommendation(self, userID):
        # query db if number of visit count > currentVisitCount
        # print(type(self.__currentDbCount))
        if self.__currentDbCount< self.__dbVisitCount():
            # refresh data
            self.__prepareDBData()
        # check if user is already in recommendation cache
        if userID in self.__recommCache:
            return self.__recommCache[userID]
        return self.processRecommendation(userID)
    
    
# process: RecommenderEngine = RecommenderEngine()

# print(process.getRecommendation('U1001'))
# print(process.processRecommendation('U1047'))