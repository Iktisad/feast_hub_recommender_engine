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
    # _dataframeVisitCount:int

    def __init__(self):
        self.__prepareDBData()       
        self.__prepareRecommendationCache()

    def __prepareDBData(self):
        # conn = psycopg2.connect(host="localhost",dbname="postgres",user="postgres",password="admin",port=5432)
        self.__conn = psycopg2.connect(host="localhost",dbname="feast_hub_sub",user="iktis",password="",port=5434)
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
        self.__conn = psycopg2.connect(host="localhost",dbname="feast_hub_sub",user="iktis",password="",port=5434)
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
    
    def __matrix(self):
        matrix_data= self.__data.pivot_table(index='userID', columns='placeID' , values='overall_rating')
        return matrix_data

    def __matrixNormalization(self):
        matrix_list= self.__matrix()
        matrix_norm=matrix_list.subtract(matrix_list.mean(axis=1),axis='rows')
        return matrix_norm

    # User Similarity matrix using Pearson's correlation
    def __pearsonCorrelation(self):
        matrix_norm = self.__matrixNormalization()
        user_similarity=matrix_norm.T.corr()
        return user_similarity

    def __recommendRestaurants(self, picked_userID, number_of_similar_user,number_of_top_restaurants):
        pr=self.__pearsonCorrelation()
        picked_userID: str = picked_userID
        #Remove picked user Id from the lsit
        pr.drop(index=picked_userID,inplace=True)

        # Number of similar users
        n:int = number_of_similar_user
        #User similarity threshold
        user_similarity_threshold : float =0.3
        #Get top n similar users
        similar_users = pr[pr[picked_userID]>user_similarity_threshold][picked_userID].sort_values(ascending=False)[:n]

        #Restaurant visited by the target user
        matrix_normalization= self.__matrixNormalization()
        picked_userID_visited = matrix_normalization[ matrix_normalization.index == picked_userID].dropna(axis=1, how='all')

        #Restaurant that similar user visited. Remove restaurants that none of the similar user have visited   
        similar_user_visits = matrix_normalization[matrix_normalization.index.isin(similar_users.index)].dropna(axis=1, how='all')

        # Remove visited restaurant from the list
        similar_user_visits.drop(picked_userID_visited.columns,axis=1, inplace=True, errors='ignore')

        # A dictionary to store item scores
        item_score = {}

        # Loop through items
        for i in similar_user_visits.columns:
            # Get the ratings for restaurant i
            restaurant_rating = similar_user_visits[i]
            # Create a variable to store the score
            total = 0
            # Create a variable to store the number of scores
            count = 0
            # Loop through similar users
            for u in similar_users.index:
                # If the restaurant has rating
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
        return ranked_item_score.head(m)

    def __recommendPopularRestaurants(self):
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
                # print(final_list)
                # final_list=pd.concat([final_list, pd.DataFrame(place_exists.groupby('placeID')['overall_rating'].mean())])
        # print(final_list)
        return final_list
    

    
    def __processRecommendation(self, userID:str):
        
        specificUserData=self.__data.loc[self.__data['userID'] == userID]
        
        
        if (len(specificUserData.index)>2):  
            number_of_similar_user: int =100
            number_of_top_restaurants: int =100
            recommendList= self.__recommendRestaurants(userID,number_of_similar_user,number_of_top_restaurants)
            df1 = recommendList.rename(columns={'place':'placeID'})
            joined_frame = df1.merge(self.__df, on='placeID', how='left')
            #Drop N/A
            joined_frame_without_NAN=joined_frame.dropna()
            result=joined_frame_without_NAN.drop('id', axis=1)
            # print(result.to_json(orient='records', indent=2))
            return result.to_json(orient='records')
        
        pRestaurants = self.__recommendPopularRestaurants()
        # join with restaurants
        popCopyRestaurants = self.__df.copy()
        # print(popCopyRestaurants)
        restaurants =  popCopyRestaurants.merge(pRestaurants, on="placeID", how="right")
        # sort values by asc
        restaurants = restaurants.sort_values(by='overall_rating', ascending=False)
        # drop the id column
        restaurants = restaurants.drop(['id'], axis=1)
        # convert to json response
        # return restaurants.to_json(orient='records', indent=2)
        return restaurants.to_json(orient='records')
    
    def __prepareRecommendationCache(self):
        # {U001: [{placeID:1234, rcuisine:"spanish" placeScore: 0.552}]}
        # prepare recommendation array
        unique_users = self.__data['userID'].unique();
        for user in unique_users:

            self.__recommCache[user] = self.__processRecommendation(user);
        # self.__recommCache['U1067'] = self.getRecommendation('U1067');
        # print(self.__recommCache)
    
    def getRecommendation(self, userID):
        # query db if number of visit count > currentVisitCount
        # print(type(self.__currentDbCount))
        if self.__currentDbCount< self.__dbVisitCount():
            # refresh data
            self.__prepareDBData()
        # check if user is already in recommendation cache
        if userID in self.__recommCache:
            return self.__recommCache[userID]
        return self.__processRecommendation(userID)
    
    
# process: RecommenderEngine = RecommenderEngine()

# print(process.getRecommendation('K1001'))