import re


"""
1. How many users, activities and trackpoints are there in the dataset 
    (after it is inserted into the database).
"""

def NumberOfUsersActivitiesTrackpoints(program):
    user_collection = program.db.User
    users = user_collection.find({})
    print("Number of users: ", users.count())

    activity_collection = program.db.Activity
    activities = activity_collection.find({})
    print("Number of activities: ", activities.count())
    
    trackpoint_collection = program.db.TrackPoint 
    trackpoints = trackpoint_collection.find({})
    print("Number of trackpoints: ", trackpoints.count())   
    

"""
2. Find the average number of activities per user.
"""

def AverageNumberOfActivities(program):
    user_collection = program.db.User
    users = user_collection.find({})

    activity_collection = program.db.Activity
    activities = activity_collection.find({}, {'user':1})

    per_user = {}
    for user in users:
        if user in activities:
            per_user[user] = 0

    for activity



    #join on user_id
    #count gorup by user_id
    



"""
3. Find the top 20 users with the highest number of activities.
"""
def TopNUsersMostActivities(program, n):
    activity_collection = program.db.Activity
    topNUsers = activity_collection.aggregate([
    { '$group': { 'user': "$Activity", 'count': { '$sum': 1 } } },
    { '$sort': { 'count': -1 } }
  ])
    

"""
4. Find all users who have taken a taxi.
"""
def UsersTakeTaxi(program):
    user_collection = program.db.User
    users = user_collection.find()

    activity_collection = program.db.Activity
    users_with_taxi = activity_collection.find({'transportation_mode': 'taxi'}, {'user': 1}).distinct("user")
    
    print("Users who took taxi:")
    for user in users_with_taxi:
        print("|    User:       " + str(user) + "     |")
    print("\n\nTotal number of users who have taken taxi: " + str(len(users_with_taxi)))


"""
5. Find all types of transportation modes and count how many activities that are
    tagged with these transportation mode labels. Do not count the rows where the
    mode is null.
"""
def TypesAndAmountofTransportationModes(program):
    activity_collection = program.db.Activity
    transportation_modes = activity_collection.find({'transportation_mode': { '$not': re.compile('-') }}, {'transportation_mode': 1}).distinct("transportation_mode")

    mode_count = activity_collection.aggregate([{'$group': { '_id': '$transportation_mode', 'count': {'$sum': 1}}}]) 

    print("Transportation modes", transportation_modes)
    print("Count pr mode: ", mode_count)


"""
6. a) Find the year with the most activities.
    b) Is this also the year with most recorded hours?
"""
def YearMostActivities(program):
    pass

def YearMostRecordedHours(program):
    pass


"""
7. Find the total distance (in km) ​walked​ in 2008, by user with id=112.
"""
def DistanceWalked(program, year, user):
    pass



"""
8. Find the top 20 users who have gained the most altitude ​meters​.
    ○ Output should be a table with (id, total meters gained per user).
    ○ Remember that some altitude-values are invalid
    ○ Tip: ∑(tp n .altitude − tp n−1 .altitude), tp n .altitude > tp n−1 .altitude
"""
def TopNUsersMostAltitude(n):
    pass


"""
9. Find all users who have invalid activities, and the number of invalid activities per user
    ○ An invalid activity is defined as an activity with consecutive trackpoints where 
        the timestamps deviate with at least 5 minutes.
"""
def UsersAmountOfInvalidActivities(program):
    pass


"""
10. Find the users who have tracked an activity in the Forbidden City of Beijing.
    ○ In this question you can consider the Forbidden City to have coordinates
        that correspond to: ​lat ​39.916, lon 116.397.
"""
def UsersActivityWithCoordinates(program):
    pass



"""
11. Find all users who have registered transportation_mode and their most used
    transportation_mode.
    ○ The answer should be on format (user_id,
        most_used_transportation_mode) sorted on user_id.
    ○ Some users may have the same number of activities tagged with e.g. walk
        and car. In this case it is up to you to decide which transportation mode
        to include in your answer (choose one).
    ○ Do not count the rows where the mode is null.
"""
def UsersMostUsedTransportationMode(program):
    pass