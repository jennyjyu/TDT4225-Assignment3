import re
from haversine import haversine


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
    pass

    # join on user_id
    # count gorup by user_id


"""
3. Find the top 20 users with the highest number of activities.
"""


def TopNUsersMostActivities(program, n):
    activity_collection = program.db.Activity
    topNUsers = activity_collection.aggregate([
        {'$group': {'_id': "$user", 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}},
        {'$limit': n}
    ])

    for count in topNUsers:
        print("User:    " + str(count['_id']) + "    |     "
              "# of activities:  " + str(count['count']) + ".")


"""
4. Find all users who have taken a taxi.
"""


def UsersTakeTaxi(program):
    user_collection = program.db.User
    users = user_collection.find()

    activity_collection = program.db.Activity
    users_with_taxi = activity_collection.find(
        {'transportation_mode': 'taxi'}, {'user': 1}).distinct("user")

    print("Users who took taxi:")
    for user in users_with_taxi:
        print("|    User:       " + str(user) + "     |")
    print("\n\nTotal number of users who have taken taxi: " +
          str(len(users_with_taxi)))


"""
5. Find all types of transportation modes and count how many activities that are
    tagged with these transportation mode labels. Do not count the rows where the
    mode is null.
"""


def TypesAndAmountofTransportationModes(program):
    activity_collection = program.db.Activity

    mode_count = activity_collection.aggregate([{'$match': {'transportation_mode': {'$not': re.compile(
        '-')}}}, {'$group': {'_id': '$transportation_mode', 'count': {'$sum': 1}}}])
    print("Count pr mode: ")
    for mode in mode_count:
        print("{}: {}".format(mode['_id'], mode['count']))


"""
6. a) Find the year with the most activities.
    b) Is this also the year with most recorded hours?
"""


def YearMostActivities(program):
    trackpoint_collection = program.db.TrackPoint
    activities_by_year = trackpoint_collection.aggregate(
        [{'$group': {'_id': {'year': {'$substr': ['$date_time', 0, 4]}, 'activity_id': '$activity_id'}}}, {'$group': {'_id': '$_id.year', 'count': {'$sum': 1}}}])
    print(activities_by_year)
    for year in activities_by_year:
        print("{}: {}".format(year['_id'], year['count']))


def YearMostRecordedHours(program):
    activity_collection = program.db.Activity
    hours_by_year = activity_collection.aggregate(
        [{'$group': {'_id': {'year': {'$substr': ['$start_date_time', 0, 4]}}, 'count': {'$sum': {'$divide': [{'$subtract': [
            {'$toDate': '$end_date_time'}, {'$toDate': '$start_date_time'}]}, 60 * 1000 * 60]}}}}, {'$sort': {'count': -1}}]
    )
    for line in hours_by_year:
        print("The year with the most acitivites was {}, with {} hours".format(
            line['_id']['year'], int(line['count'])))
        break


"""
7. Find the total distance (in km) ​walked​ in 2008, by user with id=112.
"""


def DistanceWalked(program, year, user):
    activity_collection = program.db.Activity
    activites_from_112 = list(
        activity_collection.find({'user': '112'}, {'_id': 1}))
    activites_from_112 = [i['_id'] for i in activites_from_112]
    trackpoint_collection = program.db.TrackPoint
    trackpoints_112 = trackpoint_collection.find(
        {'date_time': re.compile('^(2008)(.*)'), 'activity_id': {'$in': activites_from_112}}, {'lat': 1, 'lon': 1, 'activity_id': 1})

    tot_dist = 0
    for i in range(trackpoints_112.count() - 1):
        # Check if the trackpoints is in the same activity
        if (trackpoints_112[i]['activity_id'] == trackpoints_112[i + 1]['activity_id']):
            # Haversine calculate distance between latitude longitude pairs
            from_tuple = (trackpoints_112[i]['lat'], trackpoints_112[i]['lon'])
            to_tuple = (trackpoints_112[i+i]['lat'],
                        trackpoints_112[i+1]['lon'])
            dist = haversine(from_tuple, to_tuple)
            tot_dist += dist

    print("Total distance walked by user 112 in 2008: ", tot_dist)


"""
8. Find the top 20 users who have gained the most altitude ​meters​.
    ○ Output should be a table with (id, total meters gained per user).
    ○ Remember that some altitude-values are invalid
    ○ Tip: ∑(tp n .altitude − tp n−1 .altitude), tp n .altitude > tp n−1 .altitude
"""


def TopNUsersMostAltitude(program, n):

    users_altitudes = program.db.TrackPoint.aggregate([
        {'$match': {'altitude': {'$gt': 0}}},
        {'$lookup': {'localField': 'activity_id',
                     'from': 'Activity',
                     'foreignField': '_id',
                     'as': 'activityInfo'
                     }},
        {'$unwind': '$activityInfo'},
        {'$project': {'_id': 0,
                      'altitude': 1,
                      'activityInfo.user': 1
                      }},
    ], allowDiskUse=True)

    # output: [{'altitude': 291.0, 'activityInfo': {'user': '000'}}]

    print('DONE')

    user_totol_altitude = {}

    for user_alt in users_altitudes:

        user = user_alt['activityInfo']['user']
        altitude = user_alt['altitude']

        if user in user_totol_altitude.keys():
            comp_altitude = user_totol_altitude[user][1]
            if altitude > comp_altitude:
                old_total = user_totol_altitude[user][0]
                new_total = old_total + (altitude-comp_altitude)
                user_totol_altitude[user][0] = new_total
            user_totol_altitude[user][1] = altitude

        else:
            user_totol_altitude[user] = [0, altitude]

    sorted_user_total_altitude = {k: v for k, v in sorted(
        user_totol_altitude.items(), key=lambda item: item[1][0])}
    top_20 = sorted_user_total_altitude[:20]
    print('********** USERS WITH THE MOST GAINED ALTITUDES **********' + '\n')
    for user in top_20.keys():
        print('User: ' + str(user) + ' with altitude: ' +
              str(top_20[user][0]))


"""
9. Find all users who have invalid activities, and the number of invalid activities per user
    ○ An invalid activity is defined as an activity with consecutive trackpoints where
        the timestamps deviate with at least 5 minutes.
"""


def UsersAmountOfInvalidActivities(program):

    invalid_activities_per_user = {}

    trackpoints_for_act = program.db.TrackPoint.aggregate([
        {'$group': {
            '_id': '$activity_id',
            'datetimes': {'$push': '$date_time'}}
         }], allowDiskUse=True)

    invalid_activities = []

    for activity_times in trackpoints_for_act:
        activity_id = activity_times['_id']
        times = activity_times['datetimes']
        deviations = 0
        for i, time in enumerate(times):
            if deviations >= 10:
                invalid_activities.append(activity_id)
                break

            if i == len(times)-1:
                break

            if abs((int(times[i+1][17:]) - int(time[17:]))) >= 5:
                deviations += 1
            else:
                deviations = 0

    activities_for_user = program.db.Activity.aggregate([
        {'$group': {
            '_id': '$user',
            'activities': {'$push': '$_id'}}
         }], allowDiskUse=True)

    for user_actvities in activities_for_user:
        user = user_actvities['_id']
        activities = user_actvities['activities']
        amount_invalid = 0
        for activity in activities:
            if activity in invalid_activities:
                amount_invalid += 1

        if amount_invalid > 0:
            invalid_activities_per_user[user] = amount_invalid

    print('******** INVALID ACTVITIES PER USER ********')
    for user in invalid_activities_per_user.keys():
        invalid_activities = invalid_activities_per_user[user]
        print('User: ' + str(user) + ', invalid activities: ' +
              str(invalid_activities))


"""
10. Find the users who have tracked an activity in the Forbidden City of Beijing.
    ○ In this question you can consider the Forbidden City to have coordinates
        that correspond to: ​lat ​39.916, lon 116.397.
"""


def UsersActivityWithCoordinates(program):

    trackpoints_for_act = program.db.TrackPoint.aggregate([
        {'$project': {
            'activity_id': 1,
            'lat': {'$round': ['$lat', 3]},
            'lon': {'$round': ['$lon', 3]},
        }},
        {'$match': {
            'lat': 39.916,
            'lon': 116.397}
         },
        {'$lookup': {
            'localField': 'activity_id',
            'from': 'Activity',
            'foreignField': '_id',
            'as': 'activityInfo'
        }},
        {'$project': {
            '_id': 0,
            'activityInfo.user': 1
        }},
        {'$group': {
            '_id': '$activityInfo.user'
        }
        },
    ], allowDiskUse=True)

    activities_forbidden_city = []

    print('********* Users who have tracked activities in the Forbidden City*********')
    for user in trackpoints_for_act:
        print('User: ' + str(user['_id']) +
              ' has tracked an activity in the forbidden city.')


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

    users_modes = program.db.Activity.aggregate([
        {'$project': {
            'user': 1,
            'transportation_mode': 1,
        }},
        {'$match': {
            'transportation_mode': {'$ne': '-'}
        }},
        {'$group': {
            '_id': '$user',
            'modes': {'$push': '$transportation_mode'}
        }},
    ], allowDiskUse=True)

    user_mode_count = {}
    # {000: {bus: 10, bike: 3, walk: 1... train: 0}, 001: {walk: 1024, bus: 345, plain: 32...}}

    for user_modes in users_modes:
        user = user_modes['_id']
        modes = user_modes['modes']

        for mode in modes:
            if user in user_mode_count.keys():
                modes_count = user_mode_count[user]
                if mode in modes_count.keys():
                    modes_count[mode] += 1
                else:
                    modes_count[mode] = 1
            else:
                user_mode_count[user] = {mode: 1}

    print('******** USERS AND THEIR MOST POPULAR TRANSPORTATION MODE ********')
    for user in sorted(user_mode_count.keys()):
        sorted_modes = sorted(
            user_mode_count[user].items(), key=lambda tup: tup[1])
        most_used_mode = sorted_modes[-1]
        print('User: ' + str(user) + ' has used: ' +
              str(most_used_mode[0]) + ' ' + str(most_used_mode[1]) + ' times.')
