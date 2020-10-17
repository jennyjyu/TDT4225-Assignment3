from pprint import pprint 
from DbConnector import DbConnector
import os



class Program:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

        # Global variables
        # Read id for each person
        self.subfolders = [ f.name for f in os.scandir("dataset/Data") if f.is_dir() ]
        self.subfolders.sort()
        self.ids = tuple(self.subfolders)
        # Set of all users that are labeled
        self.labeled = set(self.file_reader("dataset/labeled_ids.txt", False))

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)

    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)
        

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

        
    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)

   
    def file_reader(self, filepath, read_trajectory):
        f = open(filepath, "r")
        file = []
        if read_trajectory: #Getting the values if it is a trajectory
            for line in f:
                file.append(line.strip())
            f.close()
            first_point = file[6].strip()
            last_point = file[-1]
            start_time = first_point[-19:].replace(',', ' ')
            end_time = last_point[-19:].replace(',', ' ')
            return start_time, end_time
        else:
            for line in f:
                file.append(line.strip())
            f.close()
            return file



    def read_labels(self, filepath):
        file = self.file_reader(filepath, False)
        dict = {} # start_time : transportation_mode
        for line in file:
            transportation_mode = line[40:].strip()
            start_date_time = line[0:20].strip().replace("/", "-")
            dict[start_date_time] = transportation_mode
        return dict

   
    def file_reader_trackpoint(self, filepath, activity_id):
        f = open(filepath, "r")
        i=0
        trackpoints = []
        for line in f:
            trackpoint_doc = {}
            i+=1
            if i>6:
                trackpoint = line.split(",")

                trackpoint_doc['activity_id'] = activity_id
                trackpoint_doc['lat']=float(trackpoint[0]) 
                trackpoint_doc['lon']=float(trackpoint[1]) 
                trackpoint_doc['altitude']=float(trackpoint[3]) 
                trackpoint_doc['date_days']=float(trackpoint[4]) 
                trackpoint_doc['date_time'] = trackpoint[5].strip() + " " + trackpoint[6].strip() 


                trackpoints.append(trackpoint_doc)

        return trackpoints


    def insert_data(self):
        activity_id = 1

        user_collection = self.db['User']
        activity_collection = self.db['Activity']
        trackpoint_collection = self.db['TrackPoint']


        for id in self.subfolders:
            if id in self.labeled:
                query = {
                    "_id": id, 
                    "is_labeled": 1
                    }
            else:
                query = {
                    "_id": id, 
                    "is_labeled": 0
                    }
            user_collection.insert(query)            
            print("Inserted user %s" % id)
            
            # Going through the files for non-labeled users
            if id not in self.labeled:
                labeled = False
            else:
                labeled = True
            for (root, dirs, files) in os.walk('dataset/Data/'+ id, topdown=False):
                for file in files:
                    # Ignoring .-files, labels.txt and files with more than 2500 trackpoints
                    if not file.startswith('.') and file != 'labels.txt' and self.count_lines(os.path.join(root, file)) < 2506:
                        # Saving data
                        start_date_time, end_date_time = self.file_reader(os.path.join(root, file), True)
                        if labeled:
                            labels = self.read_labels('dataset/Data/' + id + '/labels.txt')
                            if start_date_time in labels:
                                transportation_mode = labels.get(start_date_time)
                            else:
                                transportation_mode = '-'
                        else:
                            transportation_mode = '-'
                        query = {
                            '_id': activity_id,
                            'transportation_mode': transportation_mode,
                            'start_date_time': start_date_time,
                            'end_date_time': end_date_time,
                            'user': id, # Might need to add ObjectId?

                        }
                        activity_collection.insert(query)                        
                        
                        trackpoints = self.file_reader_trackpoint(os.path.join(root, file), activity_id)
                        trackpoint_collection.insert_many(trackpoints)
                        print("ACTIVITY IS INSERTED:", activity_id)
                        activity_id += 1

                    else:
                        continue
    @staticmethod
    def count_lines(file):
        with open(file) as f:
            for i, l in enumerate(f):
                pass
        return i + 1                    


def main():
    program = None
    try:
        program = Program()
        
        #Drop collections (if exists?)
        #program.drop_coll(collection_name='User')
        #program.drop_coll(collection_name='Activity')
        #program.drop_coll(collection_name='TrackPoint')
        #program.drop_coll(collection_name='Trackpoint')

        #program.create_coll(collection_name="User")
        #program.create_coll(collection_name="Activity")
        #program.create_coll(collection_name="TrackPoint")
        #program.show_coll()

        #program.insert_data()
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()