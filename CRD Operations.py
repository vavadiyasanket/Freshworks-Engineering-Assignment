# # Key-value data store

# 1. importing necessary things
# - redis for inerting, reading and deleting records
# - pickle converts python dictionary to json
# - json used to deal with its object
# - os for saving and deleting files
# 
# 2. and connecting to localhost
import redis
import pickle
import json
import os

r = redis.Redis("localhost")


# inserting record to database
#     - key: key for redis record
#     - value: dictionary object
#     - ttl: Time-To-Live for redis record
#     - expiry: validity of redis record if ttl is false this parameter will not be used

# If key is not present then inserting it with ttl(expiry) otherwise givev error message

# Along with inserting record it will create json file in current directory with value as content and key as file name iff ttl is false
def create(key, value, ttl, expiry):
    val = r.get(key) # if val is not None record with provided key already exists, hence will not overwrite it
    if(val==None):
        p_value = pickle.dumps(value)
        if(ttl==True):
            r.set(key, p_value, ex=expiry)
        else:
            r.set(key, p_value)
            # saving json object as file content and key as file name if ttl is False
            file_name = key+'.json'
            with open(file_name, 'w') as json_file:
                json.dump(value, json_file)
    else:
        print('Key already exists')


# Finds key record
def read(key):
    return r.get(key)

# deleting record with specified key
#     - if record not found returns False 
#     - else returns true after removing record from database (json file still exists)


def delete(key):
    value = r.get(key)
    if(value==None):
        return False
    else:
        p_value = pickle.loads(value)
        r.delete(key)
        return True

# Checking remainnig time for perticular record (key-value pair)
#     - returns time in seconds
def validity(key):
    return r.ttl(key)


# ### Insert record in database

print('Enter key: ')
key = input()

print('Enter number of attributes: ') # specify nu/mber of attributes for json file
n = int(input())

value = {}
# for each attributes: take attribute name and its value
for i in range(n):
    print('Enter attribute name: ')
    attribute = input()
    print('Enter attribute value: ')
    val = input()
    value.update({attribute : val})
# value is python dictionary that will be converted in json object

print('Key supports TTL? y/n') # ttl should have value 'y' or 'n'
ttl = input()

if(ttl=='y'):
    print('Enter validity in seconds')
    expiry = input()
    create(key, value, True, expiry)
elif(ttl=='n'):
    create(key, value, False, -1)



# ### Search for a key
print('Enter key to search: ')
key = input()

value = read(key)

if(value==None):
    print('Key not exists')
else:
    p_value = pickle.loads(value)
    print(p_value)


# ### Delete record
print('Enter key of object to delete it: ')
key = input()

status = delete(key) # if provided key exists delete() will return True otherwise False

if(status==True):
    file_name = key+'.json' # retreiving file name
    # necessary while trying delete file of ttl redis record
    if(os.path.isfile(file_name)):
        os.remove(file_name) # deleting json file
    print('Record removed')
else:
    print('Record/Key not found')


# ### Check time remaining for key
print('Enter key check validity of key: ')
key = input()

seconds = validity(key)
if(seconds==-1):
    print(f"{key} does not have expiry timeout")
elif(seconds==-2):
    print(f"{key} does not exists")
else:
    print(f"{seconds}s")


# Save database
print(r.bgsave)


# ## Critical zone
# Below cell will remove all the records and json files as well!
print('Are you sure you want to flush all? y/n')
choice = input()

if(choice=='y'):
    r.flushdb()
    current_directory = os.getcwd() # because all json files are saved in current directory 
    for f in os.listdir(current_directory):
        # other files should not be deleted
        if not f.endswith(".json"):
            continue
        os.remove(os.path.join(current_directory, f))
    print('Data cleared')
else:
    print('Action undone')