# # Key-value data store

# 1. importing necessary things
import redis
import pickle
import json
import os

# 2. and connecting to localhost
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

if __name__ == "__main__": 
    while(True):
        # os.system('cls')
        print("---------------- MENU ----------------")
        print("1. Insert\n2. Read\n3. Delete\n4. Check time remainnig\n5. Exit\n")
        choice = int(input('$ '))
        if(choice==1):
            # ### Insert record in database
            key = input('\nEnter key: ')

            # specify nu/mber of attributes for json file
            n = int(input('\nEnter number of attributes: '))

            value = {}
            # for each attributes: take attribute name and its value
            for i in range(n):
                attribute = input(f'\nEnter attribute-{i+1} name: ')
                val = input(f'Enter attribute-{i+1} value: ')
                value.update({attribute : val})
            # value is python dictionary that will be converted in json object

            # ttl should have value 'y' or 'n'
            ttl = input('\nKey supports TTL? y/n: ')

            if(ttl=='y'):
                expiry = input('Enter validity in seconds: ')
                create(key, value, True, expiry)
            elif(ttl=='n'):
                create(key, value, False, -1)

        elif(choice==2):
            # ### Search for a key
            key = input('\nEnter key to search: ')

            value = read(key)

            if(value==None):
                print('>> Key not exists')
            else:
                p_value = pickle.loads(value)
                print(p_value)

        elif(choice==3):
            # ### Delete record
            key = input('\nEnter key of object to delete it: ')

            status = delete(key) # if provided key exists delete() will return True otherwise False

            if(status==True):
                file_name = key+'.json' # retreiving file name
                # necessary while trying delete file of ttl redis record
                if(os.path.isfile(file_name)):
                    os.remove(file_name) # deleting json file
                print('>> Record removed')
            else:
                print('>> Record/Key not found')

        elif(choice==4):
            # ### Check time remaining for key
            key = input('\nEnter key check validity of key: ')

            seconds = validity(key)
            if(seconds==-1):
                print(f">> {key} does not have expiry timeout")
            elif(seconds==-2):
                print(f">> {key} does not exists")
            else:
                print(f">> {seconds}s")

        else:
            break;
        print("\n\n")
    # while complete
    
    # Save database
    print('>> saving to database...')
    r.bgsave

    # ## Critical zone
    # Below cell will remove all the records and json files as well!
    print('---------------- ATTENTION: Flush all? (y/n) ----------------')
    choice = input()

    if(choice=='y'):
        r.flushdb()
        current_directory = os.getcwd() # because all json files are saved in current directory 
        for f in os.listdir(current_directory):
            # other files should not be deleted
            if not f.endswith(".json"):
                continue
            os.remove(os.path.join(current_directory, f))
        print('>> Data cleared')
    else:
        print('>> Action undone')