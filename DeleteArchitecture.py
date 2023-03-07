#SCRIPT TO DELETE CLUSTER ARCHITECTURE ON AWS

# In[1]:


import boto3


# In[2]:

#load the ec2 client
ec2 = boto3.resource('ec2')


# In[3]:


def delete_instances():
    """
    Function deleting all the current instances running
    """
    ids = []
    for instance in ec2.instances.all():
        if instance.public_ip_address != None:
            ids.append(instance.id)
    ec2.instances.filter(InstanceIds = ids).terminate()


# In[4]:

def main():
    """
    Main function of the program that deletes all the instances running
    """
    _input = input("Please confirm that you want to delete the AWS architecture.\n Press Y when ready:")
    while _input!="Y":
        _input = input("Please confirm that you want to delete the AWS architecture.\n Press Y when ready:")
    delete_instances()
    print("Done! All instances deleted")

if __name__ == "__main__":
    main()

