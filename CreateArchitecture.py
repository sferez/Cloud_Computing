#SCRIPT TO CREATE CLUSTER ARCHITECTURE ON AWS

# In[20]:


import boto3
import paramiko
from scp import SCPClient
import time
import Config


# In[18]:

ec2 = boto3.resource('ec2')
ec2_client = boto3.client("ec2")
key = paramiko.RSAKey.from_private_key_file(Config.SSH_KEY_PATH)


# In[19]:


def get_ip_worker():
    """
    Function returning the ip of the workers instances
    :return: list containing the ip of the workers instances
    """
    ip = []
    for instance in ec2.instances.all():
        if instance.public_ip_address != None:
            for tags in instance.tags:
                if tags["Key"]=='Name':
                    if tags['Value']=="Worker":
                        ip.append(instance.public_ip_address)

    return  ip


# In[4]:


def get_ip_master():
    """
    Function returning the ip of the master instance
    :return: list containing the ip of the master instance
    """
    ip = []
    for instance in ec2.instances.all():
        if instance.public_ip_address != None:
            for tags in instance.tags:
                if tags["Key"]=='Name':
                    if tags['Value']=="Master":
                        ip.append(instance.public_ip_address)

    return  ip


# In[5]:


def create_workers(number):
    """
    Function creating X number of worker instances from a template instance
    :param number: Number of worker wanted
    """
    if number > Config.MAX_WORKER :
        print("Not Allowed more than 8 workers")
        return 0
    if (len(get_ip_worker())+number)> Config.MAX_WORKER:
        print("Not ALlowed more than 8 workers running at the same time")
        return 0
    lt_specifics = {
    'LaunchTemplateId': Config.TEMPLATE_WORKER, #template of worker
    }
    instances = ec2.create_instances(
            LaunchTemplate=lt_specifics,
            MinCount=1,
            MaxCount=number,
        )


# In[6]:


def create_master():
    """
    Function creating a master instance form a template instance
    """
    if len(get_ip_master()) >1 :
        print("Not Allowed more than 1 master")
        return 0
    lt_specifics = {
    'LaunchTemplateId': Config.TEMPLATE_MASTER, #template of worker
    }
    instances = ec2.create_instances(
            LaunchTemplate=lt_specifics,
            MinCount=1,
            MaxCount=1,
        )


# In[7]:


def wait_until_running():
    """
    Function waiting for all the instances started to be ready
    """
    for instance in ec2.instances.all():
        if instance.public_ip_address != None:
            instance.wait_until_running()


# In[9]:


def scp_file(ips,file):
    """
    Function that will SCP a file to a list of ip addresses
    :param ips: List of ip to scp the file
    :param file: path of the file to scp to the ips
    """

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        for ip in ips:

            client.connect(hostname=ip, username="ec2-user", pkey=key)
            scp = SCPClient(client.get_transport())
            scp.put(files=file,
                remote_path='/home/ec2-user',
                recursive=True)
            client.close()
            print(ip+": Done")
    except:
        print("Failed, Start again in 5s")
        time.sleep(5)
        scp_file(ips,file)


# In[10]:


def run_cmd(ips, cmd,log=False):
    """
    Function running a command a distance on instances
    :param ips: list of ip to run the command
    :param cmd: command to run on the ip
    :param log: print the result of the terminal after running the command
    """
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    for ip in ips:

        client.connect(hostname=ip, username="ec2-user", pkey=key)
        # Execute a command(cmd) after connecting/ssh to an instance
        stdin, stdout, stderr = client.exec_command(cmd)
        if log:
            print(ip+": "+str(stdout.read()))
        # close the client connection once the job is done
        client.close()


# In[11]:


def run_cmd2(ips, cmd):
    """
    Function running a command a distance on instances
    :param ips: list of ip to run the command
    :param cmd: command to run on the ip
    """
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    for ip in ips:

        client.connect(hostname=ip, username="ec2-user", pkey=key)
        # Execute a command(cmd) after connecting/ssh to an instance
        client.exec_command(cmd)
        # close the client connection once the job is done


# In[12]:


def start_setup_workers(number):
    """
    Function running the full set up of the workers instances
    :param number: number of workers wanted
    :return time spend
    """
    start =time.time()
    print("Creating workers...")
    create_workers(number)
    time.sleep(10) #Avoid mistake
    print("Wait until running...")
    wait_until_running()
    time.sleep(25) #Avoid mistake
    print("AWS CLI Configure on workers...")
    scp_file(get_ip_worker(),Config.AWS_CLI_PATH) #Configure aws CLI
    print("SCP Scripts on workers...")
    scp_file(get_ip_worker(),"worker.py")#Scp script
    print("Installing Boto3 on workers...")
    run_cmd(get_ip_worker(),"pip3 install boto3",True) #Run script
    print("Launching scripts on workers...")
    run_cmd2(get_ip_worker(),"python3 worker.py")
    print("Workers Ready!")
    return time.time()-start


# In[13]:


def start_setup_master():
    """
    Function running the full setup of the master instance
    """
    print("Creating master...")
    create_master()
    time.sleep(10) #Avoid mistake
    print("Wait until running...")
    wait_until_running()
    time.sleep(25) #Avoid mistake
    master=get_ip_master()
    print("AWS configure...")
    scp_file(master,Config.AWS_CLI_PATH) #Configure aws CLI
    print("SCP script...")
    scp_file(master,"master.py")#Scp script
    print("Install BOTO3...")
    run_cmd(master,"pip3 install boto3",True) #Run script
    print("Install Numpy...")
    run_cmd(master,"pip3 install numpy",True)
    print("Master ready!")


# In[14]:


# In[15]:

def main():
    """
    Main function that will run the full setup of the cluster
    """
    _input = input(
        "Please be sure to have the correct credential for AWS CLI in your /.aws folder.\n Press Y when ready:")
    while _input != "Y":
        _input = input(
            "Please be sure to have the correct credential for AWS CLI in your /.aws folder.\n Press Y when ready:")

    _input = input("Please enter the amount of worker you want to set up. No more than 8 workers allowed")
    while int(_input)>9 or int(_input) <1:
        _input = input("Please enter the amount of worker you want to set up. No more than 8 workers allowed")
    start_setup_workers(int(_input))
    start_setup_master()
    print("####### START DONE #######")
    print("Master: " + str(get_ip_master()))
    print("Workers: " + str(get_ip_worker()))

if __name__ == "__main__":
    main()



