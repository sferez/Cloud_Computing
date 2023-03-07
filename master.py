#MASTER CODE TO RUN ON MASTER INSTANCE (EC2) OF THE ARCHITECTURE

# In[63]:


import boto3
import time
import numpy as np
import sys


# In[2]:

#Create the object to connect to the AWS services
ec2 = boto3.resource('ec2')
ec2_client = boto3.client("ec2")
sqs = boto3.client('sqs')
max_worker =9
sns = boto3.client('sns')
SQS = boto3.resource('sqs')


# In[3]:

#Load the queues
work_queue_url = 'https://sqs.us-east-1.amazonaws.com/160116022790/workQueue'
results_queue_url = 'https://sqs.us-east-1.amazonaws.com/160116022790/resultsQueue'
queue = SQS.get_queue_by_name(QueueName="workQueue")


# In[40]:


def send_add(matrix_a = np.random.randint(10, size=(1000, 1000)),matrix_b = np.random.randint(10, size=(1000, 1000)),size_x=None, size_y=None):
    """
    Send the work to the workers
    :param matrix_a: the first matrix
    :param matrix_b: the second matrix
    :param size_x: the size of the matrix
    :param size_y: the size of the matrix
    :return: the proof of work
    """
    if size_x!=None: #if the size is given
        matrix_a=np.random.randint(10, size=(size_x, size_y))
        matrix_b=np.random.randint(10, size=(size_x, size_y))

    if matrix_b.shape!=matrix_a.shape: #if the matrix are not the same size
        print("Shape of matrix are not the same")
        return

    if len(matrix_b)<100: #if the matrix are too small
        print("Maybe its not worth it to parallelize this work")
        return

    lenght=len(matrix_b)
    max_el=43000 #max number of elements per msg to have a msg size of 230kb
    split=max_el//lenght

    if split>lenght: #if the matrix are too small compare to the split
        split=lenght

    rest=lenght%split
    nbr_msg=len(matrix_a)//split
    msg=0
    print("SENDING")
    id=0
    messages=[]

    # Send the work to the queue
    for i in range(nbr_msg):

        messages.append({
            'Id':str(i),
            'MessageBody':str({
                'A': matrix_a[i*split:i*split+split].tolist(),
                'B': matrix_b[i*split:i*split+split].tolist(),
                'index': i,
                'op':'+'
            })
        })

        # Send the message to the queue
        sqs.send_message(
                QueueUrl=work_queue_url,
                MessageBody=messages[0]['MessageBody']
        )
        id=0
        messages=[]
        msg+=1

        #print progress bar
        z = int(msg / nbr_msg * 25)
        sys.stdout.write('\r')
        sys.stdout.write("[%-25s] %d%%" % ('=' * z, 4 * z))
        sys.stdout.flush()

    # Send the rest of the work
    if rest>0:

        messages.append({
            'Id':str(int(len(matrix_a)/split)),
            'MessageBody':str({
                'A': matrix_a[lenght-rest:lenght].tolist(),
                'B': matrix_b[lenght-rest:lenght].tolist(),
                'index': int(len(matrix_a)/split),
                'op':'+'
            })
        })

        sqs.send_message_batch(
            QueueUrl=work_queue_url,
            Entries=messages
        )
        id=0
        messages=[]

    return matrix_a,matrix_b,split,rest


# In[55]:


def receive_add(proof,start):
    """
    Receive the results from the workers
    :param proof: the proof of work
    :param start: the time when the work was started
    :return: the result
    """
    result = np.zeros(proof[0].shape,dtype=np.int8) #create the result matrix

    msg=0
    nbr_msg = len(proof[0])//proof[2]
    if proof[3]>0:
        nbr_msg+=1

    print("\nRECEIVING")

    while msg<nbr_msg:
        # Receive message from SQS queue
        response = queue.receive_messages(
                QueueUrl=results_queue_url,
                AttributeNames=['All'],
                MaxNumberOfMessages=10,
                VisibilityTimeout=30,
                WaitTimeSeconds=0
            )

        if len(response)>0:
            for resp in response:
                dict =eval(resp.body) #convert the string to a dict

                result[dict["index"]*proof[2]:dict["index"]*proof[2]+proof[2]]=dict['value']

                #delete the message
                sqs.delete_message(
                        QueueUrl=results_queue_url,
                        ReceiptHandle=resp.receipt_handle
                    )
                msg+=1
                z = int(msg / nbr_msg * 25)
                #print progress bar
                sys.stdout.write('\r')
                sys.stdout.write("[%-25s] %d%%" % ('=' * z, 4 * z))
                sys.stdout.flush()

    print("\n****** RESULTS ******")
    compute_time = time.time() - start
    print("TIME : ", compute_time)

    #Verify the result
    if result.tolist()==np.add(proof[0],proof[1]).tolist():
        print("VERIFICATION : Okay")
    else:
        print("VERIFICATION : Not Okay")

    calc_cost(compute_time,len(get_ip_worker()),nbr_msg) #calculate the cost

    # write matrix to file
    with open('Add_result.txt', 'w') as f:
        for item in result.tolist():
            f.write("%s " % item)

    print("RESULT SAVED TO FILE")
    print("****** RESULTS ******")

    return result


# In[42]:


def send_mul(matrix_a = np.random.randint(10, size=(500, 500)),matrix_b = np.random.randint(10, size=(500, 500)),size_x=None, size_y=None):
    """
    Send the work to the workers
    :param matrix_a: the first matrix
    :param matrix_b: the second matrix
    :param size_x: the size of the matrix
    :param size_y: the size of the matrix
    :return: the proof of work
    """
    if size_x!=None: #if the size is given
        matrix_a=np.random.randint(10, size=(size_x, size_y))
        matrix_b=np.random.randint(10, size=(size_x, size_y))

    if matrix_b.shape!=matrix_a.shape: #if the matrix are not the same size
        print("Shape of matrix are not the same")
        return

    if len(matrix_b)<100: #if the matrix are too small
        print("Maybe its not worth it to parallelize this work")
        return

    lenght=len(matrix_b)
    max_el=43000
    split=max_el//lenght

    if split>lenght:
        split=lenght

    rest=lenght%split
    print("SENDING")
    nbr_msg=(len(matrix_a)//split)**2
    msg=0

    id=0
    messages=[]
    # Send the work to the queue
    for i in range(int(len(matrix_a)/split)):

        for j in range(int(len(matrix_b)/split)):

            messages.append({
                'Id':str(i),
                'MessageBody':str({
                    'A': matrix_a[i*split:i*split+split].tolist(),
                    'B': matrix_b[0:,j*split:j*split+split].tolist(),
                    'index': [i,j],
                    'op':'*'
                })
            })

            sqs.send_message(
                    QueueUrl=work_queue_url,
                    MessageBody=messages[0]['MessageBody']
            )
            id=0
            messages=[]
            msg += 1
            z = int(msg / nbr_msg * 25)
            #print progress bar
            sys.stdout.write('\r')
            sys.stdout.write("[%-25s] %d%%" % ('=' * z, 4 * z))
            sys.stdout.flush()

        if rest>0:

            messages.append({
                'Id':str(int(len(matrix_a)/split)),
                'MessageBody':str({
                    'A': matrix_a[i*split:i*split+split].tolist(),
                    'B': matrix_b[0:,lenght-rest:lenght].tolist(),
                    'index': [i,lenght//split],
                    'op':'*'
                })
            })

            sqs.send_message(
                    QueueUrl=work_queue_url,
                    MessageBody=messages[0]['MessageBody']
            )
            id=0
            messages=[]

    if rest>0:

        for j in range(int(len(matrix_b)/split)):

            messages.append({
                'Id':str(j),
                'MessageBody':str({
                    'A': matrix_a[lenght-rest:lenght].tolist(),
                    'B': matrix_b[0:,j*split:j*split+split].tolist(),
                    'index': [lenght//split,j],
                    'op':'*'
                })
            })

            sqs.send_message(
                    QueueUrl=work_queue_url,
                    MessageBody=messages[0]['MessageBody']
            )
            id=0
            messages=[]

        if rest>0:

            messages.append({
                'Id':str(int(len(matrix_a)/split)),
                'MessageBody':str({
                    'A': matrix_a[lenght-rest:lenght].tolist(),
                    'B': matrix_b[0:,lenght-rest:lenght].tolist(),
                    'index': [lenght//split,lenght//split],
                    'op':'*'
                })
            })

            sqs.send_message(
                    QueueUrl=work_queue_url,
                    MessageBody=messages[0]['MessageBody']
            )
            id=0
            messages=[]

    return matrix_a,matrix_b,split,rest


# In[54]:


def receive_mul(proof,start):
    """
    Receive the results from the workers
    :param proof: the proof of work (return [matrix_a,matrix_b,split,rest] of the send_mul function)
    :param start: the time when the work was started
    :return: the result
    """
    result = np.zeros(proof[0].shape,dtype=np.int64) #create the result matrix

    nbr_msg=0
    msg=0
    if proof[3]>0:
        nbr_msg+=(len(proof[0])//proof[2]+1)**2
    else:
        nbr_msg+=(len(proof[0])//proof[2])**2

    print("\nRECEIVING")

    while msg<nbr_msg:
        # Receive message from SQS queue
        response = queue.receive_messages(
                QueueUrl=results_queue_url,
                AttributeNames=['All'],
                MaxNumberOfMessages=10,
                VisibilityTimeout=30,
                WaitTimeSeconds=0
            )

        if len(response)>0:
            for resp in response:
                dict =eval(resp.body)

                result[dict["index"][0]*proof[2]:dict["index"][0]*proof[2]+len(dict['value']),dict["index"][1]*proof[2]:dict["index"][1]*proof[2]+len(dict['value'][0])]=dict['value']

                sqs.delete_message(
                        QueueUrl=results_queue_url,
                        ReceiptHandle=resp.receipt_handle
                    )
                msg+=1
                z=int(msg/nbr_msg*25)
                #print progress bar
                sys.stdout.write('\r')
                sys.stdout.write("[%-25s] %d%%" % ('=' * z, 4 * z))
                sys.stdout.flush()

    print("\n****** RESULTS ******")
    compute_time = time.time() - start
    print("TIME : ", compute_time)

    #Verify the result
    if result.tolist() == np.dot(proof[0],proof[1]).tolist():
        print("VERIFICATION : Okay")
    else:
        print("VERIFICATION : Not Okay")

    calc_cost(compute_time,len(get_ip_worker()),nbr_msg)

    #write matrix to file
    with open('Mul_result.txt', 'w') as f:
        for item in result.tolist():
            f.write("%s " % item)

    print("RESULT SAVED TO FILE")
    print("****** RESULTS ******")

    return result


# In[ ]:


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


# In[59]:


def calc_cost(time,nbr_workers,nbr_msg):
    """
    Function calculating the cost of the computation
    :param time: time of the computation
    :param nbr_workers: number of workers
    :param nbr_msg: number of messages
    :return: cost of the computation
    """
    cost_msg=nbr_msg*2*0.2*0.09/1000 #mean msg size 200kb, Price 0.09c/Go
    cost_master=(time/3600)*0.0464 #0.0464/hour
    cost_workers=(time/3600)*nbr_workers*0.0116 #0.0116/hour/instance
    print("TOTAL COST : %.5f" %sum([cost_msg,cost_master,cost_workers]))
    print("SQS: %.5f | Master: %.5f | Workers: %.5f" %(cost_msg,cost_master,cost_workers))
    return [cost_msg,cost_master,cost_workers]


# In[44]:


def main():
    """
    Main loop of the program that ask which operation we want to and wich size of matrix we want to use for the operation from 100x100 to 40000x40000
    """
    print("Welcome to the matrix calculator")
    print("Please choose the operation you want to do")
    print("1. Addition")
    print("2. Multiplication")
    print("3. Exit")
    choice = input("Enter your choice: ")
    if choice == "1":
        print("Please choose the size of the matrix you want to use")
        print("1. 100 x 100")
        print("2. 500 x 500")
        print("3. 1 000 x 1 000")
        print("4. 5 000 x 5 000")
        print("5. 10 000 x 10 000")
        print("6. Custom")
        print("7. Exit")
        choice = input("Enter your choice: ")
        if choice == "1":
            start = time.time()
            receive_add(send_add(size_x=100,size_y=100),start)
        elif choice == "2":
            start = time.time()
            receive_add(send_add(size_x=500,size_y=500),start)
        elif choice == "3":
            start = time.time()
            receive_add(send_add(size_x=1000,size_y=1000),start)
        elif choice == "4":
            start = time.time()
            receive_add(send_add(size_x=5000,size_y=5000),start)
        elif choice == "5":
            start = time.time()
            receive_add(send_add(size_x=10000,size_y=10000),start)
        elif choice == "6":
            size_x = int(input("Enter the size of the matrix you want to use: "))
            size_y = int(input("Enter the size of the matrix you want to use: "))
            start = time.time()
            receive_add(send_add(size_x=size_x,size_y=size_y),start)
        elif choice == "7":
            print("Returning")
            main()
        else:
            print("Invalid choice")
        main()
    elif choice == "2":
        print("Please choose the size of the matrix you want to use")
        print("1. 100 x 100")
        print("2. 500 x 500")
        print("3. 1 000 x 1 000")
        print("4. 5 000 x 5 000")
        print("5. 10 000 x 10 000")
        print("6. Custom")
        print("7. Return")
        choice = input("Enter your choice: ")
        if choice == "1":
            start = time.time()
            receive_mul(send_mul(size_x=100,size_y=100),start)
        elif choice == "2":
            start = time.time()
            receive_mul(send_mul(size_x=500, size_y=500),start)
        elif choice == "3":
            start = time.time()
            receive_mul(send_mul(size_x=1000,size_y=1000),start)
        elif choice == "4":
            start = time.time()
            receive_mul(send_mul(size_x=5000,size_y=5000),start)
        elif choice == "5":
            start = time.time()
            receive_mul(send_mul(size_x=10000,size_y=10000),start)
        elif choice == "6":
            size_x = int(input("Enter the size of the matrix you want to use: "))
            size_y = int(input("Enter the size of the matrix you want to use: "))
            start = time.time()
            receive_mul(send_mul(size_x=size_x,size_y=size_y),start)
        elif choice == "7":
            print("Returning")
            main()
        else:
            print("Invalid choice")
        main()
    elif choice == "3":
        print("Exiting")
        return

    else:
        print("Invalid choice")
        main()


# In[62]:


if __name__ == "__main__":
    main()






