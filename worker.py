#WORKER CODE TO RUN ON EC2 INSTANCE (WORKER) OF THE CLUSTER

import boto3

# Create a new SQS client
sqs = boto3.client('sqs')
# Set the URL of the queue from which we will read the message
work_queue_url = 'https://sqs.us-east-1.amazonaws.com/160116022790/workQueue'

# Set the URL of the queue to which we will re-send the message
results_queue_url = 'https://sqs.us-east-1.amazonaws.com/160116022790/resultsQueue'
messages=[]
id=0

def add_matrices(matrix1, matrix2):
    """
    Add two matrices together using native Python lists
    :param matrix1: The first matrix
    :param matrix2: The second matrix
    :return: The sum of the two matrices
    """
    # Check if the matrices are the same size
    if len(matrix1) != len(matrix2) or len(matrix1[0]) != len(matrix2[0]):
        return False

    # Create a result matrix with the same size as the input matrices
    result = [[0 for j in range(len(matrix1[0]))] for i in range(len(matrix1))]

    # Add each element in the matrices
    for i in range(len(matrix1)):
        for j in range(len(matrix1[0])):
            result[i][j] = matrix1[i][j] + matrix2[i][j]

    return result

def mul_matrices(matrix_a,matrix_b):
    """
    Multiply two matrices together using native Python lists
    :param matrix_a: The first matrix
    :param matrix_b: The second matrix
    :return: The product of the two matrices
    """
    result = []

    # Loop through the rows of matrix_a
    for i in range(len(matrix_a)):
        # Initialize a row in the result matrix to an empty list
        result_row = []

        # Loop through the columns of matrix_b
        for j in range(len(matrix_b[0])):
            # Initialize a variable to store the dot product of the current row
            # of matrix_a and the current column of matrix_b
            dot_product = 0

            # Loop through the elements in the current row of matrix_a and
            # the current column of matrix_b and compute the dot product
            for k in range(len(matrix_a[0])):
                dot_product += matrix_a[i][k] * matrix_b[k][j]

            # Add the dot product to the current row of the result matrix
            result_row.append(dot_product)

        # Add the current row of the result matrix to the result matrix
        result.append(result_row)

    return result

def main():
    """
    Main function of the worker that reads a message from the work queue, performs the requested operation, and sends the result to the results queue
    """
    messages=[]
    id=0

    while True:
    # Read a message from the queue
        response = sqs.receive_message(
            QueueUrl=work_queue_url,
            AttributeNames=['All'],
            MaxNumberOfMessages=1,
            VisibilityTimeout=30,
            WaitTimeSeconds=0
        )

        if 'Messages' in response:

            dict =eval(response["Messages"][0]['Body'])

            if dict['op']=='+': #addition
                value= add_matrices(dict['A'], dict['B'])
                messages.append({
                    'Id':str(dict['index']),
                    'MessageBody':str({
                        'value': value,
                        'index': dict['index'],
                        'op':dict['op']
                    })
                })

            if dict['op']=='*': #multiplication
                value=mul_matrices(dict['A'],dict['B'])
                messages.append({
                    'Id':str(dict['index'][0]),
                    'MessageBody':str({
                        'value': value,
                        'index': dict['index'],
                        'op':dict['op']
                    })
                })

            # Delete the message from the queue
            sqs.delete_message(
                    QueueUrl=work_queue_url,
                    ReceiptHandle=response['Messages'][0]['ReceiptHandle']
            )

            id+=1
            if id>=1:
                sqs.send_message_batch(
                    QueueUrl=results_queue_url,
                    Entries=messages
                )
                id=0
                messages=[]


if __name__ == '__main__':
    main()
