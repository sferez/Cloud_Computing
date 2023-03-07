#CODE TO RUN ON SINGLE INSTANCE FOR COMPARISON

import numpy as np
import time

def add_matrices(matrix1, matrix2):
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

    # Print the result
    return result

#ask the user which operation he wants to do and which size of matrices he wants to use and create the matrices then run the chosen operation
def main():
    print("Welcome to the native Python matrix calculator")
    print("Please choose the operation you want to do")
    print("1-Addition")
    print("2-Multiplication")
    operation = int(input("Enter the number of the operation you want to do: "))
    size = int(input("Enter the size of the matrices you want to use: "))
    matrix1 = np.random.randint(10, size=(size, size))
    matrix2 = np.random.randint(10, size=(size, size))
    if operation == 1:
        start = time.time()
        add_matrices(matrix1.tolist(), matrix2.tolist())
        end = time.time()
        print("TIME: ", end - start)
        main()
    elif operation == 2:
        start = time.time()
        mul_matrices(matrix1.tolist(), matrix2.tolist())
        end = time.time()
        print("TIME: ", end - start)
        main()
    else:
        print("Invalid operation")
        main()

if __name__ == "__main__":
    main()