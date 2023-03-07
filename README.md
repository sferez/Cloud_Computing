
# Cloud Computing AWS

Project using cloud computing and AWS to build a master-worker architecture for matrix computation. 


## Demo

Guide to deploy, use and delete the project.

- Open Config.py and verify the constants variables. You may want to adapt the path of your /.aws file or change path of the ssh key.

- Once the Config.py is ready launch CreateArchitectre.py and select the number of worker you want in your achitecture (A maximum of 8 workers is allowed).

- After the fully setup of the architecture you will see in the terminal the IP of the master instance, you can now connect by ssh to this instance.

- In the master instance run master.py and start to use the matrix computation program to distribute your computations task on severals workers instances.

- After each computation the result will be saved in a file, Addresult.txt if it was an addition or Mulresult.txt for a multiplication.

- When you are done you can exit the program by choosing 3 in the main menu.

- Deconnect from the master instance and run the DeleteArchitecture.py script to fully delete all the instances created before.



## Authors

- [@sferez](https://github.com/sferez)

