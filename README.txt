On this project we analyzed queries and trained a model to classify if the query is anomalous or just a normal query.

we used Spark.ml library to achieve our goal.

on the Pyspark_Analysis.ipynb you may find all description and details of the training process.

In the demos section u may find multiple demos of the use of the trained model.

-classification with kafka and spark streaming (used only on the vLab)

-file stream - on files given to the file dstream the model will listen to directory and everytime a text file that have queries the will classify all of the queries in the file

- SQS messging service, this demo will demonstrate how the model consume messages from the sqs service and will perform predictions on each message recieved.

-socket text streaming - demonstrattion with messages recieved on the port with netcat


for additional information visit:

	https://github.com/RickyDa/BigData-Http-injections

