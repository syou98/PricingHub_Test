# PricingHub technical interview

### Usage
* Before starting, ensure you have Docker and Docker Compose installed.
* You can then run the following command to launch the Airflow environment :
```
docker-compose up
```
* After a few seconds to launch the Airflow webserver and scheduler, you can access the Airflow UI at the following address: http://localhost:8080.
* Credentials are specified in .env file (_AIRFLOW_WWW_USER_USERNAME & _AIRFLOW_WWW_USER_PASSWORD).
* To launch the DAG with the desired configuration, click on the Play button and then on "Trigger w/ config". This will open an editor with an empty dictionary. Next, simply create the "tasks" field as a list of tasks to be executed.
* The names of the 3 tasks are: "task_1", "task_2" and "task_3".
* For example {"tasks": ["task_1", "task_2"]} will only trigger the first 2 tasks, while {"tasks":["task_2"]} will only trigger the second.

### Notes
* The exercise imposes a constraint of 3 tasks in the DAG. This is why the execution configuration is retrieved in each Task.
* A logs folder is normally created in the main repository folder. This folder contains the history and various logs of DAGs executions. However, it is also possible to use the Airflow UI directly for greater simplicity.
* If the "tasks" field is not present, all tasks are skipped. This is also the case if the "tasks" field is present but is not a list.
* The choice of using a list containing the tasks to be executed was made for the sake of simplicity. A structure of the type {"tasks": {"task_1": true, "task_2": false, "task_3": true}} could also have been possible, provided that the parsing was correct and the potential new exceptions were handled properly.
* Operators are tested in isolation, taking into account the execution configuration.


Thank you



