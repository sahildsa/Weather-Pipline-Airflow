from airflow.sdk import dag,task,task_group


@dag
def group_experiment_dag():
    @task
    def task_1():
        print("Task 1")

    @task_group
    def task_group_1():
        @task
        def task_2():
            print("Task 2")

        @task
        def task_3():
            print("Task 3")

        task_2()>>task_3()

    task_1()>>task_group_1()

group_experiment_dag()