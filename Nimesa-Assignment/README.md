# NIMESA Processor

## Overview

This project features a highly efficient task processor designed to execute submitted tasks seamlessly. Each task is
defined by the following parameters:

1. **id**: Unique identifier for each task.
2. **submittedBy**: The user who submitted the task.
3. **priority**: Priority level of the task.
4. **type**: The type/category of the task.
5. **size**: The size of the task, determining the time it takes to complete.

## Current Processing Method

At present, the processor is handling tasks sequentially, one by one. However, this approach proves to be inefficient,
causing other tasks in the queue to be delayed and users to experience extended wait times.
## Test the Code

To run the code run spring boot test method inside class `ProcessorTest.java` available in package
`com.nimesa.careers.multithreading_assignment`


## Enhanced Task Processing

To address these efficiency concerns, the system has been upgraded to execute tasks in parallel, providing faster and
more responsive task processing. The following enhancements have been implemented:

### 1. Parallel Execution of All Tasks

Tasks are now executed simultaneously, allowing for optimal utilization of system resources and significantly reducing
overall processing time.

### 2. Parallel Execution of Unique User Tasks

Unique user tasks are processed concurrently, ensuring that each user's tasks are completed without unnecessary delays.
Additionally, if a user has submitted multiple tasks, each task will run sequentially, ensuring fair and efficient task
allocation.

### 3. Parallel Execution of Unique User and Task Type

Tasks are executed in parallel based on both unique user and task type, providing a streamlined approach to handle
specific types of tasks for individual users concurrently.Additionally, if a user has submitted multiple tasks od same
type, each task having same type belonging to same user will run sequentially, ensuring fair and efficient task
allocation.

### 4. Parallel Execution Based on Priority, User, and Task Type

Tasks are prioritized based on user, task type, and priority level, allowing the system to efficiently process
high-priority tasks for each unique user and task type simultaneously.

## Conclusion

With these enhancements, the task processor system ensures a faster and more responsive experience for users. The
parallel execution of tasks optimizes resource utilization and reduces waiting times, while also ensuring fair and
efficient task allocation for users who submit multiple tasks. This approach ultimately improves the overall efficiency
of the task processing system.

