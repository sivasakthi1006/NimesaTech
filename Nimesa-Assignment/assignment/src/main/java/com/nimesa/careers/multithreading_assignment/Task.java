package com.nimesa.careers.multithreading_assignment;

public class Task {
    private final TaskRequest taskRequest;
    Task(TaskRequest taskRequest){
        this.taskRequest = taskRequest;
    }

    public TaskResponse run() throws InterruptedException {

        int size = taskRequest.getSize();
        Thread.sleep(size*1000);
        return new TaskResponse(taskRequest.getId(),Status.SUCCESS);
    }

}
