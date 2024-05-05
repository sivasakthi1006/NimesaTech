package com.nimesa.careers.multithreading_assignment;


import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Processor {

    private final Queue<TaskRequest> queue = new LinkedBlockingQueue<>();


    Processor(TaskRequest taskRequest) {
        queue.offer(taskRequest);
    }

    Processor(List<TaskRequest> taskRequest) {
        for (TaskRequest request : taskRequest) {
            queue.offer(request);

        }
    }

    public List<TaskResponse> execute() throws InterruptedException, ExecutionException {

        List<TaskResponse> taskResponses = new ArrayList<>();
        Map<String, List<TaskRequest>> groupingObject = queue.stream().collect(Collectors.groupingBy(TaskRequest::getSubmittedBy));

        for (Map.Entry<String, List<TaskRequest>> entry : groupingObject.entrySet()) {
            List<TaskRequest> taskList = entry.getValue();
            ExecutorService executorService = Executors.newFixedThreadPool(5);
            CompletableFuture<Void> async = CompletableFuture.runAsync(() -> {
                sortTaskRequest(taskList);
                taskList.parallelStream().forEach(taskRequest -> {
                    System.out.println(Thread.currentThread().getName());
                    System.out.println("Starting Task " + taskRequest.getId());
                    Task task = new Task(taskRequest);
                    TaskResponse response = null;
                    try {
                        response = task.run();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    System.out.println("Completed Task " + response.getId() + "With Status" + response.getStatus());
                    taskResponses.add(response);
                });
            }, executorService);
            async.get();

        }
        return taskResponses;

        /**
         *Sequential execution
         */
        // existing code
//        for (TaskRequest taskRequest : queue) {
//            System.out.println("Starting Task "+taskRequest.getId());
//            Task task = new Task(taskRequest);
//            TaskResponse response = task.run();
//            System.out.println("Completed Task "+response.getId() +"With Status"+response.getStatus());
//            taskResponses.add(response);
//        }
//        return taskResponses;
    }

    private void sortTaskRequest(List<TaskRequest> sortTaskRequest) {
        sortTaskRequest.sort(Comparator.comparingInt(TaskRequest::getPriority));
    }
}

