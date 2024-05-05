package com.nimesa.careers.multithreading_assignment;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@SpringBootTest
class ProcessorTest {
    private final List<String> users = List.of("User4", "User1", "User3", "User2");
    private final List<TaskType> taskTypes = List.of(TaskType.values());

    @Test
    public void testProcess() throws InterruptedException, ExecutionException {
        List<TaskRequest> jobList = getJobList(8);
        Processor processor = new Processor(jobList);
        List<TaskResponse> execute = processor.execute();
        for (TaskResponse taskResponse : execute) {
            System.out.println(taskResponse.getId()+" completed "+taskResponse.getStatus());
        }
    }

    private List<TaskRequest> getJobList(int n) {
        List<TaskRequest> taskRequests = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String user = (String) getRandomStringFromList(users);
            TaskType taskType = (TaskType) getRandomStringFromList(taskTypes);
            int priority = getRandomNumber(0, 5);
            int size = getRandomNumber(1, 10);

            TaskRequest taskRequest = new TaskRequest(UUID.randomUUID().toString(), user, priority, taskType, size);
            taskRequests.add(taskRequest);
        }
        return taskRequests;
    }

    private Object getRandomStringFromList(List<?> strings) {
        int size = strings.size();
        int randomIndex = getRandomNumber(0, size);
        return strings.get(randomIndex);
    }

    private int getRandomNumber(int min, int max) {
        return (int) ((Math.random() * (max - min)) + min);
    }

}