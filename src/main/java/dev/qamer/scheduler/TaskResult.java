package dev.qamer.scheduler;
import java.time.Duration;
public record TaskResult(String taskId, Status status, int attempts, String output, String error, Duration duration) {
    public enum Status { SUCCESS, FAILED, BLOCKED }
}
