package com.saiswayam.orchestrator.persistence;

import com.saiswayam.orchestrator.model.TaskStatus;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface TaskRecordRepository extends JpaRepository<TaskRecord, String> {
    List<TaskRecord> findByStatusOrderByUpdatedAtDesc(TaskStatus status, Pageable pageable);
    List<TaskRecord> findAllByOrderByUpdatedAtDesc(Pageable pageable);
    long countByStatus(TaskStatus status);
}
