package org.chandra.repository;

import org.chandra.entity.CallDataRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CallDataRecordRepository extends JpaRepository<CallDataRecord, Long> {
}
