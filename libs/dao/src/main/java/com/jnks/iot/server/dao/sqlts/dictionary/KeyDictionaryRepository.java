package com.jnks.iot.server.dao.sqlts.dictionary;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import com.jnks.iot.server.dao.model.sqlts.dictionary.KeyDictionaryCompositeKey;
import com.jnks.iot.server.dao.model.sqlts.dictionary.KeyDictionaryEntry;

import java.util.Optional;

public interface KeyDictionaryRepository extends JpaRepository<KeyDictionaryEntry, KeyDictionaryCompositeKey> {

    Optional<KeyDictionaryEntry> findByKeyId(int keyId);

    @Query("SELECT e FROM KeyDictionaryEntry e ORDER BY e.keyId ASC")
    Page<KeyDictionaryEntry> findAll(Pageable pageable);

}