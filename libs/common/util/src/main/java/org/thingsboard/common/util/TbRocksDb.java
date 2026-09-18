package org.thingsboard.common.util;

import lombok.SneakyThrows;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.BiConsumer;

public class TbRocksDb {

    protected final String path;
    private final Options dbOptions;
    private final WriteOptions writeOptions;
    protected RocksDB db;

    static {
        RocksDB.loadLibrary();
    }

    public TbRocksDb(String path, Options dbOptions, WriteOptions writeOptions) {
        this.path = path;
        this.dbOptions = dbOptions;
        this.writeOptions = writeOptions;
    }

    @SneakyThrows
    public void init() {
        Files.createDirectories(Path.of(path).getParent());
        db = RocksDB.open(dbOptions, path);
    }

    @SneakyThrows
    public void put(String key, byte[] value) {
        db.put(writeOptions, key.getBytes(StandardCharsets.UTF_8), value);
    }

    public void forEach(BiConsumer<String, byte[]> processor) {
        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                processor.accept(key, iterator.value());
            }
        }
    }

    @SneakyThrows
    public void delete(String key) {
        db.delete(writeOptions, key.getBytes(StandardCharsets.UTF_8));
    }

    public void close() {
        if (db != null) {
            db.close();
        }
    }

}
