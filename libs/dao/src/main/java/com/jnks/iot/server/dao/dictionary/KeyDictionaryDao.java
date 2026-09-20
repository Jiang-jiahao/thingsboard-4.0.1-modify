package com.jnks.iot.server.dao.dictionary;


import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.model.sqlts.dictionary.KeyDictionaryEntry;

public interface KeyDictionaryDao {

    Integer getOrSaveKeyId(String strKey);

    String getKey(Integer keyId);

    PageData<KeyDictionaryEntry> findAll(PageLink pageLink);
}
