package com.jnks.iot.server.utils;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.leshan.core.model.InvalidDDFFileException;
import org.eclipse.leshan.core.model.ObjectModel;
import com.jnks.iot.server.common.data.ResourceType;
import com.jnks.iot.server.common.data.TbResource;
import com.jnks.iot.server.common.data.exception.JnksIotErrorCode;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.lwm2m.LwM2mInstance;
import com.jnks.iot.server.common.data.lwm2m.LwM2mObject;
import com.jnks.iot.server.common.data.lwm2m.LwM2mResourceObserve;
import com.jnks.iot.server.common.data.util.TbDDFFileParser;
import com.jnks.iot.server.dao.exception.DataValidationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.jnks.iot.server.common.data.lwm2m.LwM2mConstants.LWM2M_SEPARATOR_KEY;
import static com.jnks.iot.server.common.data.lwm2m.LwM2mConstants.LWM2M_SEPARATOR_SEARCH_TEXT;

@Slf4j
public class LwM2mObjectModelUtils {

    private static final TbDDFFileParser ddfFileParser = new TbDDFFileParser();

    public static void toLwm2mResource(TbResource resource) throws JnksIotException {
        try {
            List<ObjectModel> objectModels =
                    ddfFileParser.parse(new ByteArrayInputStream(resource.getData()), resource.getSearchText());
            if (!objectModels.isEmpty()) {
                ObjectModel objectModel = objectModels.get(0);

                String resourceKey = objectModel.id + LWM2M_SEPARATOR_KEY + objectModel.version;
                String name = objectModel.name;
                resource.setResourceKey(resourceKey);
                if (resource.getId() == null) {
                    resource.setTitle(name + " id=" + objectModel.id + " v" + objectModel.version);
                }
                resource.setSearchText(resourceKey + LWM2M_SEPARATOR_SEARCH_TEXT + name);
            } else {
                throw new DataValidationException(String.format("Could not parse the XML of objectModel with name %s", resource.getSearchText()));
            }
        } catch (InvalidDDFFileException e) {
            log.error("Failed to parse file {}", resource.getFileName(), e);
            throw new DataValidationException("Failed to parse file " + resource.getFileName());
        } catch (IOException e) {
            throw new JnksIotException(e, JnksIotErrorCode.GENERAL);
        }
        if (resource.getResourceType().equals(ResourceType.LWM2M_MODEL) && toLwM2mObject(resource, true) == null) {
            throw new DataValidationException(String.format("Could not parse the XML of objectModel with name %s", resource.getSearchText()));
        }
    }

    public static LwM2mObject toLwM2mObject(TbResource resource, boolean isSave) {
        try {
            List<ObjectModel> objectModels =
                    ddfFileParser.parse(new ByteArrayInputStream(resource.getData()), resource.getSearchText());
            if (objectModels.size() == 0) {
                return null;
            } else {
                ObjectModel obj = objectModels.get(0);
                LwM2mObject lwM2mObject = new LwM2mObject();
                lwM2mObject.setId(obj.id);
                lwM2mObject.setKeyId(resource.getResourceKey());
                lwM2mObject.setName(obj.name);
                lwM2mObject.setMultiple(obj.multiple);
                lwM2mObject.setMandatory(obj.mandatory);
                LwM2mInstance instance = new LwM2mInstance();
                instance.setId(0);
                List<LwM2mResourceObserve> resources = new ArrayList<>();
                obj.resources.forEach((k, v) -> {
                    if (isSave) {
                        LwM2mResourceObserve lwM2MResourceObserve = new LwM2mResourceObserve(k, v.name, false, false, false);
                        resources.add(lwM2MResourceObserve);
                    } else if (v.operations.isReadable()) {
                        LwM2mResourceObserve lwM2MResourceObserve = new LwM2mResourceObserve(k, v.name, false, false, false);
                        resources.add(lwM2MResourceObserve);
                    }
                });
                if (isSave || resources.size() > 0) {
                    instance.setResources(resources.toArray(LwM2mResourceObserve[]::new));
                    lwM2mObject.setInstances(new LwM2mInstance[]{instance});
                    return lwM2mObject;
                } else {
                    return null;
                }
            }
        } catch (IOException | InvalidDDFFileException e) {
            log.error("Could not parse the XML of objectModel with name [{}]", resource.getSearchText(), e);
            return null;
        }
    }

}
