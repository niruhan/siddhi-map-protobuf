package org.wso2.extension.siddhi.map.protobuf.sourcemapper;

import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.stream.input.source.AttributeMapping;
import org.wso2.siddhi.core.stream.input.source.InputEventHandler;
import org.wso2.siddhi.core.stream.input.source.SourceMapper;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.List;

/**
 * This is a sample class-level comment, explaining what the extension class does.
 */

@Extension(
        name = "protobuf",
        namespace = "sourceMapper",
        description = " ",
        parameters = {
                /*@Parameter(name = " ",
                        description = " " ,
                        dynamic = false/true,
                        optional = true/false, defaultValue = " ",
                        type = {DataType.INT, DataType.BOOL, DataType.STRING, DataType.DOUBLE,etc }),
                        type = {DataType.INT, DataType.BOOL, DataType.STRING, DataType.DOUBLE, }),*/
        },
        examples = {
                @Example(
                        syntax = " ",
                        description = " "
                )
        }
)

public class ProtobufSourceMapper extends SourceMapper {

    /**
     * The initialization method for {@link SourceMapper}, which will be called before other methods and validate
     * the all configuration and getting the initial values.
     *
     * @param streamDefinition     Associated output stream definition
     * @param optionHolder         Option holder containing static configuration related to the {@link SourceMapper}
     * @param attributeMappingList Custom attribute mapping for source-mapping
     * @param configReader         to read the {@link SourceMapper} related system configuration.
     * @param siddhiAppContext     the context of the {@link org.wso2.siddhi.query.api.SiddhiApp} used to get siddhi
     */
    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                     List<AttributeMapping> attributeMappingList, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {

    }

    /**
     * Returns the list of classes which this source can output.
     *
     * @return Array of classes that will be output by the source.
     * Null or empty array if it can produce any type of class.
     */
    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[0];
    }

    /**
     * Method to map the incoming event and as pass that via inputEventHandler to process further.
     *
     * @param eventObject           Incoming event Object based on the supported event class imported by the extensions.
     * @param inputEventHandler     Handler to pass the converted Siddhi Event for processing
     * @throws InterruptedException if it does not throw the exception immediately due to streaming
     */
    @Override
    protected void mapAndProcess(Object eventObject, InputEventHandler inputEventHandler) throws InterruptedException {

    }

    /**
     * Method used by {@link SourceMapper} to determine on how to handle transport properties with null values. If
     * this returns 'false' then {@link SourceMapper} will drop any event/s with null transport
     * property values. If this returns
     * 'true' then {@link SourceMapper} will send events even though they contains null transport properties.
     * This method will be called after init().
     *
     * @return whether {@link SourceMapper} should allow or drop events when transport properties are null.
     */
    @Override
    protected boolean allowNullInTransportProperties() {
        return false;
    }
}
