package com.intrence.cdg.bootstrap;

import com.intrence.cdg.exception.InternalServerException;
import com.intrence.cdg.util.Constants;
import com.intrence.config.ConfigProvider;
import com.intrence.config.collection.ConfigList;
import com.intrence.config.collection.ConfigMap;
import com.intrence.config.configloader.ConfigMapLoader;
import com.intrence.config.configloader.YamlFileConfigMapLoader;
import org.apache.log4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages={"com.intrence.cdg"})
public class AppConfiguration {
    private static final Logger LOGGER = Logger.getLogger(AppConfiguration.class);

    /**
     * this bean is created to know all instantiated spring beans.
     * @return
     */
    @Bean
    public ApplicationContextProvider applicationContextProvider() {
        return new ApplicationContextProvider();
    }

    @Bean
    public ConfigMap configMap() {
        return ConfigProvider.getConfig();
    }
    
    @Bean
    public ConfigMap sourcesConfigMap() {
        
        ConfigMapLoader configMapLoader = null;
        if(Constants.DEV_ENVIRONMENTS.contains(ConfigProvider.getEnvironment())) {
            configMapLoader = new YamlFileConfigMapLoader(Constants.CDG_SOURCES_CONFIG_LOCALHOST_PATH);
        } else {
//            configMapLoader = new LocalFileConfigMapLoader(Constants.DORA_SOURCES_CONFIG_LOCALHOST_PATH, 300, new SourcesConfigUpdateHandler());
        }        
        
        ConfigMap configMap = configMapLoader.getConfigMap();        
        ConfigMap supportedSources = configMap.getMap(Constants.SUPPORTED_SOURCES);
        if( supportedSources == null || supportedSources.isEmpty() ) {
            throw new ExceptionInInitializerError("Event=Error initializing configMap..  Sources config is empty!!\n" +
                    "Bad Config provided.");
        }

        for(String source: supportedSources.keySet()) {
            String sourceIntegrationType = supportedSources.getMap(source).getString(Constants.SOURCE_INTEGRATION_TYPE, null);
            ConfigList sourceOperationTypes = supportedSources.getMap(source).getList(Constants.SOURCE_OPERATION_TYPES, new ConfigList());
            if(sourceIntegrationType==null) {
                LOGGER.error(String.format("Integration type for source=%s is not provided.", source));
                throw new InternalServerException(String.format("Integration type for source=%s is not provided in Dora config file.", source));
            }
            if(sourceOperationTypes.size()<1) {
                LOGGER.error(String.format("Operation/Search type for source=%s is not provided.", source));
                throw new InternalServerException(String.format("Operation type for source=%s is not provided in Dora config file.", source));
            }
        }

        return supportedSources;
    }
}